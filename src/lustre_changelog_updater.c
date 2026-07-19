/*
 * To build:
 *
 *     export LUSTRE_LIBRARY_DIR=/home/$USER/git/lustre-release/lustre/utils/.libs
 *     export LUSTRE_INCLUDE_DIR=/home/$USER/git/lustre-release/lustre/include/
 *     gcc -D_GNU_SOURCE -I$LUSTRE_INCLUDE_DIR -I$LUSTRE_INCLUDE_DIR/uapi/ -L$LUSTRE_LIBRARY_DIR -llustreapi changelog_reader.c
 *
 * To run:
 *
 *     export LD_LIBRARY_PATH=$LUSTRE_LIBRARY_DIR
 *     ./a.out
 *
 */

#include <stdio.h>
#include <getopt.h>
#include <pthread.h>

#include "lustre/lustreapi.h"

#include "incremental_non_path.h"

static void usage(const char *prog_name) {
    fprintf(stderr, "Usage: %s --gufi-root GUFI_ROOT --consumer CONSUMER...\n", prog_name);
    fprintf(stderr, "\t\tCONSUMER format: \"<MDT_NAME>:<CONSUMER_ID>\". Example: \"lustre-MDT0000:cl1\"\n");

    exit(1);
}

/* Halt the program if malloc() fails. */
static void oom(void) {
    fprintf(stderr, "Out of memory.");
    exit(1);
}

/*
 * This holds the information needed to consume changelog records from a single MDT.
 */
struct changelog_client {
    char *mdt_name;         /* e.g., 'lustre-MDT0000' */
    char *consumer_name;    /* e.g., 'cl1' */
};

#define MAX_MDTS 64     /* Hardcoded limit; increase if this runs on an FS with more. */

/*
 * Expects a string with the lustre MDT name and changelog consumer name
 * separated by a colon.
 *
 * For example: "lustre-MDT0000:cl1" -> { "lustre-MDT0000", "cl1" }
 */
static struct changelog_client parse_changelog_client(char *s, const char *prog_name) {
    char *sep = strchr(s, ':');

    if (!sep) {
        fprintf(stderr, "Could not parse changelog consumer '%s'.\n", s);
        usage(prog_name);
    }

    *sep = '\0';

    struct changelog_client new = { NULL };

    new.mdt_name = strdup(s);
    sep++;
    new.consumer_name = strdup(sep);

    return new;
}

static struct {
    /*
     * An array of changelog clients, one for each MDT that this program should query.
     */
    int num_clients;    /* Number of entries in the changelog_clients array. */
    struct changelog_client changelog_clients[MAX_MDTS];

    char *gufi_root;    /* Root of the GUFI index tree. */

    /* State used by FH incremental API. */
    struct fh_incremental_state incremental_state;
} args = { 0 };

int parse_args(int argc, char *argv[])
{
    int opt;

    static struct option long_options[] = {
        {"consumer", required_argument, 0, 'c'},
        {"gufi-root", required_argument, 0, 1},
        {"help",     no_argument,       0, 'h'},
        {0, 0, 0, 0}
    };

    while ((opt = getopt_long(argc, argv, "c:h", long_options, NULL)) != -1) {
        switch (opt) {
            case 'c':
                if (args.num_clients >= MAX_MDTS) {
                    fprintf(stderr, "Too many changelog consumers specified. (Max: %d)\n", MAX_MDTS);
                    usage(argv[0]);
                }
                args.changelog_clients[args.num_clients++] = parse_changelog_client(optarg, argv[0]);
                break;
            case 1:
                args.gufi_root = optarg;
                break;
            case 'h':
                usage(argv[0]);
            default:
                usage(argv[0]);
        }
    }

    if (args.num_clients == 0) {
        fprintf(stderr, "Specify at least one changelog consumer.\n");
        usage(argv[0]);
    }

    if (!args.gufi_root) {
        fprintf(stderr, "Must specify root of GUFI index tree.\n");
        usage(argv[0]);
    }

    return 0;
}

enum record_type {
    REC_PARENT,     /* Any change that simply requires recreating a directory's index. */
    REC_MKDIR,      /* Creation of a new node in the index tree. */
    REC_RENAME,     /* Moving a node in the index tree. */
    REC_RMDIR,      /* Remove a node in the index tree. */
};

struct record {
    uint8_t type;               /* enum record_type */

    uint64_t timestamp;         /* Record timestamp in Lustre "packed" format. */

    struct lu_fid parent;       /* Parent directory of the object modified */
    char *name;                 /* Name of the object that was created or deleted */

    union {
        struct lu_fid old_parent;   /* For rename - this is the OLD parent */
        struct lu_fid child;        /* for mkdir, rmdir records - the FID of the target object */
    };

    char *old_name;             /* Only set for rename: OLD name of object */
};

/*
 * Extract the seconds from a Lustre packed timestamp.
 * See man 3 llapi_changelog_recv for details on the format.
 * The seconds are stored in the top 34 bits, nanoseconds in the bottom 30 bits.
 */
static uint64_t get_seconds(uint64_t lustre_packed_timestamp) {
    return lustre_packed_timestamp >> 30;
}

static uint64_t get_nanoseconds(uint64_t lustre_packed_timestamp) {
    const uint64_t nanoseconds_mask = (1ULL << 30) - 1;

    return lustre_packed_timestamp & nanoseconds_mask;
}

/* NB: Not thread safe due to static buf... */
static char *format_fid_as_bytes(struct lu_fid *fid) {
#define BUF_SIZE 4096
    static char buf[BUF_SIZE];

    memset(buf, 0, sizeof buf);

    size_t s = 0;
    s += snprintf(buf, BUF_SIZE, "0x");

    unsigned char *p = (unsigned char *) fid;

    for (size_t i = 0; i < sizeof(*fid); i++) {
        s += snprintf(&buf[s], BUF_SIZE, "%x", p[i]);
    }

    return buf;
}

static void dump_record(struct record *rec) {
    char *type;
    switch (rec->type) {
    case REC_PARENT:
        type = "PARENT";
        break;
    case REC_MKDIR:
        type = "MKDIR";
        break;
    case REC_RENAME:
        type = "RENAME";
        break;
    case REC_RMDIR:
        type = "RMDIR";
    }

    printf("%llu.%llu: %s\tpfid=%s\tname='%s', old_name='%s'\n",
            get_seconds(rec->timestamp), get_nanoseconds(rec->timestamp),
            type, format_fid_as_bytes(&rec->parent),
            rec->name ? rec->name : "",
            rec->old_name ? rec->old_name : "");
}

struct record_array {
    size_t len;                 /* Index of the last used slot. */
    size_t capacity;            /* Available slots. */
    struct record *records;     /* Pointer to the records. */
};

/*
 * Initialize a struct record_array, giving it a reasonably sized initial capacity.
 */
static void record_array_initialize(struct record_array *recs) {
    const size_t initial_capacity = 1024;

    recs->records = calloc(initial_capacity, sizeof *(recs->records));
    if (!recs->records)
        oom();

    recs->len = 0;
    recs->capacity = initial_capacity;
}

/*
 * Push a new record to the end of a record_array.
 * Doubles the capacity if there are no more empty slots at the end.
 */
static void record_array_push(struct record_array *recs, struct record new) {
    if (recs->len >= recs->capacity) {
        struct record *new = reallocarray(recs->records, recs->capacity * 2, sizeof *(recs->records));
        if (!new)
            oom();

        recs->records = new;
        recs->capacity *= 2;
    }

    recs->records[recs->len] = new;
    recs->len++;
}

/*
 * Look up a record in a record_array.
 */
struct record *record_array_get(struct record_array *recs, size_t index) {
    if (index > recs->capacity || index > recs->len) {
        fprintf(stderr, "BUG: invalid index %llu (cap: %llu, len: %llu)\n",
                index, recs->capacity, recs->len);
        exit(1);
    }

    return &recs->records[index];
}

/*
 * Re-initialize a record_array, but without re-allocating the underlying memory.
 */
static void record_array_reset(struct record_array *recs) {
    recs->len = 0;
    memset(recs->records, 0, sizeof *(recs->records) * recs->capacity);
}

/* The state of a consumer of one MDT. */
struct one_mdt_state {
    struct changelog_client *client;    /* The consumer identity for this MDT. */
    struct record_array records;        /* Changelog records that this consumer has read. */
    void *changelog_priv;               /* Private data passed to the lustre changelog functions. */
};

/*
 * Read the changelog records from a single MDT.
 * Store the ones we care about in a `struct record_array`.
 */
void *get_records_one_mdt(void *data) {
    struct one_mdt_state *state = (struct one_mdt_state *) data;
    int rc;

    struct changelog_rec *received;
    while ((rc = llapi_changelog_recv(state->changelog_priv, &received)) == 0) {
        struct record my_rec = { 0 };

        __u32 type = received->cr_type;

        my_rec.parent = received->cr_pfid;
        my_rec.timestamp = received->cr_time;

        switch (received->cr_type) {
        case CL_MKDIR:
            my_rec.type = REC_MKDIR;
            my_rec.name = strdup(changelog_rec_name(received));
            my_rec.child = received->cr_tfid;
            break;
        case CL_RENAME:
            my_rec.type = REC_RENAME;
            my_rec.name = strdup(changelog_rec_name(received));
            my_rec.old_name = strdup(changelog_rec_sname(received));
            struct changelog_ext_rename *rename_metadata = changelog_rec_rename(received);
            my_rec.old_parent = rename_metadata->cr_spfid;
            break;
        case CL_RMDIR:
            my_rec.type = REC_RMDIR;
            my_rec.name = strdup(changelog_rec_name(received));
            my_rec.child = received->cr_tfid;
            break;
        case CL_CREATE:
        case CL_MKNOD:
            my_rec.type = REC_PARENT;
            break;
        default:
            /* Don't care about any other record types that we might receive... */
            goto skip;
        }

        record_array_push(&state->records, my_rec);
skip:
        ; /* Continue on to next loop iteration. */
    }

    printf("done receiving records, rc = %d\n", rc);
    // TODO: return an error status somehow, if llapi_changelog_recv() failed?
    return NULL;
}

/*
 * Get the earliest record from the collection of records in `state`.
 * `indexes` points to the next unconsumed record in each array in `state`.
 *
 * Returns NULL if there are no more unconsumed records.
 */
struct record *get_next_record(size_t indexes[], struct one_mdt_state state[MAX_MDTS]) {
    struct record *p = NULL;
    size_t increment_index = 0;

    for (int i = 0; i < args.num_clients; i++) {
        /* If we've already consumed every record from this array, skip it: */
        if (indexes[i] >= state[i].records.len)
            continue;

        /* If this is the first array with an un-consumed record, initialize p: */
        if (!p) {
            p = record_array_get(&state[i].records, indexes[i]);
            increment_index = i;
            continue;
        }

        /* Otherwise, compare this un-consumed record with a previously-checked record: */
        struct record *q = record_array_get(&state[i].records, indexes[i]);
        if (q->timestamp < p->timestamp) {
            p = q;
            increment_index = i;
        }
    }

    if (p) {
        indexes[increment_index]++;
    }

    return p;
}

void process_records(struct one_mdt_state state[MAX_MDTS]) {
    size_t indexes[MAX_MDTS] = { 0 };

    struct record *rec;
    int rc;
    while (rec = get_next_record(indexes, state)) {
        dump_record(rec);

        switch (rec->type) {
        case REC_MKDIR:
            rc = do_mkdir(&args.incremental_state, (void *) &rec->parent, sizeof rec->parent,
                          (void *) &rec->child, sizeof rec->child, rec->name);
            if (rc) {
                return;
            }
            break;
        case REC_RENAME:
            rc = do_rename(&args.incremental_state, (void *) &rec->old_parent, sizeof rec->old_parent,
                           (void *) &rec->parent, sizeof rec->parent, rec->old_name, rec->name);
            if (rc) {
                return;
            }
            break;
        case REC_RMDIR:
            rc = do_rmdir(&args.incremental_state, (void *) &rec->parent, sizeof rec->parent,
                          (void *) &rec->child, sizeof rec->child, rec->name);
            if (rc) {
                return;
            }
            break;
        default:
            break;
        }
    }
}

void main_loop(void) {
    long long startrec = 0;
    int rc;

    pthread_t threads[MAX_MDTS] = { 0 };
    struct one_mdt_state state[MAX_MDTS] = { 0 };

    /* Initialize each MDT consumer's per-consumer state: */
    for (int i = 0; i < args.num_clients; i++) {
        state[i].client = &args.changelog_clients[i];

        record_array_initialize(&state[i].records);

        rc = llapi_changelog_start(&state[i].changelog_priv,
                       CHANGELOG_FLAG_JOBID |
                       CHANGELOG_FLAG_EXTRA_FLAGS,
                       args.changelog_clients[i].mdt_name, startrec);
        if (rc < 0) {
            fprintf(stderr, "error starting changelog: %s\n", strerror(-rc));
            return;
        }
    }

    /*
     * Main loop logic:
     *   - launch a thread for each consumer
     *   - wait for them to collect a number of records
     *   - join each thread
     *   - process all of the collected records together in chronological order
     */
    while (1) {
        for (int i = 0; i < args.num_clients; i++) {
            record_array_reset(&state[i].records);
            pthread_create(&threads[i], NULL, get_records_one_mdt, &state[i]);
        }

        for (int i = 0; i < args.num_clients; i++) {
            int rc = pthread_join(threads[i], NULL);
            // TODO: handle error joining? check for errors that occurred in get_records_one_mdt()?
        }

        process_records(state);

        break;
    }

    for (int i = 0; i < args.num_clients; i++) {
        llapi_changelog_fini(&state[i].changelog_priv);
    }
}

int main(int argc, char *argv[])
{
    void *changelog_priv;
    long long startrec = 0;

    if (parse_args(argc, argv)) {
        return EXIT_FAILURE;
    }

    sqlite3_initialize();

    args.incremental_state = open_incremental_state(args.gufi_root);

    main_loop();

    close_incremental_state(args.incremental_state);

    /* main_loop() is never supposed to exit unless an error occurs... */
    return EXIT_FAILURE;
}
