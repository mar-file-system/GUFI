#include <stdlib.h>
#include <string.h>

#include "compress.h"

#if HAVE_ZLIB
#include <zlib.h>

/* starting offset of actual data */
static const size_t COMP_OFFSET = sizeof(compressed_t);

/*
 * dst should be allocated by the caller
 *
 * it is a double pointer because it can be reallocated
 */
void compress_zlib(void **dst, void *src, const size_t struct_len, const size_t max_len) {
    /* Not checking arguments */

    compressed_t *comp = (compressed_t *) *dst;

    /* compress work item */
    comp->len = struct_len - COMP_OFFSET;
    const int ret = compress(((unsigned char *) *dst) + COMP_OFFSET, &comp->len,
                             ((unsigned char *) src)  + COMP_OFFSET, struct_len);
    comp->yes = (ret == Z_OK);

    /* if compressed enough, reallocate to reduce memory usage */
    if (comp->yes && (comp->len < max_len)) {
        void *r = realloc(*dst, COMP_OFFSET + comp->len);
        if (r) {
            *dst = r;
        }
        /* if realloc failed, dst can still be used */
    }
}

/* dst either points to src or data */
void decompress_zlib(void **dst, void *src, void *data, const size_t struct_len) {
    /* Not checking arguments */

    compressed_t *comp = (compressed_t *) data;

    if (comp->yes) {
        compressed_t *src_comp = (compressed_t *) src;
        memset(src_comp, 0, sizeof(*src_comp)); /* not strictly necessary */

        size_t len = struct_len - COMP_OFFSET;
        uncompress(((unsigned char *) src)  + COMP_OFFSET, &len,
                   ((unsigned char *) data) + COMP_OFFSET, comp->len);
        free(data);

        *dst = src;
    }
    else {
        *dst = data;
    }
}

/* free if the data was not decompressed into the struct on the stack */
void free_zlib(void *heap, void *stack) {
    if (heap != stack) {
        free(heap);
    }
}

#endif

void *compress_struct(const int comp, void *stack, const size_t struct_len) {
    void *copy = malloc(struct_len);

    #if HAVE_ZLIB
    if (comp) {
        compress_zlib((void **) &copy, stack, struct_len, struct_len);
        if (((compressed_t *) copy)->yes) {
            return copy;
        }
    }
    #else
    (void) comp;
    #endif

    memcpy(copy, stack, struct_len);

    return copy;
}

int decompress_struct(const int comp, void *data,
                      void **heap, void *stack, const size_t struct_len) {
    #if HAVE_ZLIB
    if (comp) {
        decompress_zlib((void **) heap, stack, data, struct_len);
        return 0;
    }
    #else
    (void) comp;
    (void) stack;
    #endif

    *heap = (void *) data;

    return 0;
}

int free_struct(const int comp, void *heap, void *stack, const size_t recursion_level) {
    #if HAVE_ZLIB
    if (comp) {
        free_zlib(heap, stack);
        return 0;
    }
    #else
    (void) comp;
    (void) stack;
    #endif
    if (recursion_level == 0) {
        free(heap);
    }

    return 0;
}
