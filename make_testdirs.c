#include <unistd.h>             // getopt()
#include <stdlib.h>             // exit()
#include <sys/stat.h>
#include <sys/types.h>
#include <stdio.h>
#include <fcntl.h>
#include <string.h>
#include <errno.h>

#define MAX_PATHNAME  256

void usage(const char* progname) {
  printf("Usage: %s [ options ] dirname\n", progname);
  printf("options:\n");
  printf("  -d <n_dirs>    n subdirs\n");
  printf("  -f <n_files>   n files per subdir\n");
  printf("  -h             help\n");
}


                                   
int make_it(const char* dir,
            unsigned    n_dirs,
            unsigned    n_files) {

   unsigned int rand_state;

   char subdir_path[MAX_PATHNAME];
   int  subdir;
   for (subdir=0; subdir<n_dirs; ++subdir) {
      int rc = snprintf(subdir_path, MAX_PATHNAME, "%s/dir%05d", dir, subdir);
      if ((rc < 0) || (rc >= MAX_PATHNAME)) {
         fprintf(stderr, "snprintf('%s/dir%05d') failed: %s\n", dir, subdir, strerror(errno));
         exit(-1);
      }

      printf("making %u files in subdir %s / %u\n", n_files, subdir_path, n_dirs);
      if (mkdir(subdir_path, 0777)
          && (errno != EEXIST)) {
         fprintf(stderr, "mkdir(%s) failed: %s\n", subdir_path, strerror(errno));
         exit(-1);
      }


      char file_path[MAX_PATHNAME];
      int  file;
      for (file=0; file<n_files; ++file) {
         int rc = snprintf(file_path, MAX_PATHNAME, "%s/file%05d", subdir_path, file);
         if ((rc < 0) || (rc >= MAX_PATHNAME)) {
            fprintf(stderr, "snprintf('%s/file%05d') failed: %s\n", subdir_path, file, strerror(errno));
            exit(-1);
         }

         // printf("file: %s\n", file_path);
         int fd = open(file_path, (O_WRONLY|O_CREAT|O_TRUNC), 0777);
         if (fd < 0) {
            fprintf(stderr, "open(%s) failed: %s\n", file_path, strerror(errno));
            exit(-1);
         }

         int rnd = rand_r(&rand_state) % (32 * 1024); // random [0, 64k)
         ftruncate(fd, rnd);

         close(fd);
      }
   }

   return 0;
}

int main (int argc, char* argv[]) {

  unsigned n_dirs  = 1024;        // default
  unsigned n_files = 1024;        // default
  int      help    = 0;
  char*    dir     = NULL;

  int   c;
  optind = 0;

  while ((c = getopt(argc, argv, "hd:f:")) != -1) {

    printf("-- option '%c'\n", c);
    printf("\t optarg:      '%s'\n", optarg);
    printf("\t optind:      %d\n", optind);

    switch (c) {

    case 'd':  n_dirs = atoi(optarg);        break;
    case 'f':  n_files = atoi(optarg);       break;
    case 'h':  help = 1; break;

    case '?':
      // getopt returns '?' when there is a problem.
      printf ("\t unrecognized option '%s'\n", argv[optind -1]);
      break;

    default:
      printf ("\t ?? getopt returned '%c' 0%o ??\n", (char)c, c);
    }
  }

  if (help) {
    usage(argv[0]);
    exit(0);
  }

  if (optind >= argc) {
    usage(argv[0]);
    exit(-1);
  }
  dir = argv[optind++];

  unsigned seed = 0;
  srand(seed);

  mkdir(dir, 0777);
  make_it(dir, n_dirs, n_files);

  return 0;
}
