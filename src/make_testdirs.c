/*
This file is part of GUFI, which is part of MarFS, which is released
under the BSD license.


Copyright (c) 2017, Los Alamos National Security (LANS), LLC
All rights reserved.

Redistribution and use in source and binary forms, with or without modification,
are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice, this
list of conditions and the following disclaimer.

2. Redistributions in binary form must reproduce the above copyright notice,
this list of conditions and the following disclaimer in the documentation and/or
other materials provided with the distribution.

3. Neither the name of the copyright holder nor the names of its contributors
may be used to endorse or promote products derived from this software without
specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE
OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

-----
NOTE:
-----

GUFI uses the C-Thread-Pool library.  The original version, written by
Johan Hanssen Seferidis, is found at
https://github.com/Pithikos/C-Thread-Pool/blob/master/LICENSE, and is
released under the MIT License.  LANS, LLC added functionality to the
original work.  The original work, plus LANS, LLC added functionality is
found at https://github.com/jti-lanl/C-Thread-Pool, also under the MIT
License.  The MIT License can be found at
https://opensource.org/licenses/MIT.


From Los Alamos National Security, LLC:
LA-CC-15-039

Copyright (c) 2017, Los Alamos National Security, LLC All rights reserved.
Copyright 2017. Los Alamos National Security, LLC. This software was produced
under U.S. Government contract DE-AC52-06NA25396 for Los Alamos National
Laboratory (LANL), which is operated by Los Alamos National Security, LLC for
the U.S. Department of Energy. The U.S. Government has rights to use,
reproduce, and distribute this software.  NEITHER THE GOVERNMENT NOR LOS
ALAMOS NATIONAL SECURITY, LLC MAKES ANY WARRANTY, EXPRESS OR IMPLIED, OR
ASSUMES ANY LIABILITY FOR THE USE OF THIS SOFTWARE.  If software is
modified to produce derivative works, such modified software should be
clearly marked, so as not to confuse it with the version available from
LANL.

THIS SOFTWARE IS PROVIDED BY LOS ALAMOS NATIONAL SECURITY, LLC AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
ARE DISCLAIMED. IN NO EVENT SHALL LOS ALAMOS NATIONAL SECURITY, LLC OR
CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT
OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING
IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY
OF SUCH DAMAGE.
*/



#include <unistd.h>             // getopt()
#include <stdlib.h>             // exit()
#include <sys/stat.h>
#include <sys/types.h>
#include <stdio.h>
#include <fcntl.h>
#include <string.h>
#include <errno.h>

// Usage:
//
//    make_testdirs -d <n_dirs> -f <n_files> dirname
//
// create <n_dirs> directories directly under <dirname>.  Each directory
// contains <n_files> files.  Each file has a sparse file, with size in the
// range [0, 32).
//
// <n_files> and <n_dirs> both default to 1024.


#define MAX_PATHNAME  256

void usage(const char* progname) {
  printf("Usage: %s [ options ] dirname\n", progname);
  printf("options:\n");
  printf("  -d <n_dirs>    number of subdirs\n");
  printf("  -f <n_files>   number of files per subdir\n");
  printf("  -h             help\n");
}



int make_it(const char* dir,
            unsigned    n_dirs,
            unsigned    n_files) {

   unsigned int rand_state;

   char subdir_path[MAX_PATHNAME];
   unsigned  subdir;
   for (subdir=0; subdir<n_dirs; ++subdir) {
      int rc = snprintf(subdir_path, MAX_PATHNAME, "%s/dir%05u", dir, subdir);
      if ((rc < 0) || (rc >= MAX_PATHNAME)) {
         fprintf(stderr, "snprintf('%s/dir%05u') failed: %s\n", dir, subdir, strerror(errno));
         exit(-1);
      }

      printf("making %u files in subdir %s / %u\n", n_files, subdir_path, n_dirs);
      if (mkdir(subdir_path, 0777)) {
          if (errno != EEXIST) {
             fprintf(stderr, "mkdir(%s) failed: %s\n", subdir_path, strerror(errno));
             exit(-1);
          }
          else {
             printf("  skipping %s\n", subdir_path);
             continue;              // restarting? assume directory is already full
          }
      }

      char file_path[MAX_PATHNAME];
      unsigned  file;
      for (file=0; file<n_files; ++file) {
         int rc = snprintf(file_path, MAX_PATHNAME, "%s/file%05u", subdir_path, file);
         if ((rc < 0) || (rc >= MAX_PATHNAME)) {
            fprintf(stderr, "snprintf('%s/file%05u') failed: %s\n", subdir_path, file, strerror(errno));
            exit(-1);
         }

         // printf("file: %s\n", file_path);
         int fd = open(file_path, (O_WRONLY|O_CREAT|O_TRUNC), 0777);
         if (fd < 0) {
            fprintf(stderr, "open(%s) failed: %s\n", file_path, strerror(errno));
            exit(-1);
         }

         // int rnd = rand_r(&rand_state) % (32 * 1024); // random [0, 64k)
         int rnd = rand_r(&rand_state) % (32); // random [0, 32)
         ftruncate(fd, rnd);

         close(fd);
      }
   }

   return 0;
}

int main (int argc, char* argv[]) {

   unsigned n_dirs  = 1024;        // default
   unsigned n_files = 1024;        // default
   int      ch;
   int      help    = 0;
   int      err     = 0;
   char*    dir     = NULL;

# define PARSE_INT(VAR, STR)                       \
   do {                                            \
      errno = 0;                                   \
      VAR = strtol((STR), NULL, 10);               \
      if (errno) {                                 \
         printf("PARSE_INT(" #VAR ") failed\n");   \
         err = 1;                                  \
      }                                            \
   } while (0)


   while ((! err)
          && (ch = getopt(argc, argv, "hd:f:")) != -1) {

      switch (ch) {

      case 'd':  PARSE_INT(n_dirs, optarg);       break;
      case 'f':  PARSE_INT(n_files, optarg);       break;
      case 'h':  help = 1; break;

      case '?':
         // getopt returns '?' when there is a problem.
         printf ("\t unrecognized option '%s'\n", argv[optind -1]);
         err = 1;
         break;

      default:
         printf ("\t ?? getopt returned '%c' 0%o ??\n", (char)ch, ch);
         err = 1;
      }
   }


   if (help) {
      usage(argv[0]);
      exit(0);
   }
   if (err) {
      usage(argv[0]);
      exit(-1);
   }
   if (argc == 1) {
      usage(argv[0]);
      exit(-1);
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
