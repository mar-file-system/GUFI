/*
This file is part of GUFI, which is part of MarFS.

© (or copyright) 2019. Triad National Security, LLC. All rights reserved.

This program was produced under U.S. Government contract 89233218CNA000001 for
Los Alamos National Laboratory (LANL), which is operated by Triad National
Security, LLC for the U.S. Department of Energy/National Nuclear Security
Administration.
This is open source software; you can redistribute it and/or modify it under
the terms of the public domain BSD License. If software is modified to produce
derivative works, such modified software should be clearly marked, so as not
to confuse it with the version available from LANL.

See LICENSE.txt in top-level directory for license terms.
*/



// members of struct stat have sizes that vary between OSX/Linux
#if defined(__APPLE__)
#  define STAT_ino    "llu"
#  define STAT_nlink  "hu"
#  define STAT_size   "lld"
#  define STAT_bsize  "d"
#  define STAT_blocks "lld"

// typedef uint32_t  STAT_size_t;
//#  define STAT_size_t  uint32_t

#elif defined(__linux__)
#  define STAT_ino    "lu"
#  define STAT_nlink  "lu"
#  define STAT_size   "ld"
#  define STAT_bsize  "ld"
#  define STAT_blocks "ld"

// typedef size_t    STAT_size_t;
//#  define STAT_size_t  size_t

#elif defined(__CYGWIN__)

#  define STAT_ino    "lu"
#  define STAT_nlink  "hu"
#  define STAT_size   "ld"
#  define STAT_bsize  "d"
#  define STAT_blocks "ld"

#endif
