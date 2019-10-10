#include <gtest/gtest.h>

#ifndef _POSIX_C_SOURCE
#define _POSIX_C_SOURCE 200809L
#endif

#include <cstdio>

extern "C" {
#include "config.h"
#include "trace.h"
}

static char delim[] = "\x1e";

static struct work * get_work() {
    struct work * work = new struct work;
    snprintf(work->name,     sizeof(work->name), "name");
    snprintf(work->type,     sizeof(work->type), "t");
    snprintf(work->linkname, sizeof(work->linkname), "link");
    work->statuso.st_ino     = 0xfedc;
    work->statuso.st_mode    = 0777;
    work->statuso.st_nlink   = 2048;
    work->statuso.st_uid     = 1001;
    work->statuso.st_gid     = 1001;
    work->statuso.st_size    = 4096;
    work->statuso.st_blksize = 4096;
    work->statuso.st_blocks  = 1;
    work->statuso.st_atime   = 0x1234;
    work->statuso.st_mtime   = 0x5678;
    work->statuso.st_ctime   = 0x9abc;
    snprintf(work->xattr,    sizeof(work->xattr), "xattr");
    work->crtime             = 0x9abc;
    work->ossint1            = 1;
    work->ossint2            = 2;
    work->ossint3            = 3;
    work->ossint4            = 4;
    snprintf(work->osstext1, sizeof(work->osstext1), "osstext1");
    snprintf(work->osstext2, sizeof(work->osstext2), "osstext2");
    work->pinode             = 0xdef0;
    return work;
}

static int to_string(char * line, const size_t size, struct work * work) {
    return snprintf(line, size,
                    "%s%c"
                    "%c%c"
                    "%" STAT_ino "%c"
                    "%d%c"
                    "%" STAT_nlink"%c"
                    "%d%c"
                    "%d%c"
                    "%" STAT_size "%c"
                    "%" STAT_bsize "%c"
                    "%" STAT_blocks "%c"
                    "%ld%c"
                    "%ld%c"
                    "%ld%c"
                    "%s%c"
                    "%s%c"
                    "%d%c"
                    "%d%c"
                    "%d%c"
                    "%d%c"
                    "%d%c"
                    "%s%c"
                    "%s%c"
                    "%lld%c"
                    "\n",
                    work->name,               delim[0],
                    work->type[0],            delim[0],
                    work->statuso.st_ino,     delim[0],
                    work->statuso.st_mode,    delim[0],
                    work->statuso.st_nlink,   delim[0],
                    work->statuso.st_uid,     delim[0],
                    work->statuso.st_gid,     delim[0],
                    work->statuso.st_size,    delim[0],
                    work->statuso.st_blksize, delim[0],
                    work->statuso.st_blocks,  delim[0],
                    work->statuso.st_atime,   delim[0],
                    work->statuso.st_mtime,   delim[0],
                    work->statuso.st_ctime,   delim[0],
                    work->linkname,           delim[0],
                    work->xattr,              delim[0],
                    work->crtime,             delim[0],
                    work->ossint1,            delim[0],
                    work->ossint2,            delim[0],
                    work->ossint3,            delim[0],
                    work->ossint4,            delim[0],
                    work->osstext1,           delim[0],
                    work->osstext2,           delim[0],
                    work->pinode,             delim[0]);
}

TEST(trace, worktofile) {
    struct work * work = get_work();
    ASSERT_NE(work, nullptr);

    // write a known struct to a file
    char buf[4096];
    FILE * file = fmemopen(buf, sizeof(buf), "w");
    ASSERT_NE(file, nullptr);

    const int written = worktofile(file, delim, work);
    fclose(file);

    ASSERT_GT(written, 0);
    ASSERT_LT(written, (int) sizeof(buf));

    // generate the string to compare with
    char line[4096];
    const int rc = to_string(line, sizeof(line), work);
    delete work;

    ASSERT_GT(rc, -1);
    ASSERT_LT(rc, (int) sizeof(line));

    EXPECT_STREQ(buf, line);
}

#define COMPARE(src, dst)                                          \
    EXPECT_STREQ(dst.name,            src->name);                  \
    EXPECT_STREQ(dst.type,            src->type);                  \
    EXPECT_EQ(dst.statuso.st_ino,     src->statuso.st_ino);        \
    EXPECT_EQ(dst.statuso.st_mode,    src->statuso.st_mode);       \
    EXPECT_EQ(dst.statuso.st_nlink,   src->statuso.st_nlink);      \
    EXPECT_EQ(dst.statuso.st_uid,     src->statuso.st_uid);        \
    EXPECT_EQ(dst.statuso.st_gid,     src->statuso.st_gid);        \
    EXPECT_EQ(dst.statuso.st_size,    src->statuso.st_size);       \
    EXPECT_EQ(dst.statuso.st_blksize, src->statuso.st_blksize);    \
    EXPECT_EQ(dst.statuso.st_blocks,  src->statuso.st_blocks);     \
    EXPECT_EQ(dst.statuso.st_atime,   src->statuso.st_atime);      \
    EXPECT_EQ(dst.statuso.st_mtime,   src->statuso.st_mtime);      \
    EXPECT_EQ(dst.statuso.st_ctime,   src->statuso.st_ctime);      \
    EXPECT_STREQ(dst.linkname,        src->linkname);              \
    EXPECT_STREQ(dst.xattr,           src->xattr);                 \
    EXPECT_EQ(dst.crtime,             src->crtime);                \
    EXPECT_EQ(dst.ossint1,            src->ossint1);               \
    EXPECT_EQ(dst.ossint2,            src->ossint2);               \
    EXPECT_EQ(dst.ossint3,            src->ossint3);               \
    EXPECT_EQ(dst.ossint4,            src->ossint4);               \
    EXPECT_STREQ(dst.osstext1,        src->osstext1);              \
    EXPECT_STREQ(dst.osstext2,        src->osstext2);              \
    EXPECT_EQ(dst.pinode,             src->pinode);                \

TEST(trace, filetowork) {
    struct work * src = get_work();
    ASSERT_NE(src, nullptr);

    // write a known struct using an alternative write function and convert the buffer to a file
    char buf[4096];
    const int rc = to_string(buf, sizeof(buf), src);
    ASSERT_GT(rc, -1);
    ASSERT_LT(rc, (int) sizeof(buf));

    FILE * file = fmemopen(buf, sizeof(buf), "r");
    ASSERT_NE(file, nullptr);

    // extract the data from the file
    struct work work;
    const int read = filetowork(file, delim, &work);
    fclose(file);

    EXPECT_EQ(read, 0);

    COMPARE(src, work);

    delete src;
}

TEST(trace, linetowork) {
    struct work * src = get_work();

    // write the known struct to a string using an alternative write function
    char line[4096];
    const int rc = to_string(line, sizeof(line), src);
    ASSERT_GT(rc, -1);
    ASSERT_LT(rc, (int) sizeof(line));

    // read the string
    struct work work;
    EXPECT_EQ(linetowork(line, delim, &work), 0);

    COMPARE(src, work);

    delete src;
}
