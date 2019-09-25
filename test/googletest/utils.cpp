#include <climits>
#include <cstdlib>
#include <cstring>
#include <random>

#include <gtest/gtest.h>

extern "C" {

#include "utils.h"

}

TEST(SNFORMAT_S, concatenate) {
    char buf[1024];
    SNFORMAT_S(buf, 1024, 3, "A", (size_t) 1, "BC", (size_t) 2, "DEF", (size_t) 3);
    EXPECT_STREQ(buf, "ABCDEF");
}

TEST(SNFORMAT_S, fill_buffer) {
    char buf[1024];

    EXPECT_EQ(SNFORMAT_S(buf, 10, 1, "0123456789", (size_t) 10), (size_t) 9);
    EXPECT_STREQ(buf, "012345678");
    EXPECT_EQ(buf[9], '\0');

    EXPECT_EQ(SNFORMAT_S(buf, 5, 1, "0123456789", (size_t) 10), (size_t) 4);
    EXPECT_STREQ(buf, "0123");
    EXPECT_EQ(buf[4], '\0');
}

TEST(Trie, create_cleanup) {
    struct Trie *root = getNewTrieNode();
    ASSERT_NE(root, nullptr);

    cleanup(root);
}

TEST(Trie, insert_nullptr) {
    struct Trie *root = getNewTrieNode();
    ASSERT_NE(root, nullptr);

    insertll(&root, nullptr);

    EXPECT_EQ(searchll(root, nullptr), 0);

    cleanup(root);
}

TEST(Trie, insert_empty) {
    char buf[MAXPATH] = "";

    struct Trie *root = getNewTrieNode();
    ASSERT_NE(root, nullptr);

    insertll(&root, buf);

    EXPECT_EQ(searchll(root, buf), 1);

    cleanup(root);
}

TEST(Trie, searchll) {
    char buf[MAXPATH] = "1234";

    struct Trie *root = getNewTrieNode();
    ASSERT_NE(root, nullptr);

    insertll(&root, buf);

    EXPECT_EQ(searchll(root, buf), 1);

    cleanup(root);
}

TEST(Trie, haveChildren) {
    char buf[MAXPATH] = "1234";

    struct Trie *root = getNewTrieNode();
    ASSERT_NE(root, nullptr);

    EXPECT_EQ(haveChildren(root), 0);

    insertll(&root, buf);

    EXPECT_EQ(haveChildren(root), 1);

    cleanup(root);
}

TEST(Trie, deletionll) {
    char buf[MAXPATH] = "1234";

    struct Trie *root = getNewTrieNode();
    ASSERT_NE(root, nullptr);

    insertll(&root, buf);

    EXPECT_EQ(deletionll(&root, buf), 1);
    EXPECT_EQ(searchll(root, buf), 0);

    cleanup(root);
}

TEST(Trie, substring) {
    char sub[MAXPATH] = "12";
    char str[MAXPATH] = "1234";

    struct Trie *root = getNewTrieNode();
    ASSERT_NE(root, nullptr);

    insertll(&root, sub);
    EXPECT_EQ(searchll(root, sub), 1);

    insertll(&root, str);
    EXPECT_EQ(searchll(root, sub), 1);
    EXPECT_EQ(searchll(root, str), 1);

    cleanup(root);
}

TEST(summary, zeroit) {
    struct sum summary;
    ASSERT_EQ(zeroit(&summary), 0);

    EXPECT_EQ(summary.totfiles, 0);
    EXPECT_EQ(summary.totlinks, 0);
    EXPECT_EQ(summary.minuid, LLONG_MAX);
    EXPECT_EQ(summary.maxuid, LLONG_MIN);
    EXPECT_EQ(summary.mingid, LLONG_MAX);
    EXPECT_EQ(summary.maxgid, LLONG_MIN);
    EXPECT_EQ(summary.minsize, LLONG_MAX);
    EXPECT_EQ(summary.maxsize, LLONG_MIN);
    EXPECT_EQ(summary.totltk, 0);
    EXPECT_EQ(summary.totmtk, 0);
    EXPECT_EQ(summary.totltm, 0);
    EXPECT_EQ(summary.totmtm, 0);
    EXPECT_EQ(summary.totmtg, 0);
    EXPECT_EQ(summary.totmtt, 0);
    EXPECT_EQ(summary.totsize, 0);
    EXPECT_EQ(summary.minctime, LLONG_MAX);
    EXPECT_EQ(summary.maxctime, LLONG_MIN);
    EXPECT_EQ(summary.minmtime, LLONG_MAX);
    EXPECT_EQ(summary.maxmtime, LLONG_MIN);
    EXPECT_EQ(summary.minatime, LLONG_MAX);
    EXPECT_EQ(summary.maxatime, LLONG_MIN);
    EXPECT_EQ(summary.minblocks, LLONG_MAX);
    EXPECT_EQ(summary.maxblocks, LLONG_MIN);
    EXPECT_EQ(summary.setit, 0);
    EXPECT_EQ(summary.totxattr, 0);
    EXPECT_EQ(summary.totsubdirs, 0);
    EXPECT_EQ(summary.maxsubdirfiles, LLONG_MIN);
    EXPECT_EQ(summary.maxsubdirlinks, LLONG_MIN);
    EXPECT_EQ(summary.maxsubdirsize, LLONG_MIN);
    EXPECT_EQ(summary.mincrtime, LLONG_MAX);
    EXPECT_EQ(summary.maxcrtime, LLONG_MIN);
    EXPECT_EQ(summary.minossint1, LLONG_MAX);
    EXPECT_EQ(summary.maxossint1, LLONG_MIN);
    EXPECT_EQ(summary.totossint1, 0);
    EXPECT_EQ(summary.minossint2, LLONG_MAX);
    EXPECT_EQ(summary.maxossint2, LLONG_MIN);
    EXPECT_EQ(summary.totossint2, 0);
    EXPECT_EQ(summary.minossint3, LLONG_MAX);
    EXPECT_EQ(summary.maxossint3, LLONG_MIN);
    EXPECT_EQ(summary.totossint3, 0);
    EXPECT_EQ(summary.minossint4, LLONG_MAX);
    EXPECT_EQ(summary.maxossint4, LLONG_MIN);
    EXPECT_EQ(summary.totossint4, 0);
}

TEST(summary, sumit_file) {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution <> dist(1, INT_MAX);

    struct sum summary;
    ASSERT_EQ(zeroit(&summary), 0);

    // pretend everything had been set through previous operations
    summary.setit = 1;

    struct work pwork;
    memset(&pwork, 0, sizeof(pwork));

    SNPRINTF(pwork.type, 2, "%s", "f");
    pwork.statuso.st_uid    = dist(gen);
    pwork.statuso.st_gid    = dist(gen);
    pwork.statuso.st_size   = dist(gen);
    pwork.statuso.st_ctime  = dist(gen);
    pwork.statuso.st_mtime  = dist(gen);
    pwork.statuso.st_atime  = dist(gen);
    pwork.statuso.st_blocks = dist(gen);
    pwork.pinode            = dist(gen);
    pwork.crtime            = dist(gen);
    pwork.ossint1           = dist(gen);
    pwork.ossint2           = dist(gen);
    pwork.ossint3           = dist(gen);
    pwork.ossint4           = dist(gen);
    pwork.xattrs            = dist(gen);

    ASSERT_EQ(sumit(&summary, &pwork), 0);

    EXPECT_EQ(summary.totfiles,   1);
    EXPECT_EQ(summary.totlinks,   0);
    EXPECT_EQ(summary.minblocks,  pwork.statuso.st_blocks);
    EXPECT_EQ(summary.maxblocks,  pwork.statuso.st_blocks);
    EXPECT_EQ(summary.minuid,     pwork.statuso.st_uid);
    EXPECT_EQ(summary.maxuid,     pwork.statuso.st_uid);
    EXPECT_EQ(summary.mingid,     pwork.statuso.st_gid);
    EXPECT_EQ(summary.maxgid,     pwork.statuso.st_gid);
    EXPECT_EQ(summary.minsize,    pwork.statuso.st_size);
    EXPECT_EQ(summary.maxsize,    pwork.statuso.st_size);
    EXPECT_EQ(summary.minctime,   pwork.statuso.st_ctime);
    EXPECT_EQ(summary.maxctime,   pwork.statuso.st_ctime);
    EXPECT_EQ(summary.minmtime,   pwork.statuso.st_mtime);
    EXPECT_EQ(summary.maxmtime,   pwork.statuso.st_mtime);
    EXPECT_EQ(summary.minatime,   pwork.statuso.st_atime);
    EXPECT_EQ(summary.maxatime,   pwork.statuso.st_atime);
    EXPECT_EQ(summary.mincrtime,  pwork.crtime);
    EXPECT_EQ(summary.maxcrtime,  pwork.crtime);
    EXPECT_EQ(summary.minossint1, pwork.ossint1);
    EXPECT_EQ(summary.maxossint1, pwork.ossint1);
    EXPECT_EQ(summary.minossint2, pwork.ossint2);
    EXPECT_EQ(summary.maxossint2, pwork.ossint2);
    EXPECT_EQ(summary.minossint3, pwork.ossint3);
    EXPECT_EQ(summary.maxossint3, pwork.ossint3);
    EXPECT_EQ(summary.minossint4, pwork.ossint4);
    EXPECT_EQ(summary.maxossint4, pwork.ossint4);
    EXPECT_EQ(summary.setit,      1);
    EXPECT_EQ(summary.totxattr,   1);
}

TEST(summary, sumit_link) {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution <> dist(1, INT_MAX);

    struct sum summary;
    ASSERT_EQ(zeroit(&summary), 0);

    // pretend everything had been set through previous operations
    summary.setit = 1;

    struct work pwork;
    memset(&pwork, 0, sizeof(pwork));

    SNPRINTF(pwork.type, 2, "%s", "l");
    pwork.statuso.st_uid    = dist(gen);
    pwork.statuso.st_gid    = dist(gen);
    pwork.statuso.st_size   = dist(gen);
    pwork.statuso.st_ctime  = dist(gen);
    pwork.statuso.st_mtime  = dist(gen);
    pwork.statuso.st_atime  = dist(gen);
    pwork.statuso.st_blocks = dist(gen);
    pwork.pinode            = dist(gen);
    pwork.crtime            = dist(gen);
    pwork.ossint1           = dist(gen);
    pwork.ossint2           = dist(gen);
    pwork.ossint3           = dist(gen);
    pwork.ossint4           = dist(gen);
    pwork.xattrs            = dist(gen);

    ASSERT_EQ(sumit(&summary, &pwork), 0);

    EXPECT_EQ(summary.totfiles,   0);
    EXPECT_EQ(summary.totlinks,   1);
    EXPECT_EQ(summary.minblocks,  LLONG_MAX);
    EXPECT_EQ(summary.maxblocks,  LLONG_MIN);
    EXPECT_EQ(summary.minuid,     pwork.statuso.st_uid);
    EXPECT_EQ(summary.maxuid,     pwork.statuso.st_uid);
    EXPECT_EQ(summary.mingid,     pwork.statuso.st_gid);
    EXPECT_EQ(summary.maxgid,     pwork.statuso.st_gid);
    EXPECT_EQ(summary.minsize,    LLONG_MAX);
    EXPECT_EQ(summary.maxsize,    LLONG_MIN);
    EXPECT_EQ(summary.minctime,   pwork.statuso.st_ctime);
    EXPECT_EQ(summary.maxctime,   pwork.statuso.st_ctime);
    EXPECT_EQ(summary.minmtime,   pwork.statuso.st_mtime);
    EXPECT_EQ(summary.maxmtime,   pwork.statuso.st_mtime);
    EXPECT_EQ(summary.minatime,   pwork.statuso.st_atime);
    EXPECT_EQ(summary.maxatime,   pwork.statuso.st_atime);
    EXPECT_EQ(summary.mincrtime,  pwork.crtime);
    EXPECT_EQ(summary.maxcrtime,  pwork.crtime);
    EXPECT_EQ(summary.minossint1, pwork.ossint1);
    EXPECT_EQ(summary.maxossint1, pwork.ossint1);
    EXPECT_EQ(summary.minossint2, pwork.ossint2);
    EXPECT_EQ(summary.maxossint2, pwork.ossint2);
    EXPECT_EQ(summary.minossint3, pwork.ossint3);
    EXPECT_EQ(summary.maxossint3, pwork.ossint3);
    EXPECT_EQ(summary.minossint4, pwork.ossint4);
    EXPECT_EQ(summary.maxossint4, pwork.ossint4);
    EXPECT_EQ(summary.setit,      1);
    EXPECT_EQ(summary.totxattr,   1);
}

TEST(summary, tsumit) {
    struct sum in;
    zeroit(&in);

    struct sum out;
    zeroit(&out);

    // values that get summed
    in.totfiles   = 1; out.totfiles   = 1;
    in.totlinks   = 1; out.totlinks   = 1;
    in.totltk     = 1; out.totltk     = 1;
    in.totmtk     = 1; out.totmtk     = 1;
    in.totltm     = 1; out.totltm     = 1;
    in.totmtm     = 1; out.totmtm     = 1;
    in.totmtg     = 1; out.totmtg     = 1;
    in.totmtt     = 1; out.totmtt     = 1;
    in.totsize    = 1; out.totsize    = 1;
    in.totxattr   = 1; out.totxattr   = 1;
    in.totossint1 = 1; out.totossint1 = 1;
    in.totossint2 = 1; out.totossint2 = 1;
    in.totossint3 = 1; out.totossint3 = 1;
    in.totossint4 = 1; out.totossint4 = 1;

    // all of out gets replaced
    out.maxsubdirfiles = 0;
    out.maxsubdirlinks = 0;
    out.maxsubdirsize  = 0;

    in.minuid     = 0; out.minuid     = 1;
    in.maxuid     = 1; out.maxuid     = 0;
    in.mingid     = 0; out.mingid     = 1;
    in.maxgid     = 1; out.maxgid     = 0;
    in.minsize    = 0; out.minsize    = 1;
    in.maxsize    = 1; out.maxsize    = 0;
    in.minblocks  = 0; out.minblocks  = 1;
    in.maxblocks  = 1; out.maxblocks  = 0;
    in.minctime   = 0; out.minctime   = 1;
    in.maxctime   = 1; out.maxctime   = 0;
    in.minmtime   = 0; out.minmtime   = 1;
    in.maxmtime   = 1; out.maxmtime   = 0;
    in.minatime   = 0; out.minatime   = 1;
    in.maxatime   = 1; out.maxatime   = 0;
    in.mincrtime  = 0; out.mincrtime  = 1;
    in.maxcrtime  = 1; out.maxcrtime  = 0;
    in.minossint1 = 0; out.minossint1 = 1;
    in.maxossint1 = 1; out.maxossint1 = 0;
    in.minossint2 = 0; out.minossint2 = 1;
    in.maxossint2 = 1; out.maxossint2 = 0;
    in.minossint3 = 0; out.minossint3 = 1;
    in.maxossint3 = 1; out.maxossint3 = 0;
    in.minossint4 = 0; out.minossint4 = 1;
    in.maxossint4 = 1; out.maxossint4 = 0;

    ASSERT_EQ(tsumit(&in, &out), 0);

    EXPECT_EQ(out.maxsubdirfiles, 1);
    EXPECT_EQ(out.maxsubdirlinks, 1);
    EXPECT_EQ(out.maxsubdirsize,  1);

    EXPECT_EQ(out.totfiles,   2);
    EXPECT_EQ(out.totlinks,   2);
    EXPECT_EQ(out.totltk,     2);
    EXPECT_EQ(out.totmtk,     2);
    EXPECT_EQ(out.totltm,     2);
    EXPECT_EQ(out.totmtk,     2);
    EXPECT_EQ(out.totltm,     2);
    EXPECT_EQ(out.totmtm,     2);
    EXPECT_EQ(out.totmtg,     2);
    EXPECT_EQ(out.totmtt,     2);
    EXPECT_EQ(out.totsize,    2);
    EXPECT_EQ(out.totxattr,   2);
    EXPECT_EQ(out.totossint1, 2);
    EXPECT_EQ(out.totossint2, 2);
    EXPECT_EQ(out.totossint3, 2);
    EXPECT_EQ(out.totossint4, 2);

    EXPECT_EQ(in.minuid,     out.minuid);
    EXPECT_EQ(in.maxuid,     out.maxuid);
    EXPECT_EQ(in.mingid,     out.mingid);
    EXPECT_EQ(in.maxgid,     out.maxgid);
    EXPECT_EQ(in.minsize,    out.minsize);
    EXPECT_EQ(in.maxsize,    out.maxsize);
    EXPECT_EQ(in.minblocks,  out.minblocks);
    EXPECT_EQ(in.maxblocks,  out.maxblocks);
    EXPECT_EQ(in.minctime,   out.minctime);
    EXPECT_EQ(in.maxctime,   out.maxctime);
    EXPECT_EQ(in.minmtime,   out.minmtime);
    EXPECT_EQ(in.maxmtime,   out.maxmtime);
    EXPECT_EQ(in.minatime,   out.minatime);
    EXPECT_EQ(in.maxatime,   out.maxatime);
    EXPECT_EQ(in.mincrtime,  out.mincrtime);
    EXPECT_EQ(in.maxcrtime,  out.maxcrtime);
    EXPECT_EQ(in.minossint1, out.minossint1);
    EXPECT_EQ(in.maxossint1, out.maxossint1);
    EXPECT_EQ(in.minossint2, out.minossint2);
    EXPECT_EQ(in.maxossint2, out.maxossint2);
    EXPECT_EQ(in.minossint3, out.minossint3);
    EXPECT_EQ(in.maxossint3, out.maxossint3);
    EXPECT_EQ(in.minossint4, out.minossint4);
    EXPECT_EQ(in.maxossint4, out.maxossint4);
}

#define PATH "a/b/c/d"
#define LAST "e"
#define SRC PATH "/" LAST

TEST(shortpath, file) {
    char dst[MAXPATH];
    char endname[MAXPATH];

    EXPECT_EQ(shortpath(SRC, dst, endname), 0);
    EXPECT_STREQ(PATH, dst);
    EXPECT_STREQ(LAST, endname);
}

TEST(shortpath, directory) {
    char dst[MAXPATH];
    char endname[MAXPATH];

    EXPECT_EQ(shortpath(SRC "/", dst, endname), 0);
    EXPECT_STREQ(SRC, dst);
    EXPECT_STREQ("", endname);
}

TEST(dupdir, parentfirst) {
    char parent[] = "/tmp/parent";
    struct stat orig_parent_stat;
    orig_parent_stat.st_mode = S_IRWXU; // 700
    orig_parent_stat.st_uid = geteuid();
    orig_parent_stat.st_gid = getegid();

    char child[] = "/tmp/parent/child";
    struct stat orig_child_stat;
    orig_child_stat.st_mode = S_IRWXU | S_IRWXG | S_IRWXO; // 777
    orig_child_stat.st_uid = geteuid();
    orig_child_stat.st_gid = getegid();

    // remove leftover directories
    rmdir(child);
    rmdir(parent);

    ASSERT_EQ(dupdir(parent, &orig_parent_stat), 0);
    ASSERT_EQ(dupdir(child,  &orig_child_stat),  0);

    struct stat parent_stat;
    struct stat child_stat;
    ASSERT_EQ(stat(parent, &parent_stat), 0);
    ASSERT_EQ(stat(child,  &child_stat),  0);

    // clean up
    EXPECT_EQ(rmdir(child),  0);
    EXPECT_EQ(rmdir(parent), 0);

    EXPECT_EQ(orig_parent_stat.st_mode | S_IFDIR, parent_stat.st_mode);
    EXPECT_EQ(orig_parent_stat.st_uid,            parent_stat.st_uid);
    EXPECT_EQ(orig_parent_stat.st_gid,            parent_stat.st_gid);

    EXPECT_EQ(orig_child_stat.st_mode | S_IFDIR,  child_stat.st_mode);
    EXPECT_EQ(orig_child_stat.st_uid,             child_stat.st_uid);
    EXPECT_EQ(orig_child_stat.st_gid,             child_stat.st_gid);
}

TEST(dupdir, childtfirst) {
    char parent[] = "/tmp/parent";
    struct stat orig_parent_stat;
    orig_parent_stat.st_mode = S_IRWXU; // 700
    orig_parent_stat.st_uid = geteuid();
    orig_parent_stat.st_gid = getegid();

    char child[] = "/tmp/parent/child";
    struct stat orig_child_stat;
    orig_child_stat.st_mode = S_IRWXU | S_IRWXG | S_IRWXO; // 777
    orig_child_stat.st_uid = geteuid();
    orig_child_stat.st_gid = getegid();

    // remove leftover directories
    rmdir(child);
    rmdir(parent);

    const mode_t prev_umask = umask(0);

    ASSERT_EQ(dupdir(child,  &orig_child_stat),  0);

    // parent path should have the same permissions as the child path
    {
        struct stat child_stat;
        struct stat parent_stat;
        ASSERT_EQ(stat(child,  &child_stat),  0);
        ASSERT_EQ(stat(parent, &parent_stat), 0);

        EXPECT_EQ(orig_child_stat.st_mode | S_IFDIR, child_stat.st_mode);
        EXPECT_EQ(orig_child_stat.st_uid,            child_stat.st_uid);
        EXPECT_EQ(orig_child_stat.st_gid,            child_stat.st_gid);

        EXPECT_EQ(orig_child_stat.st_mode | S_IFDIR, parent_stat.st_mode);
        EXPECT_EQ(orig_child_stat.st_uid,            parent_stat.st_uid);
        EXPECT_EQ(orig_child_stat.st_gid,            parent_stat.st_gid);

    }

    ASSERT_EQ(dupdir(parent, &orig_parent_stat), 0);

    struct stat parent_stat;
    struct stat child_stat;
    ASSERT_EQ(stat(parent, &parent_stat), 0);
    ASSERT_EQ(stat(child,  &child_stat),  0);

    // clean up
    umask(prev_umask);
    EXPECT_EQ(rmdir(child),  0);
    EXPECT_EQ(rmdir(parent), 0);

    EXPECT_EQ(orig_parent_stat.st_mode | S_IFDIR, parent_stat.st_mode);
    EXPECT_EQ(orig_parent_stat.st_uid,            parent_stat.st_uid);
    EXPECT_EQ(orig_parent_stat.st_gid,            parent_stat.st_gid);

    EXPECT_EQ(orig_child_stat.st_mode | S_IFDIR,  child_stat.st_mode);
    EXPECT_EQ(orig_child_stat.st_uid,             child_stat.st_uid);
    EXPECT_EQ(orig_child_stat.st_gid,             child_stat.st_gid);
}

TEST(mkpath, parentfirst) {
    char parent[] = "/tmp/parent";
    struct stat orig_parent_stat;
    orig_parent_stat.st_mode = S_IRWXU; // 700
    orig_parent_stat.st_uid = geteuid();
    orig_parent_stat.st_gid = getegid();

    char child[] = "/tmp/parent/child";
    struct stat orig_child_stat;
    orig_child_stat.st_mode = S_IRWXU | S_IRWXG | S_IRWXO; // 777
    orig_child_stat.st_uid = geteuid();
    orig_child_stat.st_gid = getegid();

    // remove leftover directories
    rmdir(child);
    rmdir(parent);

    const mode_t prev_umask = umask(0);
    ASSERT_EQ(mkdir (parent, orig_parent_stat.st_mode), 0);
    ASSERT_EQ(mkpath(child,  orig_child_stat.st_mode),  0);

    struct stat parent_stat;
    struct stat child_stat;
    ASSERT_EQ(stat(parent, &parent_stat), 0);
    ASSERT_EQ(stat(child,  &child_stat),  0);

    // clean up
    umask(prev_umask);
    EXPECT_EQ(rmdir(child),  0);
    EXPECT_EQ(rmdir(parent), 0);

    EXPECT_EQ(orig_parent_stat.st_mode | S_IFDIR, parent_stat.st_mode);
    EXPECT_EQ(orig_parent_stat.st_uid,            parent_stat.st_uid);
    EXPECT_EQ(orig_parent_stat.st_gid,            parent_stat.st_gid);

    EXPECT_EQ(orig_child_stat.st_mode | S_IFDIR,  child_stat.st_mode);
    EXPECT_EQ(orig_child_stat.st_uid,             child_stat.st_uid);
    EXPECT_EQ(orig_child_stat.st_gid,             child_stat.st_gid);
}

TEST(mkpath, childfirst) {
    char parent[] = "/tmp/parent";
    struct stat orig_parent_stat;
    orig_parent_stat.st_mode = S_IRWXU; // 700
    orig_parent_stat.st_uid = geteuid();
    orig_parent_stat.st_gid = getegid();

    char child[] = "/tmp/parent/child";
    struct stat orig_child_stat;
    orig_child_stat.st_mode = S_IRWXU | S_IRWXG | S_IRWXO; // 777
    orig_child_stat.st_uid = geteuid();
    orig_child_stat.st_gid = getegid();

    // remove leftover directories
    rmdir(child);
    rmdir(parent);

    const mode_t prev_umask = umask(0);
    ASSERT_EQ(mkpath(child,  orig_child_stat.st_mode),   0);
    ASSERT_EQ(mkpath(parent, orig_parent_stat.st_mode),  -1); // EEXISTS

    struct stat parent_stat;
    struct stat child_stat;
    ASSERT_EQ(stat(parent, &parent_stat), 0);
    ASSERT_EQ(stat(child,  &child_stat),  0);

    // clean up
    umask(prev_umask);
    EXPECT_EQ(rmdir(child),  0);
    EXPECT_EQ(rmdir(parent), 0);

    EXPECT_EQ(orig_child_stat.st_mode | S_IFDIR, parent_stat.st_mode);
    EXPECT_EQ(orig_child_stat.st_uid,            parent_stat.st_uid);
    EXPECT_EQ(orig_child_stat.st_gid,            parent_stat.st_gid);

    EXPECT_EQ(orig_child_stat.st_mode | S_IFDIR, child_stat.st_mode);
    EXPECT_EQ(orig_child_stat.st_uid,            child_stat.st_uid);
    EXPECT_EQ(orig_child_stat.st_gid,            child_stat.st_gid);
}
