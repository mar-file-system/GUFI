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



#include <unistd.h>

#include <gtest/gtest.h>

#include "bf.h"
#include "dbutils.h"
#include "external.h"

static const std::string exec = "exec"; // argv[0]

static const std::string h = "-h";
static const std::string H = "-H";
static const std::string x = "-x";
static const std::string P = "-P";
static const std::string b = "-b";
static const std::string a = "-a";
static const std::string n = "-n"; static const std::string n_arg = "1";
static const std::string d = "-d"; static const std::string d_arg = "|";
static const std::string o = "-o"; static const std::string o_arg = "o arg";
static const std::string O = "-O"; static const std::string O_arg = "O arg";
static const std::string u = "-u";
static const std::string I = "-I"; static const std::string I_arg = "I arg";
static const std::string T = "-T"; static const std::string T_arg = "T arg";
static const std::string S = "-S"; static const std::string S_arg = "S arg";
static const std::string E = "-E"; static const std::string E_arg = "E arg";
static const std::string F = "-F"; static const std::string F_arg = "F arg";
static const std::string r = "-r";
static const std::string R = "-R";
static const std::string Y = "-Y";
static const std::string Z = "-Z";
static const std::string W = "-W"; static const std::string W_arg = "W arg";
static const std::string A = "-A"; static const std::string A_arg = "1";
static const std::string g = "-g"; static const std::string g_arg = "1";
static const std::string c = "-c"; static const std::string c_arg = "1";
static const std::string y = "-y"; static const std::string y_arg = "1";
static const std::string z = "-z"; static const std::string z_arg = "1";
static const std::string J = "-J"; static const std::string J_arg = "J arg";
static const std::string K = "-K"; static const std::string K_arg = "K arg";
static const std::string G = "-G"; static const std::string G_arg = "G arg";
static const std::string m = "-m";
static const std::string B = "-B"; static const std::string B_arg = "1";
static const std::string w = "-w";
static const std::string f = "-f"; static const std::string f_arg = "f arg";
static const std::string j = "-j";
static const std::string X = "-X";
static const std::string L = "-L"; static const std::string L_arg = "1";
static const std::string k = "-k"; static const std::string k_arg = "k arg";
static const std::string M = "-M"; static const std::string M_arg = "1";
static const std::string C = "-C"; static const std::string C_arg = "1";
#if HAVE_ZLIB
static const std::string e = "-e";
#endif
static const std::string q = "-q";
static const std::string Q = "-Q"; static const std::string Q_arg0 = "extdb";
                                   static const std::string Q_arg1 = "table";
                                   static const std::string Q_arg2 = "template.table";
                                   static const std::string Q_arg3 = "view";
static const std::string s = "-s"; static const std::string s_arg  = "s_arg";

static bool operator==(const refstr_t &refstr, const std::string &str) {
    if (refstr.len != str.size()) {
        return false;
    }

    if (!refstr.len && !refstr.data) {
        return true;
    }

    return (str.compare(0, refstr.len, refstr.data, refstr.len) == 0);
}

TEST(input, nullptr) {
    EXPECT_EQ(input_init(nullptr), nullptr);
    EXPECT_NO_THROW(input_fini(nullptr));
}

static void check_input(struct input *in, const bool helped,
                        const bool flags, const bool options) {
    EXPECT_EQ(in->helped,                             static_cast<int>(helped));

    EXPECT_EQ(in->process_xattrs,                     flags);
    EXPECT_EQ(in->printdir,                           flags);
    EXPECT_EQ(in->buildindex,                         flags);
    EXPECT_EQ(in->andor,                              flags);
    EXPECT_EQ(in->types.prefix,                       flags);
    EXPECT_EQ(in->insertfl,                           flags);
    EXPECT_EQ(in->insertdir,                          flags);
    EXPECT_EQ(in->suspectd,                           flags);
    EXPECT_EQ(in->suspectfl,                          flags);
    EXPECT_EQ(in->keep_matime,                        flags);
    EXPECT_EQ(in->open_flags,                         flags?SQLITE_OPEN_READWRITE:SQLITE_OPEN_READONLY);
    EXPECT_EQ(in->terse,                              flags);
    EXPECT_EQ(in->dry_run,                            flags);
    #if HAVE_ZLIB
    EXPECT_EQ(in->compress,                           flags);
    #endif
    EXPECT_EQ(in->check_extdb_valid,                  flags);

    if (options) {
        EXPECT_EQ(in->maxthreads,                     (std::size_t) 1);
        EXPECT_EQ(in->delim,                          '|');
        // not checking -o and -O here
        EXPECT_EQ(in->sql.init,                       I_arg);
        EXPECT_EQ(in->sql.tsum,                       T_arg);
        EXPECT_EQ(in->sql.sum,                        S_arg);
        EXPECT_EQ(in->sql.ent,                        E_arg);
        EXPECT_EQ(in->sql.fin,                        F_arg);
        EXPECT_EQ(in->insuspect.data,                 W_arg.c_str());
        EXPECT_EQ(in->suspectfile,                    1);
        EXPECT_EQ(in->suspectmethod,                  1);
        EXPECT_EQ(in->stride,                         1);
        EXPECT_EQ(in->suspecttime,                    1);
        EXPECT_EQ(in->min_level,                      (std::size_t) 1);
        EXPECT_EQ(in->max_level,                      (std::size_t) 1);
        EXPECT_EQ(in->sql.intermediate,               J_arg);
        EXPECT_EQ(in->sql.init_agg,                   K_arg);
        EXPECT_EQ(in->sql.agg,                        G_arg);
        EXPECT_EQ(in->output_buffer_size,             (std::size_t) 1);
        EXPECT_EQ(in->format,                         f_arg);
        EXPECT_EQ(in->max_in_dir,                     (std::size_t) 1);
        EXPECT_NE(in->skip,                           nullptr);
        EXPECT_EQ(in->target_memory_footprint,        (std::size_t) 1);
        EXPECT_EQ(in->subdir_limit,                   (std::size_t) 1);
        EXPECT_EQ(sll_get_size(&in->external_attach), (std::size_t) 1);

        sll_node_t *head = sll_head_node(&in->external_attach);
        EXPECT_NE(head,                               nullptr);
        eus_t *eus = (eus_t *) sll_node_data(head);
        EXPECT_NE(eus,                                nullptr);
        EXPECT_EQ(eus->basename.data,                 Q_arg0);
        EXPECT_EQ(eus->table.data,                    Q_arg1);
        EXPECT_EQ(eus->template_table.data,           Q_arg2);
        EXPECT_EQ(eus->view.data,                     Q_arg3);

        EXPECT_EQ(in->swap_prefix.data,               s_arg);
    }
    else {
        const std::string empty = "";

        EXPECT_EQ(in->maxthreads,                     (std::size_t) 1);
        EXPECT_EQ(in->delim,                          fielddelim);
        EXPECT_EQ(in->nameto,                         empty);
        EXPECT_EQ(in->name,                           empty);
        EXPECT_EQ(in->output,                         STDOUT);
        EXPECT_EQ(in->outname,                        empty);
        EXPECT_EQ(in->sql.init,                       empty);
        EXPECT_EQ(in->sql.tsum,                       empty);
        EXPECT_EQ(in->sql.sum,                        empty);
        EXPECT_EQ(in->sql.ent,                        empty);
        EXPECT_EQ(in->sql.fin,                        empty);
        EXPECT_EQ(in->insuspect.data,                 nullptr);
        EXPECT_EQ(in->suspectfile,                    0);
        EXPECT_EQ(in->suspectmethod,                  0);
        EXPECT_EQ(in->stride,                         0);
        EXPECT_EQ(in->suspecttime,                    0);
        EXPECT_EQ(in->min_level,                      (std::size_t) 0);
        EXPECT_EQ(in->max_level,                      (std::size_t) -1);
        EXPECT_EQ(in->sql.intermediate,               empty);
        EXPECT_EQ(in->sql.init_agg,                   empty);
        EXPECT_EQ(in->sql.agg,                        empty);
        EXPECT_EQ(in->output_buffer_size,             (std::size_t) 4096);
        EXPECT_EQ(in->format,                         empty);
        EXPECT_EQ(in->max_in_dir,                     (std::size_t) 0);
        EXPECT_NE(in->skip,                           nullptr);
        EXPECT_EQ(in->target_memory_footprint,        (std::size_t) 0);
        EXPECT_EQ(in->subdir_limit,                   (std::size_t) 0);
        EXPECT_EQ(sll_get_size(&in->external_attach), (std::size_t) 0);
        EXPECT_NE(in->swap_prefix.data,               nullptr); /* default exists */
    }
}

TEST(parse_cmd_line, help) {
    const char opts[] = "h";

    const char *argv[] = {
        exec.c_str(),
        h.c_str(),
    };

    int argc = sizeof(argv) / sizeof(argv[0]);

    const int fd = dup(STDOUT_FILENO);
    ASSERT_NE(fd, -1);
    EXPECT_EQ(close(STDOUT_FILENO), 0);;

    struct input in;
    EXPECT_EQ(parse_cmd_line(argc, (char **) argv, opts, 0, "", &in), -1);

    ASSERT_EQ(dup(fd), STDOUT_FILENO);
    EXPECT_EQ(close(fd), 0);

    check_input(&in, true, false, false);

    input_fini(&in);
}

TEST(parse_cmd_line, debug) {
    const char opts[] = "HxPban:d:i:t:o:O:uI:T:S:E:F:rRYZW:A:g:c:y:z:J:K:G:mB:wf:jXL:k:M:C:" COMPRESS_OPT "qQ:s:";

    const char *argv[] = {
        exec.c_str(),
        H.c_str(),
        x.c_str(),
        P.c_str(),
        b.c_str(),
        a.c_str(),
        n.c_str(), n_arg.c_str(),
        d.c_str(), d_arg.c_str(),
        u.c_str(),
        I.c_str(), I_arg.c_str(),
        T.c_str(), T_arg.c_str(),
        S.c_str(), S_arg.c_str(),
        E.c_str(), E_arg.c_str(),
        F.c_str(), F_arg.c_str(),
        r.c_str(),
        R.c_str(),
        Y.c_str(),
        Z.c_str(),
        W.c_str(), W_arg.c_str(),
        A.c_str(), A_arg.c_str(),
        g.c_str(), g_arg.c_str(),
        c.c_str(), c_arg.c_str(),
        y.c_str(), y_arg.c_str(),
        z.c_str(), z_arg.c_str(),
        J.c_str(), J_arg.c_str(),
        K.c_str(), K_arg.c_str(),
        G.c_str(), G_arg.c_str(),
        m.c_str(),
        B.c_str(), B_arg.c_str(),
        w.c_str(),
        f.c_str(), f_arg.c_str(),
        j.c_str(),
        X.c_str(),
        L.c_str(), L_arg.c_str(),
        // k.c_str(), k_arg.c_str(),
        M.c_str(), M_arg.c_str(),
        C.c_str(), C_arg.c_str(),
        #ifdef HAVE_ZLIB
        e.c_str(),
        #endif
        q.c_str(),
        Q.c_str(), Q_arg0.c_str(), Q_arg1.c_str(), Q_arg2.c_str(), Q_arg3.c_str(),
        s.c_str(), s_arg.c_str(),
    };

    int argc = sizeof(argv) / sizeof(argv[0]);

    const int fd = dup(STDOUT_FILENO);
    ASSERT_NE(fd, -1);
    close(STDOUT_FILENO);

    struct input in;
    EXPECT_EQ(parse_cmd_line(argc, (char **) argv, opts, 0, "", &in), -1);

    ASSERT_EQ(dup(fd), STDOUT_FILENO);
    EXPECT_EQ(close(fd), 0);

    check_input(&in, true, true, true);

    input_fini(&in);
}

TEST(parse_cmd_line, flags) {
    const char opts[] = "xPbaurRYZmwjX" COMPRESS_OPT "q";

    const char *argv[] = {
        exec.c_str(),
        x.c_str(),
        P.c_str(),
        b.c_str(),
        a.c_str(),
        u.c_str(),
        r.c_str(),
        R.c_str(),
        Y.c_str(),
        Z.c_str(),
        m.c_str(),
        w.c_str(),
        j.c_str(),
        X.c_str(),
        #ifdef HAVE_ZLIB
        e.c_str(),
        #endif
        q.c_str(),
    };

    int argc = sizeof(argv) / sizeof(argv[0]);

    struct input in;
    EXPECT_EQ(parse_cmd_line(argc, (char **) argv, opts, 0, "", &in), argc);
    check_input(&in, false, true, false);

    input_fini(&in);
}

TEST(parse_cmd_line, options) {
    const char opts[] = "n:d:i:t:I:T:S:E:F:W:A:g:c:y:z:J:K:G:B:f:L:k:M:C:Q:s:";

    const char *argv[] = {
        exec.c_str(),
        n.c_str(), n_arg.c_str(),
        d.c_str(), d_arg.c_str(),
        I.c_str(), I_arg.c_str(),
        T.c_str(), T_arg.c_str(),
        S.c_str(), S_arg.c_str(),
        E.c_str(), E_arg.c_str(),
        F.c_str(), F_arg.c_str(),
        W.c_str(), W_arg.c_str(),
        A.c_str(), A_arg.c_str(),
        g.c_str(), g_arg.c_str(),
        c.c_str(), c_arg.c_str(),
        y.c_str(), y_arg.c_str(),
        z.c_str(), z_arg.c_str(),
        J.c_str(), J_arg.c_str(),
        K.c_str(), K_arg.c_str(),
        G.c_str(), G_arg.c_str(),
        B.c_str(), B_arg.c_str(),
        f.c_str(), f_arg.c_str(),
        L.c_str(), L_arg.c_str(),
        // k.c_str(), k_arg.c_str(),
        M.c_str(), M_arg.c_str(),
        C.c_str(), C_arg.c_str(),
        Q.c_str(), Q_arg0.c_str(), Q_arg1.c_str(), Q_arg2.c_str(), Q_arg3.c_str(),
        s.c_str(), s_arg.c_str(),
    };

    int argc = sizeof(argv) / sizeof(argv[0]);

    struct input in;
    ASSERT_EQ(parse_cmd_line(argc, (char **) argv, opts, 0, "", &in), argc);
    check_input(&in, false, false, true);

    input_fini(&in);
}

TEST(parse_cmd_line, delimiter) {
    const char opts[] = "d:";

    // x -> \x1E
    {
        const char dx[] = "x";

        const char *argv[] = {
            exec.c_str(),
            d.c_str(), dx,
        };

        int argc = sizeof(argv) / sizeof(argv[0]);

        struct input in;
        ASSERT_EQ(parse_cmd_line(argc, (char **) argv, opts, 0, "", &in), argc);
        EXPECT_EQ(in.delim, fielddelim);

        input_fini(&in);
    }

    // others
    {
        const char dpipe[] = "|";

        const char *argv[] = {
            exec.c_str(),
            d.c_str(), dpipe,
        };

        int argc = sizeof(argv) / sizeof(argv[0]);

        struct input in;
        ASSERT_EQ(parse_cmd_line(argc, (char **) argv, opts, 0, "", &in), argc);
        EXPECT_EQ(in.delim, dpipe[0]);

        input_fini(&in);
    }
}

TEST(parse_cmd_line, output_arguments) {
    const char opts[] = "o:O:";

    // neither
    {
        const char *argv[] = {
            exec.c_str(),
        };

        int argc = sizeof(argv) / sizeof(argv[0]);

        struct input in;
        ASSERT_EQ(parse_cmd_line(argc, (char **) argv, opts, 0, "", &in), argc);
        EXPECT_EQ(in.output, STDOUT);

        input_fini(&in);
    }

    // -o
    {
        const char *argv[] = {
            exec.c_str(),
            o.c_str(), o_arg.c_str(),
        };

        int argc = sizeof(argv) / sizeof(argv[0]);

        struct input in;
        ASSERT_EQ(parse_cmd_line(argc, (char **) argv, opts, 0, "", &in), argc);
        EXPECT_EQ(in.output,  OUTFILE);
        EXPECT_EQ(in.outname, o_arg);

        input_fini(&in);
    }

    // -O
    {
        const char *argv[] = {
            exec.c_str(),
            O.c_str(), O_arg.c_str(),
        };

        int argc = sizeof(argv) / sizeof(argv[0]);

        struct input in;
        ASSERT_EQ(parse_cmd_line(argc, (char **) argv, opts, 0, "", &in), argc);
        EXPECT_EQ(in.output,  OUTDB);
        EXPECT_EQ(in.outname, O_arg);

        input_fini(&in);
    }

    // -o then -O
    {
        const char *argv[] = {
            exec.c_str(),
            o.c_str(), o_arg.c_str(),
            O.c_str(), O_arg.c_str(),
        };

        int argc = sizeof(argv) / sizeof(argv[0]);

        struct input in;
        ASSERT_EQ(parse_cmd_line(argc, (char **) argv, opts, 0, "", &in), argc);
        EXPECT_EQ(in.output,  OUTDB);
        EXPECT_EQ(in.outname, O_arg);

        input_fini(&in);
    }

    // -O then -o
    {
        const char *argv[] = {
            exec.c_str(),
            O.c_str(), O_arg.c_str(),
            o.c_str(), o_arg.c_str(),
        };

        int argc = sizeof(argv) / sizeof(argv[0]);

        struct input in;
        ASSERT_EQ(parse_cmd_line(argc, (char **) argv, opts, 0, "", &in), argc);
        EXPECT_EQ(in.output,  OUTFILE);
        EXPECT_EQ(in.outname, o_arg);

        input_fini(&in);
    }
}

TEST(parse_cmd_line, positional) {
    const std::string pos1 = "positional1";
    const std::string pos2 = "positional2";

    const char *argv[] = {
        exec.c_str(),
        pos1.c_str(),
        pos2.c_str(),
    };

    int argc = sizeof(argv) / sizeof(argv[0]);

    struct input in;
    // 1, since no options were read
    ASSERT_EQ(parse_cmd_line(argc, (char **) argv, "", 0, "", &in), 1);

    check_input(&in, false, false, false);

    input_fini(&in);
}

TEST(parse_cmd_line, unused) {
    #define UNUSED "0"

    const char opts[] = UNUSED;

    const char *argv[] = {
        exec.c_str(),
        "-" UNUSED,
    };

    int argc = sizeof(argv) / sizeof(argv[0]);

    struct input in;
    EXPECT_EQ(parse_cmd_line(argc, (char **) argv, opts, 0, "", &in), -1);

    check_input(&in, true, false, false);

    input_fini(&in);
}

TEST(parse_cmd_line, invalid) {
    #define INVALID ";"

    const char opts[] = INVALID;

    const char *argv[] = {
        exec.c_str(),
        "-" INVALID,
    };

    int argc = sizeof(argv) / sizeof(argv[0]);

    struct input in;
    EXPECT_EQ(parse_cmd_line(argc, (char **) argv, opts, 0, "", &in), -1);

    check_input(&in, true, false, false);

    input_fini(&in);
}

TEST(INSTALL, STR) {
    const std::string SOURCE = "INSTALL_STR";
    refstr_t dst;
    EXPECT_EQ(INSTALL_STR(&dst, SOURCE.c_str()), 0);
    EXPECT_EQ(dst, SOURCE);

    EXPECT_EQ(INSTALL_STR(&dst, nullptr), -1);
}

template <typename T>
void test_install_number(const std::string &name, const char *fmt,
                         void (* func)(T *, const char *, const T, const T, const char *, int *),
                         const T min, const T max,
                         const T test_ok, const T test_too_small, const T test_too_big) {
    T dst = 0;
    char buf[10];
    int retval = 0;
    const std::string fullname = "INSTALL_" + name;

    snprintf(buf, sizeof(buf), fmt, test_ok);
    func(&dst, buf, min, max, (fullname + " good test").c_str(), &retval);
    EXPECT_EQ(retval, 0);
    EXPECT_EQ(dst, test_ok);

    snprintf(buf, sizeof(buf), fmt, "");
    retval = 0;
    func(&dst, buf, min, max, (fullname + " bad input").c_str(), &retval);
    EXPECT_EQ(retval, -1);

    snprintf(buf, sizeof(buf), fmt, test_too_small);
    retval = 0;
    func(&dst, buf, min, max, (fullname + " too small test").c_str(), &retval);
    EXPECT_EQ(retval, -1);

    snprintf(buf, sizeof(buf), fmt, test_too_big);
    retval = 0;
    func(&dst, buf, min, max, (fullname + " too big test").c_str(), &retval);
    EXPECT_EQ(retval, -1);
}

TEST(INSTALL, INT) {
    test_install_number <int> ("INT", "%d", INSTALL_INT,
                               -1024, 1024,
                               123, -1234, 1234);
}

TEST(INSTALL, SIZE) {
    test_install_number <std::size_t> ("SIZE", "%zu", INSTALL_SIZE,
                                       1, 1024,
                                       123, 0, 1234);
}

TEST(INSTALL, UINT64) {
    test_install_number <uint64_t> ("UINT64", "%" PRIu64, INSTALL_UINT64,
                                    1, 1024,
                                    123, 0, 1234);
}
