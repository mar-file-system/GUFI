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
#include <sqlite3.h>

extern "C" {

#include "bf.h"

}

static const std::string exec = "exec"; // argv[0]

static const std::string h = "-h";
static const std::string H = "-H";
static const std::string x = "-x";
static const std::string p = "-p";
static const std::string P = "-P";
static const std::string N = "-N";
static const std::string V = "-V";
static const std::string b = "-b";
static const std::string a = "-a";
static const std::string n = "-n"; static const std::string n_arg = "1";
static const std::string d = "-d"; static const std::string d_arg = "|";
static const std::string i = "-i"; static const std::string i_arg = "i arg";
static const std::string t = "-t"; static const std::string t_arg = "t arg";
static const std::string o = "-o"; static const std::string o_arg = "o arg";
static const std::string O = "-O"; static const std::string O_arg = "O arg";
static const std::string I = "-I"; static const std::string I_arg = "I arg";
static const std::string T = "-T"; static const std::string T_arg = "T arg";
static const std::string S = "-S"; static const std::string S_arg = "S arg";
static const std::string E = "-E"; static const std::string E_arg = "E arg";
static const std::string F = "-F"; static const std::string F_arg = "F arg";
static const std::string r = "-r";
static const std::string R = "-R";
static const std::string D = "-D";
static const std::string Y = "-Y";
static const std::string Z = "-Z";
static const std::string W = "-W"; static const std::string W_arg = "W arg";
static const std::string A = "-A"; static const std::string A_arg = "1";
static const std::string g = "-g"; static const std::string g_arg = "1";
static const std::string c = "-c"; static const std::string c_arg = "1";
static const std::string u = "-u";
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

#define UNUSED ";"

static void check_input(struct input *in, const bool helped,
                        const bool flags, const bool options) {
    EXPECT_EQ(in->helped,                      static_cast<int>(helped));

    if (flags) {
        EXPECT_EQ(in->external_enabled,        1);
        EXPECT_EQ(in->printing,                1);
        EXPECT_EQ(in->printdir,                1);
        EXPECT_EQ(in->printheader,             1);
        EXPECT_EQ(in->printrows,               1);
        EXPECT_EQ(in->buildindex,              1);
        EXPECT_EQ(in->andor,                   1);
        EXPECT_EQ(in->insertfl,                1);
        EXPECT_EQ(in->insertdir,               1);
        EXPECT_EQ(in->dontdescend,             1);
        EXPECT_EQ(in->suspectd,                1);
        EXPECT_EQ(in->suspectfl,               1);
        EXPECT_EQ(in->infile,                  1);
        EXPECT_EQ(in->keep_matime,             1);
        EXPECT_EQ(in->open_flags,              SQLITE_OPEN_READWRITE);
        EXPECT_EQ(in->terse,                   1);
        EXPECT_EQ(in->dry_run,                 1);
        #ifdef HAVE_ZLIB
        EXPECT_EQ(in->compress,                1);
        #endif
    }
    else {
        EXPECT_EQ(in->external_enabled,        0);
        EXPECT_EQ(in->printing,                0);
        EXPECT_EQ(in->printdir,                0);
        EXPECT_EQ(in->printheader,             0);
        EXPECT_EQ(in->printrows,               0);
        EXPECT_EQ(in->buildindex,              0);
        EXPECT_EQ(in->andor,                   0);
        EXPECT_EQ(in->insertfl,                0);
        EXPECT_EQ(in->insertdir,               0);
        EXPECT_EQ(in->dontdescend,             0);
        EXPECT_EQ(in->suspectd,                0);
        EXPECT_EQ(in->suspectfl,               0);
        EXPECT_EQ(in->infile,                  0);
        EXPECT_EQ(in->keep_matime,             0);
        EXPECT_EQ(in->open_flags,              SQLITE_OPEN_READONLY);
        EXPECT_EQ(in->terse,                   0);
        EXPECT_EQ(in->dry_run,                 0);
        #ifdef HAVE_ZLIB
        EXPECT_EQ(in->compress,                0);
        #endif
    }

    if (options) {
        EXPECT_EQ(in->maxthreads,              1);
        EXPECT_EQ(in->delim,                   '|');
        EXPECT_EQ(in->nameto,                  t_arg.c_str());
        EXPECT_EQ(in->nameto_len,              t_arg.size());
        EXPECT_EQ(in->name,                    i_arg.c_str());
        EXPECT_EQ(in->name_len,                i_arg.size());
        // not checking -o and -O here
        EXPECT_EQ(in->sql.init,                I_arg.c_str());
        EXPECT_EQ(in->sql.init_len,            I_arg.size());
        EXPECT_EQ(in->sql.tsum,                T_arg.c_str());
        EXPECT_EQ(in->sql.tsum_len,            T_arg.size());
        EXPECT_EQ(in->sql.sum,                 S_arg.c_str());
        EXPECT_EQ(in->sql.sum_len,             S_arg.size());
        EXPECT_EQ(in->sql.ent,                 E_arg.c_str());
        EXPECT_EQ(in->sql.ent_len,             E_arg.size());
        EXPECT_EQ(in->sql.fin,                 F_arg.c_str());
        EXPECT_EQ(in->sql.fin_len,             F_arg.size());
        EXPECT_EQ(in->insuspect,               W_arg.c_str());
        EXPECT_EQ(in->suspectfile,             1);
        EXPECT_EQ(in->suspectmethod,           1);
        EXPECT_EQ(in->stride,                  1);
        EXPECT_EQ(in->suspecttime,             1);
        EXPECT_EQ(in->min_level,               (std::size_t) 1);
        EXPECT_EQ(in->max_level,               (std::size_t) 1);
        EXPECT_EQ(in->sql.intermediate,        J_arg.c_str());
        EXPECT_EQ(in->sql.intermediate_len,    J_arg.size());
        EXPECT_EQ(in->sql.init_agg,            K_arg.c_str());
        EXPECT_EQ(in->sql.init_agg_len,        K_arg.size());
        EXPECT_EQ(in->sql.agg,                 G_arg.c_str());
        EXPECT_EQ(in->sql.agg_len,             G_arg.size());
        EXPECT_EQ(in->output_buffer_size,      (std::size_t) 1);
        EXPECT_EQ(in->format,                  f_arg.c_str());
        EXPECT_EQ(in->max_in_dir,              (std::size_t) 1);
        EXPECT_EQ(in->skip,                    k_arg.c_str());
        EXPECT_EQ(in->target_memory_footprint, (std::size_t) 1);
        EXPECT_EQ(in->subdir_limit,            (std::size_t) 1);
    }
    else {
        EXPECT_EQ(in->maxthreads,              1);
        EXPECT_EQ(in->delim,                   fielddelim);
        EXPECT_EQ(in->nameto,                  nullptr);
        EXPECT_EQ(in->nameto_len,              (std::size_t) 0);
        EXPECT_EQ(in->name,                    nullptr);
        EXPECT_EQ(in->name_len,                (std::size_t) 0);
        EXPECT_EQ(in->output,                  STDOUT);
        EXPECT_EQ(in->outname,                 nullptr);
        EXPECT_EQ(in->outname_len,             (std::size_t) 0);
        EXPECT_EQ(in->sql.init,                nullptr);
        EXPECT_EQ(in->sql.init_len,            (std::size_t) 0);
        EXPECT_EQ(in->sql.tsum,                nullptr);
        EXPECT_EQ(in->sql.tsum_len,            (std::size_t) 0);
        EXPECT_EQ(in->sql.sum,                 nullptr);
        EXPECT_EQ(in->sql.sum_len,             (std::size_t) 0);
        EXPECT_EQ(in->sql.ent,                 nullptr);
        EXPECT_EQ(in->sql.ent_len,             (std::size_t) 0);
        EXPECT_EQ(in->sql.fin,                 nullptr);
        EXPECT_EQ(in->sql.fin_len,             (std::size_t) 0);
        EXPECT_EQ(in->insuspect,               nullptr);
        EXPECT_EQ(in->suspectfile,             0);
        EXPECT_EQ(in->suspectmethod,           0);
        EXPECT_EQ(in->stride,                  0);
        EXPECT_EQ(in->suspecttime,             0);
        EXPECT_EQ(in->min_level,               (std::size_t) 0);
        EXPECT_EQ(in->max_level,               (std::size_t) -1);
        EXPECT_EQ(in->sql.intermediate,        nullptr);
        EXPECT_EQ(in->sql.init_agg,            nullptr);
        EXPECT_EQ(in->sql.agg,                 nullptr);
        EXPECT_EQ(in->output_buffer_size,      (std::size_t) 4096);
        EXPECT_EQ(in->format,                  nullptr);
        EXPECT_EQ(in->max_in_dir,              (std::size_t) 0);
        EXPECT_EQ(in->skip,                    nullptr);
        EXPECT_EQ(in->target_memory_footprint, (std::size_t) 0);
        EXPECT_EQ(in->subdir_limit,            (std::size_t) 0);
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
}

TEST(parse_cmd_line, debug) {
    const char opts[] = "HxpPNVban:d:i:t:o:O:I:T:S:E:F:rRDYZW:A:g:c:uy:z:J:K:G:mB:wf:jXL:k:M:C:" COMPRESS_OPT;

    const char *argv[] = {
        exec.c_str(),
        H.c_str(),
        x.c_str(),
        p.c_str(),
        P.c_str(),
        N.c_str(),
        V.c_str(),
        b.c_str(),
        a.c_str(),
        n.c_str(), n_arg.c_str(),
        d.c_str(), d_arg.c_str(),
        i.c_str(), i_arg.c_str(),
        t.c_str(), t_arg.c_str(),
        I.c_str(), I_arg.c_str(),
        T.c_str(), T_arg.c_str(),
        S.c_str(), S_arg.c_str(),
        E.c_str(), E_arg.c_str(),
        F.c_str(), F_arg.c_str(),
        r.c_str(),
        R.c_str(),
        D.c_str(),
        Y.c_str(),
        Z.c_str(),
        W.c_str(), W_arg.c_str(),
        A.c_str(), A_arg.c_str(),
        g.c_str(), g_arg.c_str(),
        c.c_str(), c_arg.c_str(),
        u.c_str(),
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
        k.c_str(), k_arg.c_str(),
        M.c_str(), M_arg.c_str(),
        C.c_str(), C_arg.c_str(),
        #ifdef HAVE_ZLIB
        e.c_str(),
        #endif
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
}

TEST(parse_cmd_line, flags) {
    const char opts[] = "xpPNVbarRDYZumwjX" COMPRESS_OPT;

    const char *argv[] = {
        exec.c_str(),
        x.c_str(),
        p.c_str(),
        P.c_str(),
        N.c_str(),
        V.c_str(),
        b.c_str(),
        a.c_str(),
        r.c_str(),
        R.c_str(),
        D.c_str(),
        Y.c_str(),
        Z.c_str(),
        u.c_str(),
        m.c_str(),
        w.c_str(),
        j.c_str(),
        X.c_str(),
        #ifdef HAVE_ZLIB
        e.c_str(),
        #endif
    };

    int argc = sizeof(argv) / sizeof(argv[0]);

    struct input in;
    EXPECT_EQ(parse_cmd_line(argc, (char **) argv, opts, 0, "", &in), argc);
    check_input(&in, false, true, false);
}

TEST(parse_cmd_line, options) {
    const char opts[] = "n:d:i:t:I:T:S:E:F:W:A:g:c:y:z:J:K:G:B:f:L:k:M:C:";

    const char *argv[] = {
        exec.c_str(),
        n.c_str(), n_arg.c_str(),
        d.c_str(), d_arg.c_str(),
        i.c_str(), i_arg.c_str(),
        t.c_str(), t_arg.c_str(),
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
        k.c_str(), k_arg.c_str(),
        M.c_str(), M_arg.c_str(),
        C.c_str(), C_arg.c_str(),
    };

    int argc = sizeof(argv) / sizeof(argv[0]);

    struct input in;
    ASSERT_EQ(parse_cmd_line(argc, (char **) argv, opts, 0, "", &in), argc);
    check_input(&in, false, false, true);
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
        EXPECT_EQ(in.output,      STDOUT);
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
        EXPECT_EQ(in.output,      OUTFILE);
        EXPECT_EQ(in.outname,     o_arg.c_str());
        EXPECT_EQ(in.outname_len, o_arg.size());
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
        EXPECT_EQ(in.output,      OUTDB);
        EXPECT_EQ(in.outname,     O_arg.c_str());
        EXPECT_EQ(in.outname_len, O_arg.size());
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
        EXPECT_EQ(in.output,      OUTDB);
        EXPECT_EQ(in.outname,     O_arg.c_str());
        EXPECT_EQ(in.outname_len, O_arg.size());
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
        EXPECT_EQ(in.output,      OUTFILE);
        EXPECT_EQ(in.outname,     o_arg.c_str());
        EXPECT_EQ(in.outname_len, o_arg.size());
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
}

TEST(parse_cmd_line, invalid) {
    const char opts[] = UNUSED;

    const char *argv[] = {
        exec.c_str(),
        "-" UNUSED,
    };

    int argc = sizeof(argv) / sizeof(argv[0]);

    struct input in;
    EXPECT_EQ(parse_cmd_line(argc, (char **) argv, opts, 0, "", &in), -1);

    check_input(&in, true, false, false);
}

TEST(INSTALL_STR, good) {
    int retval = 0;
    const char SOURCE[] = "INSTALL_STR good test";
    char *dst = nullptr;
    INSTALL_STR(dst, SOURCE, MAXPATH, SOURCE);
    EXPECT_EQ(retval, 0);
    EXPECT_STREQ(SOURCE, dst);
}

TEST(INSTALL_STR, bad) {
    int retval = 0;
    const char SOURCE[] = "INSTALL_STR bad test";
    char *dst = nullptr;
    INSTALL_STR(dst, SOURCE, 1, SOURCE);
    EXPECT_EQ(retval, -1);
    EXPECT_STRNE(SOURCE, dst);
}

TEST(INSTALL_INT, good) {
    int retval = 0;
    int src = 123;
    int dst = 0;

    char buf[10];
    snprintf(buf, 10, "%d", src);
    INSTALL_INT(dst, buf, -1024, 1024, "INSTALL_INT good test");
    EXPECT_EQ(retval, 0);
    EXPECT_EQ(src, dst);
}

TEST(INSTALL_INT, too_small) {
    int retval = 0;
    int src = -1234;
    int dst = 0;

    char buf[10];
    snprintf(buf, 10, "%d", src);
    INSTALL_INT(dst, buf, -1024, 1024, "INSTALL_INT too small test");
    EXPECT_EQ(retval, -1);
}

TEST(INSTALL_INT, too_big) {
    int retval = 0;
    int src = 1234;
    int dst = 0;

    char buf[10];
    snprintf(buf, 10, "%d", src);
    INSTALL_INT(dst, buf, -1024, 1024, "INSTALL_INT too big test");
    EXPECT_EQ(retval, -1);
}

TEST(INSTALL_UINT, good) {
    int retval = 0;
    std::size_t src = 123;
    std::size_t dst = 0;

    char buf[10];
    snprintf(buf, 10, "%zu", src);
    INSTALL_UINT(dst, buf, (std::size_t) 1, (std::size_t) 1024, "INSTALL_UINT good test");
    EXPECT_EQ(retval, 0);
    EXPECT_EQ(src, dst);
}

TEST(INSTALL_UINT, too_small) {
    int retval = 0;
    std::size_t src = 0;
    std::size_t dst = 0;

    char buf[10];
    snprintf(buf, 10, "%zu", src);
    INSTALL_UINT(dst, buf, (std::size_t) 1, (std::size_t) 1024, "INSTALL_UINT too small test");
    EXPECT_EQ(retval, -1);
    EXPECT_EQ(dst, (std::size_t) 0);
}

TEST(INSTALL_UINT, too_big) {
    int retval = 0;
    std::size_t src = 1234;
    std::size_t dst = 0;

    char buf[10];
    snprintf(buf, 10, "%zu", src);
    INSTALL_UINT(dst, buf, (std::size_t) 1, (std::size_t) 1024, "INSTALL_UINT too big test");
    EXPECT_EQ(retval, -1);
    EXPECT_EQ(src, dst);
}
