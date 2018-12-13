# DFW       = dfw
BFW       = bfwi bfti bfq bfwreaddirplus2db
TOOLS     = querydb querydbn make_testdirs make_testtree sqlite3-pcre/pcre.so

.PHONY: sqlite3-pcre googletest test

SQLITE3_PCRE      ?= sqlite3-pcre
GTEST_SRC         ?= $(realpath googletest)
GTEST_BUILD_DIR   ?= $(GTEST_SRC)/build
GTEST_INSTALL_DIR ?= $(GTEST_SRC)/install

MYSQL_FILES = bfmi

# bffuse:        run query under fuse
# bfresultfuse:  fuse-view of query-results in a DB
# FUSE_FILES    = bffuse bfresultfuse
FUSE_FILES    = bfresultfuse



all:   bfw tools

bfw:   $(BFW)
tools: $(TOOLS)

$(SQLITE3_PCRE)/pcre.so:
	$(MAKE) -C $(SQLITE3_PCRE)

# obsolete?
# dfw:   $(DFW)

# recurse with set-up for special build
fuse:
	$(MAKE) -C . $(FUSE_FILES) FUSE=1

# recurse with set-up for special build
mysql:
	$(MAKE) -C . $(MYSQL_FILES) MYSQL=1

everything: all mysql fuse

help:
	@ echo "make [ target ]"
	@ echo "    where <target> is one of:"
	@ echo
	@ echo "    all   -- $(BFW) $(TOOLS)"
	@ echo "    mysql -- bfmi, requires you to have installed sqlite3, *and* mysql"
	@ echo "    fuse  -- bffuse/bfresultfuse, needs osxfuse (on Mac), or libfuse (on Linux)"
	@ echo "    everything -- all of the above"
	@ echo
	@ echo "    with no target, we will build 'all'"

# putils.c was assimilated into utils.c
LIBFILES = bf structq dbutils utils

INCS :=
LIBS := -lgufi -pthread


# CFLAGS += -std=c11 -D_POSIX_C_SOURCE=2
CFLAGS += -std=gnu11
ifneq ($(DEBUG),)
	CFLAGS += -g -O0 -DDEBUG
	CXXFLAGS += -g -O0 -DDEBUG
else
	CFLAGS += -O3
	CXXFLAGS += -O3
endif


# this is invoked in a recursive build, for bfmi.mysql
# (see target "%.mysql")
ifeq ($(MYSQL),)
	LIBS += -lsqlite3
	LIBFILES += dbutils
else
	INCS += $(shell mysql_config --include)
	# bfmi currently uses *both* sqlite3 and mysql!
	LIBS += $(shell mysql_config --libs_r)
	LIBS += -lsqlite3
endif

# different fuse libs for OSX/Linux
UNAME_S = $(shell uname -s)
ifeq ($(UNAME_S), Darwin)
	CPPFLAGS += -D_DARWIN_C_SOURCE
	LIBFUSE   = -losxfuse
endif
ifeq ($(UNAME_S), Linux)
	LIBFUSE   = -lfuse
endif


ifneq ($(FUSE),)
	CPPFLAGS += -D_FILE_OFFSET_BITS=64
	LIBS     += $(LIBFUSE)
endif


LIB_C = $(addsuffix .c,$(LIBFILES))
LIB_O = $(addsuffix .o,$(LIBFILES))
LIB_H = $(addsuffix .h,$(LIBFILES))


# --- library

libgufi.a: $(LIB_O) thpool.o
	ar -rs $@ $^

# NOTE
#
# We're now using the C-Thread-Pool library
# (https://github.com/Pithikos/C-Thread-Pool). thpool.h from that "library"
# provides an incomplete definition of struct thpool_.  The developers
# suggest compiling like so:
#
#    gcc example.c thpool.c -D THPOOL_DEBUG -pthread -o example
#
# That's fine if you are compiling an aplication in one shot, but it fails
# for the case of building a .o that is part of a library.  Thus, we have a
# git submodule from a forked version of the repo, which we have modified
# to allow this.

thpool.o: C-Thread-Pool/thpool.c C-Thread-Pool/thpool.h
	$(CC) $(CFLAGS) $(CPPFLAGS) -c -o $@ $< -pthread

%.o: %.c $(LIB_H)
	$(CC) $(CFLAGS) $(CPPFLAGS) -c -o $@ $< -pthread



# --- apps

%: %.c libgufi.a
	$(CC) $(CFLAGS) $(INCS) $(CPPFLAGS) $(LDFLAGS) -o $@ -L. $< $(LIBS)

%: %.cpp libgufi.a
	$(CXX) $(CXXFLAGS) -std=c++11 $(INCS) $(CPPFLAGS) $(LDFLAGS) -o $@ -L. $< $(LIBS)

# --- clean-up

# trashable files and dirs produced by the test/run* tests, and example_run
TEST_PRODUCTS = \
	test/testout.* \
	test/testdirdup \
	test/outdb* \
	test/outq.* \
	test/core.* \
	test/in \
	test/out

$(GTEST_INSTALL_DIR):
	mkdir -p $(GTEST_BUILD_DIR) && cd $(GTEST_BUILD_DIR) && cmake -DCMAKE_INSTALL_PREFIX=$(GTEST_INSTALL_DIR) .. && $(MAKE) -j install

test: $(GTEST_INSTALL_DIR)
	$(MAKE) -C test

clean_test:
	rm -rf $(TEST_PRODUCTS)
	$(MAKE) -C test clean

clean:
	rm -f libgufi.a
	rm -f *.o
	rm -f *~
	rm -rf *.dSYM
	rm -rf core.*
	@ # for F in `ls *.c | sed -e 's/\.c$//'`; do [ -f $F ] && rm $F; done
	@ # final "echo" is so sub-shell will return success
	@ (for F in `ls *.c* | sed -e 's/\.c.*$$//'`; do [ -f $$F ] && (echo rm $$F; rm $$F); done; echo done > /dev/null)
	rm -f $(SQLITE3_PCRE)/pcre.so
	rm -rf $(GTEST_BUILD_DIR) $(GTEST_INSTALL_DIR)
