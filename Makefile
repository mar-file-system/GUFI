CC?=cc
CXX?=c++
CFLAGS?=
CXXFLAGS?=-std=c++11
LDFLAGS?=

PCRE_CFLAGS     ?= $(shell pkg-config --cflags libpcre)
PCRE_LDFLAGS    ?= $(shell pkg-config --libs   libpcre)

SQLITE3_CFLAGS  ?= $(shell pkg-config --cflags sqlite3)
SQLITE3_LDFLAGS ?= $(shell pkg-config --libs   sqlite3)

# different fuse libs for OSX/Linux
ifeq ($(UNAME_S), Darwin)
	FUSE_PKG = osxfuse
else
	FUSE_PKG = fuse
endif

FUSE_CFLAGS   ?= $(shell pkg-config --cflags $(FUSE_PKG))
FUSE_LDFLAGS  ?= $(shell pkg-config --libs   $(FUSE_PKG))

MYSQL_CFLAGS  ?= $(shell mysql_config --include)
MYSQL_LDFLAGS ?= $(shell mysql_config --libs_r)

MAKEFILE_PATH         = $(shell dirname $(abspath $(lastword $(MAKEFILE_LIST))))

THPOOL_PATH          ?= $(MAKEFILE_PATH)/C-Thread-Pool
THPOOL_CFLAGS        ?= -I.
THPOOL_LDFLAGS       ?= -L$(THPOOL_PATH) -lthpool -pthread
THPOOL_LIB           ?= $(THPOOL_PATH)/libthpool.a

SQLITE3_PCRE_PATH    ?= $(MAKEFILE_PATH)/sqlite3-pcre
SQLITE3_PCRE_CFLAGS  ?= -I$(SQLITE3_PCRE_PATH) $(SQLITE3_CFLAGS) $(PCRE_CFLAGS)
SQLITE3_PCRE_LDFLAGS ?= -L$(SQLITE3_PCRE_PATH) -lsqlite3-pcre $(SQLITE3_LDFLAGS) $(PCRE_LDFLAGS)
SQLITE3_PCRE_LIB     ?= $(SQLITE3_PCRE_PATH)/libsqlite3-pcre.a

GTEST_SRC            ?= $(realpath googletest)
GTEST_BUILD_DIR      ?= $(GTEST_SRC)/build
GTEST_INSTALL_DIR    ?= $(GTEST_SRC)/install

DFW       = dfw
BFW       = bfwi bfti bfq bfwreaddirplus2db $(DFW)
BFW_MYSQL = bfmi.mysql


# TOOLS = querydb querydbn make_testdirs dbdump
TOOLS = querydb querydbn make_testdirs make_testtree

.PHONY: C-Thread-Pool sqlite3-pcre googletest test

all:   bfw tools

bfw:   $(BFW)
mysql: $(BFW_MYSQL)
# dfw:   $(DFW)
tools: $(TOOLS)

# export all variables into submakes
export

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
# for the case of building a .o that is part of a library.  Thus, we have
# a forked version of the repo, which we have modified to allow this.

$(THPOOL_LIB): $(THPOOL_PATH)/thpool.c $(THPOOL_PATH)/thpool.h
	$(CC) $(CFLAGS) -static -c -o $@ $< -pthread

$(SQLITE3_PCRE_LIB): $(SQLITE3_PCRE_PATH)/pcre.c $(SQLITE3_PCRE_PATH)/pcre.h
	$(MAKE) -C sqlite3-pcre libsqlite3-pcre.a

CFLAGS  += $(THPOOL_CFLAGS)  $(SQLITE3_PCRE_CFLAGS)
LDFLAGS += $(THPOOL_LDFLAGS) $(SQLITE3_PCRE_LDFLAGS)

# putils.c was assimilated into utils.c
LIBFILES = bf structq dbutils utils

# CFLAGS += -std=c11 -D_POSIX_C_SOURCE=2
ifneq ($(DEBUG),)
   CFLAGS += -g -O0 -DDEBUG
else
   CFLAGS += -O3
endif

# --- DARWIN
UNAME_S = $(shell uname -s)
ifeq ($(UNAME_S), Darwin)
	CFLAGS += -D_DARWIN_C_SOURCE
endif

# this is invoked in a recursive build, for bfmi.mysql
# (see target "%.mysql")
# bfmi currently uses *both* sqlite3 and mysql!
ifeq ($(MYSQL),)
	LIBFILES += dbutils
else
	CFLAGS  += $(MYSQL_CFLAGS)
	LDFLAGS += $(MYSQL_LDFLAGS)
endif

ifneq ($(FUSE),)
	CFLAGS  += $(FUSE_CFLAGS)
	LDFLAGS += $(FUSE_LDFLAGS)
endif

LIB_C = $(addsuffix .c,$(LIBFILES))
LIB_O = $(addsuffix .o,$(LIBFILES))
LIB_H = $(addsuffix .h,$(LIBFILES))

%.o: %.c $(LIB_H) $(SQLITE3_PCRE_LIB)
	$(CC) $(CFLAGS) -c -o $@ $< -pthread

# --- library

libgufi.a: $(LIB_O)
	ar -rs $@ $^

# --- apps

%: %.c libgufi.a $(THPOOL_LIB)
	$(CC) $(CFLAGS) -o $@ -L. $< -lgufi $(LDFLAGS)

%: %.cpp libgufi.a $(SQLITE3_PCRE_LIB) $(THPOOL_LIB)
	$(CXX) $(CFLAGS) $(CXXFLAGS) -o $@ -L. $< -lgufi $(LDFLAGS)

# recursive make of the '%' part
# recursive make will catch the ifneq ($(MYSQL),) ... above
%.mysql:
	$(MAKE) -C . $* MYSQL=1

# these are trashable files and dirs produced by the test/run* tests
TEST_PRODUCTS = test/testout.* test/testdirdup test/outdb* test/outq.* test/core.*

$(GTEST_INSTALL_DIR):
	mkdir -p $(GTEST_BUILD_DIR) && cd $(GTEST_BUILD_DIR) && cmake -DCMAKE_INSTALL_PREFIX=$(GTEST_INSTALL_DIR) .. && $(MAKE) -j install

test_prereqs: all $(GTEST_INSTALL_DIR)
	$(MAKE) -C test

test: test_prereqs
	test/runtests test/testdir.tar test/testdir test/testdir.gufi
	# cd test && ./gufitest.py all
	test/verifyknowntree
	test/googletest/test

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
	rm -rf $(GTEST_BUILD_DIR) $(GTEST_INSTALL_DIR)
	rm -f $(THPOOL_LIB)
	if [[ -d $(SQLITE3_PCRE_PATH) ]]; then $(MAKE) -C $(SQLITE3_PCRE_PATH) clean; fi
