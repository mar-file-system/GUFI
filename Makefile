DFW       = dfw
BFW       = bfwi bfti bfq bfwreaddirplus2db
BFW_MYSQL = bfmi.mysql

# TOOLS = querydb querydbn make_testdirs dbdump
TOOLS = querydb querydbn make_testdirs make_testtree sqlite3-pcre/pcre.so


.PHONY: sqlite3-pcre googletest test

SQLITE3_PCRE      ?= sqlite3-pcre
GTEST_SRC         ?= $(realpath googletest)
GTEST_BUILD_DIR   ?= $(GTEST_SRC)/build
GTEST_INSTALL_DIR ?= $(GTEST_SRC)/install

all:   bfw tools

bfw:   $(BFW)
mysql: $(BFW_MYSQL)
dfw:   $(DFW)
tools: $(TOOLS)

$(SQLITE3_PCRE)/pcre.so:
	$(MAKE) -C $(SQLITE3_PCRE)

# putils.c was assimilated into utils.c
LIBFILES = bf structq dbutils utils



# CFLAGS += -std=c11 -D_POSIX_C_SOURCE=2
CFLAGS += -std=gnu11
ifneq ($(DEBUG),)
   CFLAGS   += -g -O0 -DDEBUG
   CXXFLAGS += -g -O0 -DDEBUG
else
   CFLAGS   += -O3
   CXXFLAGS += -O3
endif




# --- DARWIN
UNAME_S = $(shell uname -s)
ifeq ($(UNAME_S), Darwin)
	CFLAGS += -D_DARWIN_C_SOURCE
endif



# --- SQL
# this is invoked in a recursive build, for bfmi.mysql
# (see target "%.mysql")
# bfmi currently uses *both* sqlite3 and mysql!
ifeq ($(MYSQL),)
	LIBFILES += dbutils
else
	INCS += $(shell mysql_config --include)
	LIBS += $(shell mysql_config --libs_r)
endif

# We always need sqlite
# Set env-var PKG_CONFIG_PATH to your install
CFLAGS   += $(shell pkg-config --cflags      sqlite3)
CXXFLAGS += $(shell pkg-config --cflags      sqlite3)
LDFLAGS  += $(shell pkg-config --libs-only-L sqlite3)
LIBS     += $(shell pkg-config --libs-only-l sqlite3)



# --- FUSE
# different fuse libs for OSX/Linux
ifeq ($(UNAME_S), Darwin)
	FUSE_PKG = osxfuse
else
	FUSE_PKG = fuse
endif


ifneq ($(FUSE),)
	CFLAGS   += $(shell pkg-config --cflags      $(FUSE_PKG))
	CXXFLAGS += $(shell pkg-config --cflags      $(FUSE_PKG))
	LDFLAGS  += $(shell pkg-config --libs-only-L $(FUSE_PKG))
	LIBS     += $(shell pkg-config --libs-only-l $(FUSE_PKG))
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
# recursive make of the '%' part
# recursive make will catch the ifneq ($(MYSQL),) ... above
%.mysql:
	$(MAKE) -C . $* MYSQL=1

# these are trashable files and dirs produced by the test/run* tests
TEST_PRODUCTS = test/testout.* test/testdirdup test/outdb* test/outq.* test/core.*

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
