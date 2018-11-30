DFW       = dfw
BFW       = bfwi bfti bfq bfwreaddirplus2db
BFW_MYSQL = bfmi.mysql

# TOOLS = querydb querydbn make_testdirs dbdump 
TOOLS = querydb querydbn make_testdirs

# # TBD ...
# cc bffuse.c -I /usr/local/include/osxfuse -D_FILE_OFFSET_BITS=64 -I.. -L../.libs -l sqlite3 -L /usr/local/lib -l osxfuse -o bffuse 

all: bfw tools

bfw:   $(BFW)
mysql: $(BFW_MYSQL)
dfw:   $(DFW)
tools: $(TOOLS)



# putils.c was assimilated into utils.c
LIBFILES = bf structq dbutils utils

# CFLAGS += -std=c11 -D_POSIX_C_SOURCE=2
CFLAGS += -std=gnu11
ifneq ($(DEBUG),)
	CFLAGS += -g -O0
else
	CFLAGS += -g -O3
endif


ifeq ($(shell uname -s), Darwin)
	CPPFLAGS +=  -D _DARWIN_C_SOURCE
endif


INCS :=
LIBS := -lgufi -pthread

# this is invoked in a recursive build, for bfmi.mysql
# (see target "%.mysql")
ifeq ($(MYSQL),)
	INCS +=
	LIBS += -lsqlite3
	LIBFILES += dbutils
else
	INCS += $(shell mysql_config --include)
	# bfmi currently uses *both* sqlite3 and mysql!
	LIBS += $(shell mysql_config --libs_r)
	LIBS += -lsqlite3
endif


LIB_C = $(addsuffix .c,$(LIBFILES))
LIB_O = $(addsuffix .o,$(LIBFILES))
LIB_H = $(addsuffix .h,$(LIBFILES))


# --- library

libgufi.a: $(LIB_O) $(LIB_H) thpool.o
	ar -r $@ $(LIB_O) thpool.o
	ranlib $@

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

%.o: %.c
	$(CC) $(CFLAGS) $(CPPFLAGS) -c -o $@ $< -pthread



# --- apps

%: %.c libgufi.a
	$(CC) $(CFLAGS) $(INCS) $(CPPFLAGS) $(LDFLAGS) -o $@ -L. $< $(LIBS)

# recursive make of the '%' part
# recursive make will catch the ifneq ($(MYSQL),) ... above 
%.mysql:
	$(MAKE) -C . $* MYSQL=1

# these are trashable files and dirs produced by the test/run* tests
TEST_PRODUCTS = test/testout.* test/testdirdup test/outdb* test/outq.* test/core.*
clean_test:
	rm -rf $(TEST_PRODUCTS)

clean:
	rm -f libgufi.a
	rm -f *.o
	rm -f *~
	rm -rf *.dSYM
	rm -rf core.*
	@ # for F in `ls *.c | sed -e 's/\.c$//'`; do [ -f $F ] && rm $F; done
	@ # final "echo" is so sub-shell will return success
	@ (for F in `ls *.c | sed -e 's/\.c$$//'`; do [ -f $$F ] && (echo rm $$F; rm $$F); done; echo done > /dev/null)
