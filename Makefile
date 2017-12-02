
# LIBFILES = bf structq utils dbutils putils
LIBFILES = bf structq dbutils utils
LIB_C = $(addsuffix .c,$(LIBFILES))
LIB_O = $(addsuffix .o,$(LIBFILES))
LIB_H = bf.h

# cc bffuse.c -I /usr/local/include/osxfuse -D_FILE_OFFSET_BITS=64 -I.. -L../.libs -l sqlite3 -L /usr/local/lib -l osxfuse -o bffuse 

DFW  = dfw dfwrplus dfwrplusdb dfwrplusdbthread dfwrplusdbthreadsort rpluslistdbthreadsort
BFW  = bfwi bfti bfq 
# TOOLS = querydb querydbn make_testdirs dbdump 
TOOLS = querydb querydbn make_testdirs


all: all.bfw all.tools

all.dfw: $(DFW)

all.bfw: $(BFW)

all.tools: $(TOOLS)


# crude, for now -- just comment this in/out
DEBUG := -g -O1


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


# git submodule refers to a forked version of C-Thread-Pool
thpool.o: C-Thread-Pool/thpool.c C-Thread-Pool/thpool.h
	cc $(DEBUG) -c -o $@ -D _DARWIN_C_SOURCE $< -pthread

%.o: %.c
	cc $(DEBUG) -c -o $@ -D _DARWIN_C_SOURCE $< -pthread


%: %.c libgufi.a
	cc $(DEBUG) -o $@ -L. $<  -lgufi -pthread -lsqlite3

# echo "make all"
# # cc bfq.c -I.. -L../.libs -l sqlite3 -DBSDXATTRS -o bfq
# cc bfq.c -pthread -l sqlite3 -o bfq
# cc bfti.c -pthread -l sqlite3 -o bfti
# # cc bfi.c -I.. -L../.libs -l sqlite3 -DBSDXATTRS -o bfi
# cc dbdump.c -pthread -l sqlite3 -o dbdump 
# # cc dbdump.c -I.. -L../.libs -l sqlite3 -DBSDXATTRS -o dbdump
# cc bfi.c -pthread -l sqlite3 -o bfi
# # cc bfi.c -I.. -L../.libs -l sqlite3 -DBSDXATTRS -o bfi
# # cc dfw.c -DBSDXATTRS -o dfw
# cc dfw.c -o dfw
# # cc bfw.c -DBSDXATTRS -o bfw
# cc dfw.c -o dfw
# # cc bffuse.c -I /usr/local/include/osxfuse -D_FILE_OFFSET_BITS=64 -I.. -L../.libs -l sqlite3 -L /usr/local/lib -l osxfuse -o bffuse 
# cc dfwrplusdb.c -I.. -L../.libs -l sqlite3 -DBSDXATTRS -o dfwrplusdb
# cc dfwrplusdbthread.c -I.. -L../.libs -l sqlite3 -DBSDXATTRS -o dfwrplusdbthread
# cc dfwrplusdbthreadsort.c -I.. -L../.libs -l sqlite3 -DBSDXATTRS -o dfwrplusdbthreadsort
# cc rpluslistdbthreadsort.c -I.. -L../.libs -l sqlite3 -DBSDXATTRS -o rpluslistdbthreadsort
# cc dfwrplus.c -DBSDXATTRS -o dfwrplus
# cc bfhi.c -I.. -L../.libs -l sqlite3 -DBSDXATTRS -o bfhi
# cc bfri.c -I.. -L../.libs -l sqlite3 -DBSDXATTRS -o bfri

clean:
	rm -f libgufi.a
	rm -f *.o
	rm -f *~
	rm -rf *.dSYM
	@ # for F in `ls *.c | sed -e 's/\.c$//'`; do [ -f $F ] && rm $F; done
	@ # final "echo" is so sub-shell will return success
	@ (for F in `ls *.c | sed -e 's/\.c$$//'`; do [ -f $$F ] && (echo rm $$F; rm $$F); done; echo done > /dev/null)
