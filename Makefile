

LIBFILES = dbutils structq utils
LIB_C = $(addsuffix .c,$(LIBFILES))
LIB_O = $(addsuffix .o,$(LIBFILES))
LIB_H = bf.h

libgufi.a: $(LIB_O) $(LIB_H)
	ar -r $@ $(LIB_O)

%.o: %.c
	cc  -DBSDXATTRS -o $@ $< -pthread -l sqlite3

%: %.c
	cc  -DBSDXATTRS -o $@ $< -pthread -l sqlite3

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
