# libGUFI files
LIBFILES = bf structq dbutils utils
LIB_C = $(addsuffix .c,$(LIBFILES))
LIB_O = $(addsuffix .o,$(LIBFILES))
LIB_H = $(addsuffix .h,$(LIBFILES))
LIB_A = ../libgufi.a

# executables
DFW       = dfw
BFW       = bfwi bfti bfq bfwreaddirplus2db
BFW_MYSQL = bfmi.mysql
TOOLS     = querydb querydbn make_testdirs make_testtree
TARGETS   = $(LIB_A) $(addprefix ../,$(DFW) $(BFW) $(TOOLS))

all: $(TARGETS)

# utils.c.in requires a pattern to be replaced
# and is included by dfw.c, so the file needs to exist
utils.c: utils.c.in
	sed -e 's/@XATTR_H@/attr\/xattr.h/g' $< > $@

%.o: %.c $(LIB_H)
	$(CC) $(CFLAGS) -c -o $@ $< -pthread

# --- library

$(LIB_A): $(LIB_O)
	ar -rs $@ $^

# --- apps

../%: %.c $(LIB_A) $(SQLITE3_PCRE_LIB) $(THPOOL_LIB)
	$(CC) $(CFLAGS) -o $@ -L.. $< -lgufi $(LDFLAGS)

../%: %.cpp $(LIB_A) $(SQLITE3_PCRE_LIB) $(THPOOL_LIB)
	$(CXX) $(CFLAGS) $(CXXFLAGS) -o $@ -L.. $< -lgufi $(LDFLAGS)

# recursive make of the '%' part
# recursive make will catch the ifneq ($(MYSQL),) ... above
%.mysql:
	$(MAKE) -C . $* MYSQL=1

clean:
	rm -f $(LIB_O) $(LIB_A) $(TARGETS)
	rm -f *~
	rm -rf *.dSYM
	rm -rf core.*
	@ # for F in `ls *.c | sed -e 's/\.c$//'`; do [ -f $F ] && rm $F; done
	@ # final "echo" is so sub-shell will return success
	@ (for F in `ls *.c* | sed -e 's/\.c.*$$//'`; do [ -f $$F ] && (echo rm $$F; rm $$F); done; echo done > /dev/null)
	if [[ -d $(THPOOL_PATH) ]]; then rm -f ${THPOOL_LIB}; fi
	if [[ -d $(SQLITE3_PCRE_PATH) ]]; then $(MAKE) -C $(SQLITE3_PCRE_PATH) clean; fi
