SUBDIRECTORIES=googletest

.PHONY: $(SUBDIRECTORIES)

all: $(SUBDIRECTORIES)

$(SUBDIRECTORIES):
	$(MAKE) -C $@

clean-%:
	$(MAKE) -C $* clean

clean: $(addprefix clean-, $(SUBDIRECTORIES))
