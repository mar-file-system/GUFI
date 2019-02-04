SUBDIRECTORIES=googletest

.PHONY: $(SUBDIRECTORIES)

all: $(SUBDIRECTORIES)

export

$(SUBDIRECTORIES):
	$(MAKE) -C $@

clean-%:
	$(MAKE) -C $* clean

clean: $(addprefix clean-, $(SUBDIRECTORIES))
