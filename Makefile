
.PHONY: all
all:
	@cd server; $(MAKE)
	@cd test; $(MAKE)
	@cd perf; $(MAKE)

.PHONY: test
test:
	kill -9 $(pidof app) || true
	./server/app & sleep 2 && ./test/test

.PHONY: perf
perf:
	kill -9 $(pidof app) || true
	./server/app & sleep 2 && ./perf/client --conn 24 --reqs 100

.PHONY: clean
clean:
	@cd server; $(MAKE) clean
	@cd test; $(MAKE) clean
	@cd perf; $(MAKE) clean
