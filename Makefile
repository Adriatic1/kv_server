
.PHONY: bin
bin:
	@cd server; $(MAKE)
	@cd test; $(MAKE)
	@cd perf; $(MAKE)

.PHONY: test
test: bin
	pkill -9 app || true
	./server/app & sleep 2 && ./test/test && pkill app

.PHONY: perf
perf: bin
	pkill -9 app || true
	./server/app & sleep 2 && ./perf/client --conn 24 --reqs 100

.PHONY: clean
clean:
	@cd server; $(MAKE) clean
	@cd test; $(MAKE) clean
	@cd perf; $(MAKE) clean

cleandb:
	rm -f ./kvdb_data.*.bin
