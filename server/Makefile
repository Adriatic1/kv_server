COMPILER = g++
CFLAGS = -g
MODE = release

app: /opt/seastar/build/$(MODE)/libseastar.a app.o db.o store_cache.o
	$(COMPILER) app.o db.o store_cache.o $(shell pkg-config --libs --cflags --static /opt/seastar/build/$(MODE)/seastar.pc) $(CFLAGS) -o app

app.o: app.cc
	$(COMPILER) app.cc $(shell pkg-config --libs --cflags --static /opt/seastar/build/$(MODE)/seastar.pc) $(CFLAGS) -c app.o

db.o: db.cc db.hh
	$(COMPILER) db.cc $(shell pkg-config --libs --cflags --static /opt/seastar/build/$(MODE)/seastar.pc) $(CFLAGS) -c db.o

store_cache.o: store_cache.cc store_cache.hh
	$(COMPILER) store_cache.cc $(shell pkg-config --libs --cflags --static /opt/seastar/build/$(MODE)/seastar.pc) $(CFLAGS) -c store_cache.o

/opt/seastar/build/$(MODE)/libseastar.a:
	cd /opt/seastar && ./configure.py --mode="$(MODE)" --disable-dpdk --disable-hwloc --cflags="$(CFLAGS)" --compiler="$(COMPILER)"
	ninja -C /opt/seastar/build/$(MODE) libseastar.a

clean:
	rm -f ./app ./*.o
