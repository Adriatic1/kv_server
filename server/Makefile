COMPILER = g++
CFLAGS = -g
MODE = release
LIBFLAGS = $(shell pkg-config --libs --cflags --static /opt/seastar/build/$(MODE)/seastar.pc)

app: /opt/seastar/build/$(MODE)/libseastar.a app.o db.o store_cache.o store_disk.o
	$(COMPILER) app.o db.o store_cache.o store_disk.o $(LIBFLAGS) $(CFLAGS) -o app

app.o: app.cc
	$(COMPILER) app.cc $(LIBFLAGS) $(CFLAGS) -c app.o

db.o: db.cc db.hh
	$(COMPILER) db.cc $(LIBFLAGS) $(CFLAGS) -c db.o

store_cache.o: store_cache.cc store_cache.hh
	$(COMPILER) store_cache.cc $(LIBFLAGS) $(CFLAGS) -c store_cache.o

store_disk.o: store_disk.cc store_disk.hh
	$(COMPILER) store_disk.cc $(LIBFLAGS) $(CFLAGS) -c store_disk.o

/opt/seastar/build/$(MODE)/libseastar.a:
	cd /opt/seastar && ./configure.py --mode="$(MODE)" --disable-dpdk --disable-hwloc --cflags="$(CFLAGS)" --compiler="$(COMPILER)"
	ninja -C /opt/seastar/build/$(MODE) libseastar.a

clean:
	rm -f ./app ./*.o

