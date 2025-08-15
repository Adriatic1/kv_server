COMPILER = g++
CFLAGS = -g
MODE = release

client: /opt/seastar/build/$(MODE)/libseastar.a seawreck.cc
	$(COMPILER) seawreck.cc $(shell pkg-config --libs --cflags --static /opt/seastar/build/$(MODE)/seastar.pc) $(CFLAGS) -o client

/opt/seastar/build/$(MODE)/libseastar.a:
	cd /opt/seastar && ./configure.py --mode="$(MODE)" --disable-dpdk --disable-hwloc --cflags="$(CFLAGS)" --compiler="$(COMPILER)"
	ninja -C /opt/seastar/build/$(MODE) libseastar.a

clean:
	rm -f ./client
