COMPILER = g++
CFLAGS = -g
MODE = release

test: /opt/seastar/build/$(MODE)/libseastar.a test.cc
	$(COMPILER) test.cc $(shell pkg-config --libs --cflags --static /opt/seastar/build/$(MODE)/seastar.pc) $(CFLAGS) -o test

/opt/seastar/build/$(MODE)/libseastar.a:
	cd /opt/seastar && ./configure.py --mode="$(MODE)" --disable-dpdk --disable-hwloc --cflags="$(CFLAGS)" --compiler="$(COMPILER)"
	ninja -C /opt/seastar/build/$(MODE) libseastar.a

clean:
	rm -f ./test
