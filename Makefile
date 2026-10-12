CXX ?= clang++
FLAGS := -O2 -std=c++20 $(shell pkg-config --cflags arrow parquet arrow-compute)
LIBS := $(shell pkg-config --libs arrow parquet arrow-compute)

all: gen bench
gen: gen.cc
	$(CXX) $(FLAGS) $< -o $@ $(LIBS)
bench: bench.cc transcode.h sim.h
	$(CXX) $(FLAGS) $< -o $@ $(LIBS)
clean:
	rm -f gen bench
