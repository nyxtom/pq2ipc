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

# Regenerates the exact benchmark inputs (~8.4 GB); `make check-data` verifies them.
data: gen
	mkdir -p data/dir
	./gen --out=data/snappy.parquet --codec=snappy
	./gen --out=data/zstd.parquet --codec=zstd
	./gen --out=data/none.parquet --codec=none
	for i in $$(seq 1 16); do \
	  ./gen --out=data/dir/part-$$(printf %02d $$i).parquet --rows=1000 --rg-rows=400 --seed=$$i --id-base=$$((i*1000000)); \
	done
check-data:
	cd data && shasum -a 256 -c ../SHA256SUMS

.PHONY: all clean data check-data
