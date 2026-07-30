LOCAL_BUILD_DIR ?= build-local
RDMA_BUILD_DIR ?= build-rdma
BUILD_TYPE ?= Debug

.PHONY: all local-build local-test sanitize rdma-build test-2 test-4 clean

all: local-test

local-build:
	cmake -S . -B $(LOCAL_BUILD_DIR) -DCMAKE_BUILD_TYPE=$(BUILD_TYPE) -DBUILD_RDMA=OFF
	cmake --build $(LOCAL_BUILD_DIR) -j

local-test: local-build
	ctest --test-dir $(LOCAL_BUILD_DIR) --output-on-failure

sanitize:
	cmake -S . -B $(LOCAL_BUILD_DIR)-san -DCMAKE_BUILD_TYPE=Debug -DBUILD_RDMA=OFF -DENABLE_SANITIZERS=ON
	cmake --build $(LOCAL_BUILD_DIR)-san -j
	ctest --test-dir $(LOCAL_BUILD_DIR)-san --output-on-failure

rdma-build:
	cmake -S . -B $(RDMA_BUILD_DIR) -DCMAKE_BUILD_TYPE=Release -DBUILD_RDMA=ON
	cmake --build $(RDMA_BUILD_DIR) -j

test-2: rdma-build
	./scripts/run_2_processes.sh "$$(pwd)"

test-4: rdma-build
	./scripts/run_4_processes.sh "$$(pwd)"

clean:
	rm -rf $(LOCAL_BUILD_DIR) $(LOCAL_BUILD_DIR)-san $(RDMA_BUILD_DIR) logs/*.log

.PHONY: report
report:
	cd report && pdflatex -interaction=nonstopmode -halt-on-error report.tex
	cd report && pdflatex -interaction=nonstopmode -halt-on-error report.tex
