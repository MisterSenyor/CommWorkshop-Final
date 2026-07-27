BUILD_DIR ?= build
BUILD_TYPE ?= Debug

.PHONY: all configure build test demo clean

all: build

configure:
	cmake -S . -B $(BUILD_DIR) -DCMAKE_BUILD_TYPE=$(BUILD_TYPE)

build: configure
	cmake --build $(BUILD_DIR) -j

test: build
	ctest --test-dir $(BUILD_DIR) --output-on-failure

demo: build
	./$(BUILD_DIR)/ring_demo

clean:
	rm -rf $(BUILD_DIR)