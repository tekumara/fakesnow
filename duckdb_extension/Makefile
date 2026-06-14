.PHONY: all configure debug release test test_debug clean clean_all

EXTENSION_NAME=fakesnow
TARGET_DUCKDB_VERSION=v1.2.0
TARGET_DUCKDB_VERSION_MAJOR=1
TARGET_DUCKDB_VERSION_MINOR=2
TARGET_DUCKDB_VERSION_PATCH=0
PLATFORM := $(shell uv run python -c "import duckdb; print(duckdb.connect().execute('PRAGMA platform').fetchone()[0])")
EXTENSION_VERSION := $(shell git rev-parse --short HEAD 2>/dev/null || echo dev)

all: debug

configure:
	@mkdir -p configure
	@printf '%s\n' '$(PLATFORM)' > configure/platform.txt
	@printf '%s\n' '$(EXTENSION_VERSION)' > configure/extension_version.txt

cmake_build/debug/Makefile: CMakeLists.txt src/fakesnow.c
	cmake \
		-DEXTENSION_NAME=$(EXTENSION_NAME) \
		-DTARGET_DUCKDB_VERSION_MAJOR=$(TARGET_DUCKDB_VERSION_MAJOR) \
		-DTARGET_DUCKDB_VERSION_MINOR=$(TARGET_DUCKDB_VERSION_MINOR) \
		-DTARGET_DUCKDB_VERSION_PATCH=$(TARGET_DUCKDB_VERSION_PATCH) \
		-DCMAKE_BUILD_TYPE=Debug \
		-S . \
		-B cmake_build/debug

cmake_build/release/Makefile: CMakeLists.txt src/fakesnow.c
	cmake \
		-DEXTENSION_NAME=$(EXTENSION_NAME) \
		-DTARGET_DUCKDB_VERSION_MAJOR=$(TARGET_DUCKDB_VERSION_MAJOR) \
		-DTARGET_DUCKDB_VERSION_MINOR=$(TARGET_DUCKDB_VERSION_MINOR) \
		-DTARGET_DUCKDB_VERSION_PATCH=$(TARGET_DUCKDB_VERSION_PATCH) \
		-DCMAKE_BUILD_TYPE=Release \
		-S . \
		-B cmake_build/release

debug: configure cmake_build/debug/Makefile
	cmake --build cmake_build/debug --config Debug
	mkdir -p build/debug/extension/$(EXTENSION_NAME)
	cp cmake_build/debug/lib$(EXTENSION_NAME).* build/debug/
	uv run python scripts/append_extension_metadata.py \
		-l build/debug/lib$(EXTENSION_NAME).* \
		-o build/debug/$(EXTENSION_NAME).duckdb_extension \
		-n $(EXTENSION_NAME) \
		-dv $(TARGET_DUCKDB_VERSION) \
		-ev $(EXTENSION_VERSION) \
		-p $(PLATFORM)
	cp build/debug/$(EXTENSION_NAME).duckdb_extension build/debug/extension/$(EXTENSION_NAME)/$(EXTENSION_NAME).duckdb_extension

release: configure cmake_build/release/Makefile
	cmake --build cmake_build/release --config Release
	mkdir -p build/release/extension/$(EXTENSION_NAME)
	cp cmake_build/release/lib$(EXTENSION_NAME).* build/release/
	uv run python scripts/append_extension_metadata.py \
		-l build/release/lib$(EXTENSION_NAME).* \
		-o build/release/$(EXTENSION_NAME).duckdb_extension \
		-n $(EXTENSION_NAME) \
		-dv $(TARGET_DUCKDB_VERSION) \
		-ev $(EXTENSION_VERSION) \
		-p $(PLATFORM)
	cp build/release/$(EXTENSION_NAME).duckdb_extension build/release/extension/$(EXTENSION_NAME)/$(EXTENSION_NAME).duckdb_extension

test: test_debug

test_debug: debug
	uv run python -c 'import duckdb; con = duckdb.connect(config={"allow_unsigned_extensions": "true"}); con.execute("LOAD '\''build/debug/fakesnow.duckdb_extension'\''"); assert con.execute("SELECT _fs_sum(x) FROM (SELECT 1 AS x)").fetchone()[0] == 1; assert con.execute("SELECT typeof(_fs_sum(x)) FROM (SELECT 1 AS x)").fetchone()[0] == "BIGINT"; assert con.execute("SELECT _fs_sum(x) FROM (SELECT '\''1'\'' AS x)").fetchone()[0] == 1.0; assert con.execute("SELECT typeof(_fs_sum(x)) FROM (SELECT '\''1'\'' AS x)").fetchone()[0] == "DOUBLE"'

clean:
	rm -rf build cmake_build

clean_all: clean
	rm -rf configure
