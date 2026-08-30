# Building the extension

To build the extension, you first need to clone this repository and initialize the necessary git submodules:

```sh
git clone --recursive git@github.com:Query-farm/quickjs.git
cd quickjs
```

If you have already cloned the repository without the `--recursive` flag, you can initialize the submodules with:
```sh
git submodule update --init --recursive
```

Once the submodules are ready, you can build the extension using `make`:

```sh
make
```

The main binaries that will be built are:
```sh
./build/release/duckdb
./build/release/test/unittest
./build/release/extension/quickjs/quickjs.duckdb_extension
```
- `duckdb` is the binary for the DuckDB shell with the extension code automatically loaded.
- `unittest` is the test runner for DuckDB.
- `quickjs.duckdb_extension` is the loadable binary for the extension.

## Requirements

- DuckDB 1.3.1 or later
- C++11 compatible compiler
- CMake 3.16 or later

## Running the tests

To run the SQL tests for the extension, use the following command:

```sh
make test
```
