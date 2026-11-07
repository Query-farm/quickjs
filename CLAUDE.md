# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

DuckDB QuickJS Extension - embeds the QuickJS-NG JavaScript engine into DuckDB, allowing JavaScript execution within SQL queries.

## Build Commands

```sh
# Build release (default)
GEN=ninja make

# Build debug
GEN=ninja make debug

# Run tests
make test

# Format code
make format

# Run clang-tidy
make tidy-check
```

It is best to test with a debug build since it tests more assertions.

All extension functions should be documented inside of DuckDB with CreateScalarFunctionInfo or CreateAggregateFunctionInfo or the appropriate type for the function.  This documentation of the function should include examples, parameter types and parameter names.  The function should be categorized.

When making changes the version should always be updated to the current date plus an ordinal counter in the form of YYYYMMDDCC.


## Build Outputs

- `./build/release/duckdb` - DuckDB shell with extension auto-loaded
- `./build/release/test/unittest` - Test runner
- `./build/release/extension/quickjs/quickjs.duckdb_extension` - Loadable extension binary

## Testing

Tests are in `test/sql/*.test` using DuckDB's SQL test format. Run a specific test pattern:
```sh
./build/release/test/unittest "test/sql/quickjs.test"
```

## Architecture

This is a DuckDB extension built using the extension template pattern:

- **Main source**: `src/quickjs_extension.cpp` - Contains all extension logic
- **Header**: `src/include/quickjs_extension.hpp` - Extension class declaration
- **QuickJS submodule**: `quickjs/` - The QuickJS-NG engine
- **DuckDB submodule**: `duckdb/` - DuckDB source for building

### Extension Functions

Three SQL functions are registered:

1. **`quickjs(code)` (scalar)** - Executes JS, returns result as VARCHAR
2. **`quickjs_eval(function, ...args)` (scalar)** - Executes JS function with args, returns JSON
3. **`quickjs(code, ...args)` (table)** - Executes JS returning array, each element becomes a row

### Key Implementation Details

- `QuickJSEngine` wraps one `JSRuntime` with the limits: `quickjs_memory_limit` (JS_SetMemoryLimit, default 256MB),
  `quickjs_timeout_ms` (interrupt handler deadline, default 0 = off), query interruption / `max_execution_time`
  (the interrupt handler calls `ClientContext::BlockingInterruptCheck()` and never lets a C++ exception cross QuickJS
  C frames), and a native stack budget measured from the real thread stack (`ComputeStackBudget()`), re-armed by
  `BeginEvaluation()` before every `JS_Eval`/`JS_Call`. Always call `BeginEvaluation()` before running JS.
- `quickjs(code)`: runtime cached per thread (FunctionLocalState), fresh `JSContext` per row so globals never leak.
- `quickjs_eval(fn, ...)`: runtime + context cached per thread, the function is compiled once and recompiled only
  when the script text changes. Arguments are converted by `VectorRowToJS()`/`ValueToJS()` (numbers, BigInt beyond
  2^53, Date, Uint8Array, arrays, objects). `undefined` results are SQL NULL. NULL args reach JS as `null`.
- Promises returned by any function are settled by running the job queue (`SettlePromise()`).
- Never pass a possibly-NULL `JS_ToCString` result to std::string; always use `JSValueToString()` / `JSStringToUTF8()`
  (also replaces lone surrogates, which QuickJS emits as invalid UTF-8).
- Table function: arguments become globals `arg0..argN` and `parsed_arg0`; the code runs as a global script and its
  completion value must be an array.
- The version string lives in `QUICKJS_EXTENSION_VERSION` (keep `EXTENSION_VERSION` in extension_config.cmake equal).

### Table Function Parameter Convention

- `arg0`, `arg1`, etc. - Raw parameters (converted like quickjs_eval arguments)
- `parsed_arg0` - First parameter parsed as JSON when it is a string holding valid JSON, otherwise arg0 itself

## Submodule Management

```sh
# Initialize submodules
git submodule update --init --recursive

# Update submodules to latest
make update
```

## Requirements

- DuckDB 2.0 (this branch)
- C++17 compatible compiler
- CMake 3.16+
