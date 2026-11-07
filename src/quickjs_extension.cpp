#define DUCKDB_EXTENSION_MAIN

#include "quickjs_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/string_vector.hpp"
#include "duckdb/common/vector/unified_vector_format.hpp"
#include "duckdb/execution/expression_executor_state.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "query_farm_telemetry.hpp"
#include "utf8proc_wrapper.hpp"

#include "quickjs.h"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>

#if defined(_WIN32)
#include "duckdb/common/windows.hpp"
#elif defined(__APPLE__) || defined(__linux__)
#include <pthread.h>
#endif

//! The extension version, reported both by Version() and by telemetry
#define QUICKJS_EXTENSION_VERSION "2026110701"

namespace duckdb {

//! A positional-only parameter with a real name (bare types would be reported as col0, col1, ...)
static FunctionParameter Positional(const char *name, LogicalType type) {
	return FunctionParameter(name, std::move(type), {}, FunctionParameterKind::POSITIONAL_ONLY);
}

//===--------------------------------------------------------------------===//
// Settings
//===--------------------------------------------------------------------===//
static constexpr const char *MEMORY_LIMIT_SETTING = "quickjs_memory_limit";
static constexpr const char *TIMEOUT_SETTING = "quickjs_timeout_ms";
static constexpr const char *DEFAULT_MEMORY_LIMIT = "256MB";

struct QuickJSLimits {
	//! Maximum heap size of one JavaScript runtime in bytes, 0 = unlimited
	idx_t memory_limit = 0;
	//! The memory limit as the user wrote it, for error messages
	string memory_limit_text;
	//! Maximum wall-clock time of one evaluation in milliseconds, 0 = unlimited
	int64_t timeout_ms = 0;
};

static idx_t ParseQuickJSMemoryLimit(const string &text) {
	auto limit = DBConfig::ParseMemoryLimit(text);
	return limit.IsValid() ? limit.GetIndex() : 0;
}

static void ValidateMemoryLimit(ClientContext &, SetScope, Value &parameter) {
	if (!parameter.IsNull()) {
		ParseQuickJSMemoryLimit(parameter.ToString());
	}
}

static void ValidateTimeout(ClientContext &, SetScope, Value &parameter) {
	if (!parameter.IsNull() && parameter.GetValue<int64_t>() < 0) {
		throw InvalidInputException("%s must be >= 0 (0 disables the timeout)", TIMEOUT_SETTING);
	}
}

static QuickJSLimits ReadLimits(ClientContext &context) {
	QuickJSLimits limits;
	Value value;
	limits.memory_limit_text = DEFAULT_MEMORY_LIMIT;
	if (context.TryGetCurrentSetting(MEMORY_LIMIT_SETTING, value) && !value.IsNull()) {
		limits.memory_limit_text = value.ToString();
	}
	limits.memory_limit = ParseQuickJSMemoryLimit(limits.memory_limit_text);
	if (context.TryGetCurrentSetting(TIMEOUT_SETTING, value) && !value.IsNull()) {
		limits.timeout_ms = value.GetValue<int64_t>();
	}
	return limits;
}

//===--------------------------------------------------------------------===//
// Native stack budget
//===--------------------------------------------------------------------===//
// QuickJS guards recursion by comparing the stack pointer with a limit. Its default (1MB) is larger than many
// thread stacks DuckDB runs on (macOS worker threads: 512KB, musl: 128KB, wasm: 64KB+), so deep recursion would
// overflow the real stack and crash the process. Instead we measure the stack that is actually left on the
// calling thread and give QuickJS most of it, keeping a safety margin for the native frames QuickJS itself
// pushes between checks.

//! Lowest usable address of the current thread's stack, or 0 if unknown
static uintptr_t CurrentThreadStackLow() {
#if defined(_WIN32)
	MEMORY_BASIC_INFORMATION mbi;
	if (VirtualQuery(&mbi, &mbi, sizeof(mbi)) == 0) {
		return 0;
	}
	// The allocation base is the bottom of the reserved stack region; leave room for the guard pages
	return reinterpret_cast<uintptr_t>(mbi.AllocationBase) + 64 * 1024;
#elif defined(__APPLE__)
	pthread_t self = pthread_self();
	auto top = reinterpret_cast<uintptr_t>(pthread_get_stackaddr_np(self));
	auto size = static_cast<uintptr_t>(pthread_get_stacksize_np(self));
	return top > size ? top - size : 0;
#elif defined(__linux__)
	pthread_attr_t attr;
	if (pthread_getattr_np(pthread_self(), &attr) != 0) {
		return 0;
	}
	void *addr = nullptr;
	size_t size = 0;
	auto rc = pthread_attr_getstack(&attr, &addr, &size);
	pthread_attr_destroy(&attr);
	if (rc != 0) {
		return 0;
	}
	return reinterpret_cast<uintptr_t>(addr);
#else
	// Unknown platform (including wasm, where querying the stack would need symbols a side module may not get):
	// ComputeStackBudget() falls back to a conservative fixed budget
	return 0;
#endif
}

static size_t ComputeStackBudget() {
	static constexpr size_t FALLBACK_BUDGET = 256 * 1024;
	static constexpr size_t MIN_BUDGET = 16 * 1024;
	static constexpr size_t MAX_BUDGET = 8 * 1024 * 1024;
	static constexpr size_t MIN_MARGIN = 64 * 1024;

	// Querying the stack bounds can be expensive (glibc reads /proc/self/maps for the main thread), so cache it
	static thread_local uintptr_t stack_low = 0;
	static thread_local bool stack_low_known = false;
	if (!stack_low_known) {
		stack_low = CurrentThreadStackLow();
		stack_low_known = true;
	}
	if (stack_low == 0) {
		return FALLBACK_BUDGET;
	}
	volatile char marker = 0;
	auto sp = reinterpret_cast<uintptr_t>(&marker);
	if (sp <= stack_low) {
		return MIN_BUDGET;
	}
	size_t available = sp - stack_low;
	size_t margin = MaxValue<size_t>(MIN_MARGIN, available / 4);
	if (available <= margin + MIN_BUDGET) {
		return MIN_BUDGET;
	}
	return MinValue<size_t>(available - margin, MAX_BUDGET);
}

//===--------------------------------------------------------------------===//
// RAII Wrappers for QuickJS Resources
//===--------------------------------------------------------------------===//

//! Heap allocator for a QuickJS runtime that enforces quickjs_memory_limit itself (instead of JS_SetMemoryLimit) so
//! that hitting the limit is observable: when QuickJS cannot even allocate the "out of memory" error object it throws
//! null, and how close to the limit that happens depends on the platform's malloc. Every block carries a small header
//! holding its size, so accounting is identical on all platforms.
struct QuickJSAllocator {
	//! Maximum number of bytes, 0 = unlimited
	size_t limit = 0;
	//! Bytes currently allocated (including headers)
	size_t used = 0;
	//! Set when an allocation was refused because of the limit
	bool limit_hit = false;

	//! Extra bytes allowed above the limit while an error is converted to a message
	static constexpr size_t ERROR_HEADROOM = 1024 * 1024;

	static constexpr size_t HEADER =
	    alignof(std::max_align_t) > sizeof(size_t) ? alignof(std::max_align_t) : sizeof(size_t);

	bool Reserve(size_t bytes) {
		if (limit != 0 && (bytes > limit || used > limit - bytes)) {
			limit_hit = true;
			return false;
		}
		used += bytes;
		return true;
	}

	static size_t &BlockSize(void *base) {
		return *static_cast<size_t *>(base);
	}

	static void *Base(const void *ptr) {
		return const_cast<char *>(static_cast<const char *>(ptr)) - HEADER;
	}

	static void *Malloc(void *opaque, size_t size) {
		auto &alloc = *static_cast<QuickJSAllocator *>(opaque);
		if (size > SIZE_MAX - HEADER || !alloc.Reserve(size + HEADER)) {
			return nullptr;
		}
		void *base = std::malloc(size + HEADER);
		if (!base) {
			alloc.used -= size + HEADER;
			return nullptr;
		}
		BlockSize(base) = size;
		return static_cast<char *>(base) + HEADER;
	}

	static void *Calloc(void *opaque, size_t count, size_t size) {
		if (size != 0 && count > SIZE_MAX / size) {
			return nullptr;
		}
		auto ptr = Malloc(opaque, count * size);
		if (ptr) {
			std::memset(ptr, 0, count * size);
		}
		return ptr;
	}

	static void Free(void *opaque, void *ptr) {
		if (!ptr) {
			return;
		}
		auto &alloc = *static_cast<QuickJSAllocator *>(opaque);
		void *base = Base(ptr);
		alloc.used -= BlockSize(base) + HEADER;
		std::free(base);
	}

	static void *Realloc(void *opaque, void *ptr, size_t size) {
		if (!ptr) {
			return Malloc(opaque, size);
		}
		if (size == 0) {
			Free(opaque, ptr);
			return nullptr;
		}
		auto &alloc = *static_cast<QuickJSAllocator *>(opaque);
		if (size > SIZE_MAX - HEADER) {
			return nullptr;
		}
		void *base = Base(ptr);
		size_t old_size = BlockSize(base);
		if (size > old_size && !alloc.Reserve(size - old_size)) {
			return nullptr;
		}
		void *new_base = std::realloc(base, size + HEADER);
		if (!new_base) {
			if (size > old_size) {
				alloc.used -= size - old_size;
			}
			return nullptr;
		}
		if (size < old_size) {
			alloc.used -= old_size - size;
		}
		BlockSize(new_base) = size;
		return static_cast<char *>(new_base) + HEADER;
	}

	static size_t UsableSize(const void *ptr) {
		return ptr ? BlockSize(Base(ptr)) : 0;
	}
};

static const JSMallocFunctions QUICKJS_MALLOC_FUNCTIONS = {QuickJSAllocator::Calloc, QuickJSAllocator::Malloc,
                                                           QuickJSAllocator::Free, QuickJSAllocator::Realloc,
                                                           QuickJSAllocator::UsableSize};

class QuickJSRuntime {
public:
	explicit QuickJSRuntime(QuickJSAllocator &allocator) : rt(JS_NewRuntime2(&QUICKJS_MALLOC_FUNCTIONS, &allocator)) {
		if (!rt) {
			throw IOException("Failed to create QuickJS runtime.");
		}
	}

	~QuickJSRuntime() {
		if (rt) {
			JS_RunGC(rt);
			JS_FreeRuntime(rt);
		}
	}

	QuickJSRuntime(const QuickJSRuntime &) = delete;
	QuickJSRuntime &operator=(const QuickJSRuntime &) = delete;

	JSRuntime *Get() const {
		return rt;
	}

private:
	JSRuntime *rt;
};

class QuickJSContext {
public:
	explicit QuickJSContext(JSRuntime *rt) : ctx(JS_NewContext(rt)) {
		if (!ctx) {
			throw InvalidInputException("Failed to create QuickJS context (is %s too small?)", MEMORY_LIMIT_SETTING);
		}
	}

	~QuickJSContext() {
		if (ctx) {
			JS_FreeContext(ctx);
		}
	}

	QuickJSContext(const QuickJSContext &) = delete;
	QuickJSContext &operator=(const QuickJSContext &) = delete;

	JSContext *Get() const {
		return ctx;
	}

private:
	JSContext *ctx;
};

class QuickJSValue {
public:
	QuickJSValue() : ctx(nullptr), val(JS_UNDEFINED) {
	}
	QuickJSValue(JSContext *ctx, JSValue val) : ctx(ctx), val(val) {
	}

	~QuickJSValue() {
		Reset();
	}

	QuickJSValue(const QuickJSValue &) = delete;
	QuickJSValue &operator=(const QuickJSValue &) = delete;

	QuickJSValue(QuickJSValue &&other) noexcept : ctx(other.ctx), val(other.val) {
		other.ctx = nullptr;
		other.val = JS_UNDEFINED;
	}

	QuickJSValue &operator=(QuickJSValue &&other) noexcept {
		if (this != &other) {
			Reset();
			ctx = other.ctx;
			val = other.val;
			other.ctx = nullptr;
			other.val = JS_UNDEFINED;
		}
		return *this;
	}

	void Reset() {
		if (ctx) {
			JS_FreeValue(ctx, val);
		}
		ctx = nullptr;
		val = JS_UNDEFINED;
	}

	JSValue Get() const {
		return val;
	}

	bool IsException() const {
		return JS_IsException(val);
	}

private:
	JSContext *ctx;
	JSValue val;
};

//! Copy a JS C string into a std::string that is valid UTF-8. QuickJS encodes lone surrogates (e.g. '\uD800') as
//! 3-byte WTF-8 sequences, which DuckDB must never see in a VARCHAR: replace each with U+FFFD.
static string JSStringToUTF8(const char *data, size_t len) {
	if (Utf8Proc::IsValid(data, len)) {
		return string(data, len);
	}
	string result;
	result.reserve(len + 8);
	for (size_t i = 0; i < len; i++) {
		auto c = static_cast<unsigned char>(data[i]);
		if (c == 0xED && i + 2 < len && (static_cast<unsigned char>(data[i + 1]) & 0xE0) == 0xA0) {
			result += "\xEF\xBF\xBD";
			i += 2;
			continue;
		}
		result += static_cast<char>(c);
	}
	if (!Utf8Proc::IsValid(result.c_str(), result.size())) {
		Utf8Proc::MakeValid(&result[0], result.size());
	}
	return result;
}

//===--------------------------------------------------------------------===//
// Engine: a runtime with limits, interrupt handling and error translation
//===--------------------------------------------------------------------===//

class QuickJSEngine {
public:
	QuickJSEngine(ClientContext &client, QuickJSLimits limits_p)
	    : runtime(allocator), client(client), limits(std::move(limits_p)) {
		JS_SetInterruptHandler(runtime.Get(), InterruptHandler, this);
	}

	QuickJSEngine(const QuickJSEngine &) = delete;
	QuickJSEngine &operator=(const QuickJSEngine &) = delete;

	JSRuntime *Runtime() const {
		return runtime.Get();
	}

	//! Create a context and arm the memory limit (applied after the context exists so the limit covers the script)
	unique_ptr<QuickJSContext> NewContext() {
		allocator.limit = 0;
		auto result = make_uniq<QuickJSContext>(runtime.Get());
		allocator.limit = limits.memory_limit;
		return result;
	}

	//! Must be called right before any JS_Eval / JS_Call: re-anchors the stack guard to the current thread and
	//! starts the timeout clock
	void BeginEvaluation() {
		JS_UpdateStackTop(runtime.Get());
		JS_SetMaxStackSize(runtime.Get(), ComputeStackBudget());
		interrupt_reason = InterruptReason::NONE;
		allocator.limit_hit = false;
		if (limits.timeout_ms > 0) {
			deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(limits.timeout_ms);
		}
	}

	//! If value is a Promise (e.g. the result of an async function), run the job queue until it settles and replace
	//! value with the fulfilled value; a rejection is thrown as an error. Runs within the current evaluation's
	//! timeout/interrupt budget.
	void SettlePromise(JSContext *ctx, QuickJSValue &value) {
		if (!JS_IsPromise(value.Get())) {
			return;
		}
		while (JS_PromiseState(ctx, value.Get()) == JS_PROMISE_PENDING) {
			JSContext *job_ctx = nullptr;
			int rc = JS_ExecutePendingJob(runtime.Get(), &job_ctx);
			if (rc < 0) {
				ThrowPendingException(job_ctx ? job_ctx : ctx);
			}
			if (rc == 0) {
				throw InvalidInputException("JavaScript promise never settled (no pending jobs left)");
			}
		}
		auto state = JS_PromiseState(ctx, value.Get());
		QuickJSValue settled(ctx, JS_PromiseResult(ctx, value.Get()));
		if (state == JS_PROMISE_REJECTED) {
			JS_PromiseMarkAsHandled(ctx, value.Get());
			JS_Throw(ctx, JS_DupValue(ctx, settled.Get()));
			ThrowPendingException(ctx, "JavaScript promise rejected");
		}
		value = std::move(settled);
	}

	//! Run any queued promise jobs (so none outlive the context that queued them); errors in them are ignored,
	//! but the timeout and query interruption still apply
	void DrainPendingJobs(JSContext *ctx) {
		while (true) {
			JSContext *job_ctx = nullptr;
			int rc = JS_ExecutePendingJob(runtime.Get(), &job_ctx);
			if (rc == 0) {
				return;
			}
			if (rc < 0) {
				if (interrupt_reason != InterruptReason::NONE) {
					ThrowPendingException(job_ctx ? job_ctx : ctx);
				}
				JS_FreeValue(job_ctx ? job_ctx : ctx, JS_GetException(job_ctx ? job_ctx : ctx));
			}
		}
	}

	//! Translate the pending JavaScript exception (or interrupt) into a DuckDB exception
	[[noreturn]] void ThrowPendingException(JSContext *ctx, const string &prefix = string()) {
		JSValue exception = JS_GetException(ctx);
		// Give the conversion a little headroom above the limit: stringifying the error needs some memory (bounded,
		// since a thrown object's toString() is user code)
		bool out_of_memory = allocator.limit_hit;
		if (allocator.limit != 0) {
			allocator.limit = allocator.limit + MinValue<size_t>(QuickJSAllocator::ERROR_HEADROOM, SIZE_MAX - allocator.limit);
		}
		string message;
		if (out_of_memory && JS_IsNull(exception)) {
			// QuickJS could not allocate the error object at the limit and threw null instead
			message = "InternalError: out of memory";
		} else {
			size_t len = 0;
			const char *c_str = JS_ToCStringLen(ctx, &len, exception);
			if (c_str) {
				message = string(c_str, len);
				JS_FreeCString(ctx, c_str);
			} else {
				// e.g. a thrown Symbol: ToString throws, discard that secondary exception
				JS_FreeValue(ctx, JS_GetException(ctx));
				message = out_of_memory ? "InternalError: out of memory"
				                        : "uncaught JavaScript exception (value cannot be converted to a string)";
			}
		}
		JS_FreeValue(ctx, exception);
		allocator.limit = limits.memory_limit;
		allocator.limit_hit = false;

		switch (interrupt_reason) {
		case InterruptReason::CANCELLED:
			client.BlockingInterruptCheck();
			throw InterruptException();
		case InterruptReason::TIMEOUT:
			throw InvalidInputException("JavaScript execution exceeded %s (%lld ms)", TIMEOUT_SETTING,
			                            static_cast<long long>(limits.timeout_ms));
		default:
			break;
		}

		message = JSStringToUTF8(message.c_str(), message.size());
		if (StringUtil::Contains(message, "out of memory")) {
			message += StringUtil::Format(" (%s is %s)", MEMORY_LIMIT_SETTING, limits.memory_limit_text);
		} else if (StringUtil::Contains(message, "stack overflow") ||
		           StringUtil::Contains(message, "Maximum call stack size exceeded")) {
			message += " (recursion too deep for the native thread stack)";
		}
		if (prefix.empty()) {
			throw InvalidInputException(message);
		}
		throw InvalidInputException("%s: %s", prefix, message);
	}

private:
	enum class InterruptReason { NONE, CANCELLED, TIMEOUT };

	static int InterruptHandler(JSContext *, void *opaque) {
		auto &engine = *static_cast<QuickJSEngine *>(opaque);
		// Called from inside QuickJS (C code): never let a C++ exception escape from here
		try {
			engine.client.BlockingInterruptCheck();
		} catch (...) {
			engine.interrupt_reason = InterruptReason::CANCELLED;
			return 1;
		}
		if (engine.limits.timeout_ms > 0 && std::chrono::steady_clock::now() >= engine.deadline) {
			engine.interrupt_reason = InterruptReason::TIMEOUT;
			return 1;
		}
		return 0;
	}

	//! Declared before runtime: the runtime frees its memory through the allocator on destruction
	QuickJSAllocator allocator;
	QuickJSRuntime runtime;
	ClientContext &client;
	QuickJSLimits limits;
	std::chrono::steady_clock::time_point deadline;
	InterruptReason interrupt_reason = InterruptReason::NONE;
};

//===--------------------------------------------------------------------===//
// JS -> DuckDB helpers
//===--------------------------------------------------------------------===//

//! Convert a value with ToString semantics. Throws on a JS exception (e.g. a Symbol).
static string JSValueToString(QuickJSEngine &engine, JSContext *ctx, JSValue val) {
	size_t len = 0;
	const char *c_str = JS_ToCStringLen(ctx, &len, val);
	if (!c_str) {
		engine.ThrowPendingException(ctx, "Failed to convert result to a string");
	}
	auto result = JSStringToUTF8(c_str, len);
	JS_FreeCString(ctx, c_str);
	return result;
}

//! JSON.stringify a value; returns false when the value has no JSON representation (undefined, functions, symbols)
static bool JSValueToJSON(QuickJSEngine &engine, JSContext *ctx, JSValue val, string &out) {
	QuickJSValue json(ctx, JS_JSONStringify(ctx, val, JS_UNDEFINED, JS_UNDEFINED));
	if (json.IsException()) {
		engine.ThrowPendingException(ctx, "Failed to stringify result to JSON");
	}
	if (JS_IsUndefined(json.Get())) {
		return false;
	}
	out = JSValueToString(engine, ctx, json.Get());
	return true;
}

//! Let JSON.stringify serialize BigInt values (as JSON numbers when JSON.rawJSON exists, strings otherwise)
static void InstallJSONHelpers(QuickJSEngine &engine, JSContext *ctx) {
	static const char *script =
	    "Object.defineProperty(BigInt.prototype, 'toJSON', {value: typeof JSON.rawJSON === 'function' ? "
	    "function() { return JSON.rawJSON(this.toString()); } : function() { return this.toString(); }, "
	    "writable: true, configurable: true});";
	engine.BeginEvaluation();
	QuickJSValue result(ctx, JS_Eval(ctx, script, strlen(script), "<quickjs-init>", JS_EVAL_TYPE_GLOBAL));
	if (result.IsException()) {
		engine.ThrowPendingException(ctx, "Failed to initialize QuickJS context");
	}
}

//===--------------------------------------------------------------------===//
// DuckDB -> JS conversion
//===--------------------------------------------------------------------===//
static constexpr int64_t JS_MAX_SAFE_INTEGER = 9007199254740991LL;

static JSValue BigIntFromString(JSContext *ctx, const string &digits) {
	JSValue global = JS_GetGlobalObject(ctx);
	JSValue ctor = JS_GetPropertyStr(ctx, global, "BigInt");
	JS_FreeValue(ctx, global);
	if (JS_IsException(ctor)) {
		return ctor;
	}
	JSValue arg = JS_NewStringLen(ctx, digits.c_str(), digits.size());
	if (JS_IsException(arg)) {
		JS_FreeValue(ctx, ctor);
		return arg;
	}
	JSValue result = JS_Call(ctx, ctor, JS_UNDEFINED, 1, &arg);
	JS_FreeValue(ctx, arg);
	JS_FreeValue(ctx, ctor);
	return result;
}

//! Integers inside Number.MAX_SAFE_INTEGER become numbers; larger magnitudes become BigInt to keep every digit
static JSValue Int64ToJS(JSContext *ctx, int64_t v) {
	if (v >= -JS_MAX_SAFE_INTEGER && v <= JS_MAX_SAFE_INTEGER) {
		return JS_NewInt64(ctx, v);
	}
	return JS_NewBigInt64(ctx, v);
}

//! Timestamps are int64 counts of a unit since the epoch; infinity is +/- int64 max
static bool IsInfiniteTimestamp(int64_t raw) {
	return raw == NumericLimits<int64_t>::Maximum() || raw == -NumericLimits<int64_t>::Maximum();
}

static JSValue StringToJS(JSContext *ctx, const string &s) {
	return JS_NewStringLen(ctx, s.c_str(), s.size());
}

//! Define an own enumerable property (never invokes setters such as __proto__); takes ownership of val
static int DefineProperty(JSContext *ctx, JSValue obj, const string &name, JSValue val) {
	JSAtom atom = JS_NewAtomLen(ctx, name.c_str(), name.size());
	if (atom == JS_ATOM_NULL) {
		JS_FreeValue(ctx, val);
		return -1;
	}
	int rc = JS_DefinePropertyValue(ctx, obj, atom, val, JS_PROP_C_W_E);
	JS_FreeAtom(ctx, atom);
	return rc;
}

static JSValue ValueToJS(JSContext *ctx, const Value &val);

static JSValue ValuesToJSArray(JSContext *ctx, const vector<Value> &children) {
	JSValue arr = JS_NewArray(ctx);
	if (JS_IsException(arr)) {
		return arr;
	}
	for (idx_t i = 0; i < children.size(); i++) {
		JSValue child = ValueToJS(ctx, children[i]);
		if (JS_IsException(child) || JS_SetPropertyUint32(ctx, arr, static_cast<uint32_t>(i), child) < 0) {
			JS_FreeValue(ctx, arr);
			return JS_EXCEPTION;
		}
	}
	return arr;
}

static JSValue ValueToJS(JSContext *ctx, const Value &val) {
	if (val.IsNull()) {
		return JS_NULL;
	}
	auto &type = val.type();
	switch (type.id()) {
	case LogicalTypeId::BOOLEAN:
		return JS_NewBool(ctx, val.GetValue<bool>());
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::USMALLINT:
		return JS_NewInt32(ctx, val.GetValue<int32_t>());
	case LogicalTypeId::UINTEGER:
	case LogicalTypeId::BIGINT:
		return Int64ToJS(ctx, val.GetValue<int64_t>());
	case LogicalTypeId::UBIGINT: {
		auto v = val.GetValue<uint64_t>();
		if (v <= static_cast<uint64_t>(JS_MAX_SAFE_INTEGER)) {
			return JS_NewInt64(ctx, static_cast<int64_t>(v));
		}
		return BigIntFromString(ctx, val.ToString());
	}
	case LogicalTypeId::HUGEINT: {
		auto v = val.GetValue<hugeint_t>();
		if (v >= hugeint_t(-JS_MAX_SAFE_INTEGER) && v <= hugeint_t(JS_MAX_SAFE_INTEGER)) {
			return JS_NewInt64(ctx, Hugeint::Cast<int64_t>(v));
		}
		return BigIntFromString(ctx, val.ToString());
	}
	case LogicalTypeId::UHUGEINT: {
		auto v = val.GetValue<uhugeint_t>();
		if (v <= uhugeint_t(static_cast<uint64_t>(JS_MAX_SAFE_INTEGER))) {
			return JS_NewInt64(ctx, static_cast<int64_t>(v.lower));
		}
		return BigIntFromString(ctx, val.ToString());
	}
	case LogicalTypeId::FLOAT:
	case LogicalTypeId::DOUBLE:
	case LogicalTypeId::DECIMAL:
		return JS_NewFloat64(ctx, val.GetValue<double>());
	case LogicalTypeId::VARCHAR:
		return StringToJS(ctx, StringValue::Get(val));
	case LogicalTypeId::BLOB: {
		auto &bytes = StringValue::Get(val);
		return JS_NewUint8ArrayCopy(ctx, reinterpret_cast<const uint8_t *>(bytes.data()), bytes.size());
	}
	case LogicalTypeId::DATE: {
		auto d = val.GetValue<date_t>();
		if (!d.IsFinite()) {
			return StringToJS(ctx, val.ToString());
		}
		return JS_NewDate(ctx, static_cast<double>(Date::EpochDays(d)) * 86400000.0);
	}
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ:
	case LogicalTypeId::TIMESTAMP_SEC:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP_NS: {
		auto raw = val.GetValueUnsafe<int64_t>();
		if (IsInfiniteTimestamp(raw)) {
			return StringToJS(ctx, val.ToString());
		}
		double ms;
		switch (type.id()) {
		case LogicalTypeId::TIMESTAMP_SEC:
			ms = static_cast<double>(raw) * 1000.0;
			break;
		case LogicalTypeId::TIMESTAMP_MS:
			ms = static_cast<double>(raw);
			break;
		case LogicalTypeId::TIMESTAMP_NS:
			ms = static_cast<double>(raw) / 1000000.0;
			break;
		default:
			ms = static_cast<double>(raw) / 1000.0;
			break;
		}
		return JS_NewDate(ctx, ms);
	}
	case LogicalTypeId::LIST:
		return ValuesToJSArray(ctx, ListValue::GetChildren(val));
	case LogicalTypeId::ARRAY:
		return ValuesToJSArray(ctx, ArrayValue::GetChildren(val));
	case LogicalTypeId::STRUCT: {
		JSValue obj = JS_NewObject(ctx);
		if (JS_IsException(obj)) {
			return obj;
		}
		auto &children = StructValue::GetChildren(val);
		for (idx_t i = 0; i < children.size(); i++) {
			auto &name = StructType::GetChildName(type, i).GetIdentifierName();
			JSValue child = ValueToJS(ctx, children[i]);
			if (JS_IsException(child) || DefineProperty(ctx, obj, name, child) < 0) {
				JS_FreeValue(ctx, obj);
				return JS_EXCEPTION;
			}
		}
		return obj;
	}
	case LogicalTypeId::MAP: {
		// Like to_json(): a MAP becomes an object keyed by the stringified keys
		JSValue obj = JS_NewObject(ctx);
		if (JS_IsException(obj)) {
			return obj;
		}
		for (auto &entry : MapValue::GetChildren(val)) {
			auto &kv = StructValue::GetChildren(entry);
			JSValue child = ValueToJS(ctx, kv[1]);
			if (JS_IsException(child) || DefineProperty(ctx, obj, kv[0].ToString(), child) < 0) {
				JS_FreeValue(ctx, obj);
				return JS_EXCEPTION;
			}
		}
		return obj;
	}
	case LogicalTypeId::UNION:
		return ValueToJS(ctx, UnionValue::GetValue(val));
	default:
		return StringToJS(ctx, val.ToString());
	}
}

//! Convert one row of a vector, with fast paths for the common flat types
static JSValue VectorRowToJS(JSContext *ctx, const Vector &vec, const UnifiedVectorFormat &format, idx_t row) {
	auto idx = format.sel->get_index(row);
	if (!format.validity.RowIsValid(idx)) {
		return JS_NULL;
	}
	switch (vec.GetType().id()) {
	case LogicalTypeId::BOOLEAN:
		return JS_NewBool(ctx, format.GetData<bool>()[idx]);
	case LogicalTypeId::TINYINT:
		return JS_NewInt32(ctx, format.GetData<int8_t>()[idx]);
	case LogicalTypeId::SMALLINT:
		return JS_NewInt32(ctx, format.GetData<int16_t>()[idx]);
	case LogicalTypeId::INTEGER:
		return JS_NewInt32(ctx, format.GetData<int32_t>()[idx]);
	case LogicalTypeId::BIGINT:
		return Int64ToJS(ctx, format.GetData<int64_t>()[idx]);
	case LogicalTypeId::FLOAT:
		return JS_NewFloat64(ctx, format.GetData<float>()[idx]);
	case LogicalTypeId::DOUBLE:
		return JS_NewFloat64(ctx, format.GetData<double>()[idx]);
	case LogicalTypeId::VARCHAR: {
		auto &s = format.GetData<string_t>()[idx];
		return JS_NewStringLen(ctx, s.GetData(), s.GetSize());
	}
	case LogicalTypeId::BLOB: {
		auto &s = format.GetData<string_t>()[idx];
		return JS_NewUint8ArrayCopy(ctx, reinterpret_cast<const uint8_t *>(s.GetData()), s.GetSize());
	}
	default:
		return ValueToJS(ctx, vec.GetValue(row));
	}
}

//===--------------------------------------------------------------------===//
// quickjs(code) scalar
//===--------------------------------------------------------------------===//

struct QuickJSExecuteLocalState : public FunctionLocalState {
	explicit QuickJSExecuteLocalState(ClientContext &context) : engine(context, ReadLimits(context)) {
	}
	QuickJSEngine engine;
};

static unique_ptr<FunctionLocalState> QuickJSExecuteInit(ExpressionState &state, const BoundFunctionExpression &,
                                                         FunctionData *) {
	return make_uniq<QuickJSExecuteLocalState>(state.GetContext());
}

static void QuickJSExecute(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &engine = ExecuteFunctionState::GetFunctionState(state)->Cast<QuickJSExecuteLocalState>().engine;
	UnaryExecutor::Execute<string_t, string_t>(args.data[0], result, args.size(), [&](string_t script) {
		// Each row gets a fresh context so global declarations never leak between rows; the runtime is reused
		auto context = engine.NewContext();
		JSContext *ctx = context->Get();

		// JS_Eval requires a NUL-terminated buffer
		std::string script_str = script.GetString();
		engine.BeginEvaluation();
		QuickJSValue val(ctx, JS_Eval(ctx, script_str.c_str(), script_str.length(), "<eval>",
		                              JS_EVAL_TYPE_GLOBAL | JS_EVAL_FLAG_STRICT));
		if (val.IsException()) {
			engine.ThrowPendingException(ctx);
		}
		engine.SettlePromise(ctx, val);
		auto text = JSValueToString(engine, ctx, val.Get());
		val.Reset();
		engine.DrainPendingJobs(ctx);
		return StringVector::AddString(result, text);
	});
}

//===--------------------------------------------------------------------===//
// quickjs_eval(function, args...) scalar
//===--------------------------------------------------------------------===//

struct QuickJSEvalLocalState : public FunctionLocalState {
	explicit QuickJSEvalLocalState(ClientContext &context) : engine(context, ReadLimits(context)) {
	}

	//! Return the compiled function for this script, compiling it only when the script text changes
	JSValue GetFunction(const string_t &script) {
		if (!js_context) {
			js_context = engine.NewContext();
			InstallJSONHelpers(engine, js_context->Get());
			this_obj = QuickJSValue(js_context->Get(), JS_NewObject(js_context->Get()));
		}
		if (has_function && script == string_t(cached_script)) {
			return function.Get();
		}
		JSContext *ctx = js_context->Get();
		has_function = false;
		function.Reset();
		cached_script = script.GetString();
		engine.BeginEvaluation();
		QuickJSValue func(ctx, JS_Eval(ctx, cached_script.c_str(), cached_script.length(), "<eval>",
		                               JS_EVAL_TYPE_GLOBAL | JS_EVAL_FLAG_STRICT));
		if (func.IsException()) {
			engine.ThrowPendingException(ctx);
		}
		if (!JS_IsFunction(ctx, func.Get())) {
			throw InvalidInputException("First argument to quickjs_eval must be a function");
		}
		function = std::move(func);
		has_function = true;
		return function.Get();
	}

	// Declaration order matters: values are freed before the context, the context before the runtime
	QuickJSEngine engine;
	unique_ptr<QuickJSContext> js_context;
	QuickJSValue this_obj;
	QuickJSValue function;
	string cached_script;
	bool has_function = false;
};

static unique_ptr<FunctionLocalState> QuickJSEvalInit(ExpressionState &state, const BoundFunctionExpression &,
                                                      FunctionData *) {
	return make_uniq<QuickJSEvalLocalState>(state.GetContext());
}

static void QuickJSEval(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &local = ExecuteFunctionState::GetFunctionState(state)->Cast<QuickJSEvalLocalState>();
	auto &engine = local.engine;
	auto count = args.size();
	idx_t n_js_args = args.ColumnCount() - 1;

	vector<UnifiedVectorFormat> formats(args.ColumnCount());
	for (idx_t c = 0; c < args.ColumnCount(); c++) {
		args.data[c].ToUnifiedFormat(formats[c]);
	}
	auto &script_format = formats[0];
	auto script_data = script_format.GetData<string_t>();

	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto result_data = FlatVector::GetDataMutable<string_t>(result);
	auto &result_validity = FlatVector::ValidityMutable(result);

	vector<QuickJSValue> js_args;
	vector<JSValue> js_arg_values;
	js_args.reserve(n_js_args);
	js_arg_values.reserve(n_js_args);

	string json;
	for (idx_t i = 0; i < count; i++) {
		auto script_idx = script_format.sel->get_index(i);
		if (!script_format.validity.RowIsValid(script_idx)) {
			result_validity.SetInvalid(i);
			continue;
		}
		JSValue func = local.GetFunction(script_data[script_idx]);
		JSContext *ctx = local.js_context->Get();

		js_args.clear();
		js_arg_values.clear();
		for (idx_t j = 0; j < n_js_args; j++) {
			JSValue arg = VectorRowToJS(ctx, args.data[j + 1], formats[j + 1], i);
			if (JS_IsException(arg)) {
				engine.ThrowPendingException(ctx, "Failed to convert argument");
			}
			js_args.emplace_back(ctx, arg);
			js_arg_values.push_back(arg);
		}

		engine.BeginEvaluation();
		QuickJSValue js_result(
		    ctx, JS_Call(ctx, func, local.this_obj.Get(), static_cast<int>(n_js_args), js_arg_values.data()));
		if (js_result.IsException()) {
			engine.ThrowPendingException(ctx);
		}
		engine.SettlePromise(ctx, js_result);
		// undefined (and functions/symbols) have no JSON form: return SQL NULL
		if (!JSValueToJSON(engine, ctx, js_result.Get(), json)) {
			result_validity.SetInvalid(i);
			continue;
		}
		result_data[i] = StringVector::AddString(result, json);
	}
	if (args.AllConstant()) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

//===--------------------------------------------------------------------===//
// quickjs(code, args...) table function
//===--------------------------------------------------------------------===//

class QuickJSTableBindData : public TableFunctionData {
public:
	std::string js_code;
	vector<Value> parameters;
};

class QuickJSTableGlobalState : public GlobalTableFunctionState {
public:
	//! One entry per array element; an invalid entry is a SQL NULL (element without JSON form, e.g. undefined)
	vector<string> results;
	vector<bool> valid;
	idx_t current_index = 0;
};

static void QuickJSTableFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &state = data_p.global_state->Cast<QuickJSTableGlobalState>();

	if (state.current_index >= state.results.size()) {
		output.SetChildCardinality(0);
		return;
	}

	idx_t chunk_size = MinValue<idx_t>(STANDARD_VECTOR_SIZE, state.results.size() - state.current_index);
	output.SetChildCardinality(chunk_size);

	auto &result_vector = output.data[0];
	auto result_data = FlatVector::GetDataMutable<string_t>(result_vector);
	auto &result_validity = FlatVector::ValidityMutable(result_vector);

	for (idx_t i = 0; i < chunk_size; i++) {
		auto src = state.current_index + i;
		if (!state.valid[src]) {
			result_validity.SetInvalid(i);
			continue;
		}
		result_data[i] = StringVector::AddString(result_vector, state.results[src]);
	}

	state.current_index += chunk_size;
}

static unique_ptr<FunctionData> QuickJSTableBind(ClientContext &context, TableFunctionBindInput &input,
                                                 vector<LogicalType> &return_types, vector<Identifier> &names) {
	if (input.inputs.size() < 1) {
		throw BinderException("quickjs() expects at least one argument (JavaScript code)");
	}

	return_types.push_back(LogicalType::JSON());
	names.push_back("result");

	auto bind_data = make_uniq<QuickJSTableBindData>();
	if (input.inputs[0].IsNull()) {
		throw BinderException("quickjs() requires a non-null JavaScript code string");
	}
	bind_data->js_code = input.inputs[0].GetValue<string>();
	for (idx_t i = 1; i < input.inputs.size(); i++) {
		bind_data->parameters.push_back(input.inputs[i]);
	}
	return std::move(bind_data);
}

static unique_ptr<GlobalTableFunctionState> QuickJSTableInit(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<QuickJSTableBindData>();
	auto result = make_uniq<QuickJSTableGlobalState>();

	QuickJSEngine engine(context, ReadLimits(context));
	auto js_context = engine.NewContext();
	JSContext *ctx = js_context->Get();
	InstallJSONHelpers(engine, ctx);

	// Expose the parameters as globals arg0, arg1, ... and parsed_arg0 (arg0 parsed as JSON when it is a string
	// holding valid JSON, otherwise arg0 itself)
	{
		QuickJSValue global(ctx, JS_GetGlobalObject(ctx));
		for (idx_t i = 0; i < bind_data.parameters.size(); i++) {
			JSValue arg = ValueToJS(ctx, bind_data.parameters[i]);
			if (JS_IsException(arg)) {
				engine.ThrowPendingException(ctx, "Failed to convert argument");
			}
			if (i == 0) {
				JSValue parsed = JS_UNDEFINED;
				if (JS_IsString(arg)) {
					size_t len = 0;
					const char *c_str = JS_ToCStringLen(ctx, &len, arg);
					if (c_str) {
						engine.BeginEvaluation();
						parsed = JS_ParseJSON(ctx, c_str, len, "<arg0>");
						JS_FreeCString(ctx, c_str);
					}
					if (JS_IsException(parsed) || !c_str) {
						JS_FreeValue(ctx, JS_GetException(ctx));
						parsed = JS_UNDEFINED;
					}
				}
				if (JS_IsUndefined(parsed)) {
					parsed = JS_DupValue(ctx, arg);
				}
				if (JS_SetPropertyStr(ctx, global.Get(), "parsed_arg0", parsed) < 0) {
					JS_FreeValue(ctx, arg);
					engine.ThrowPendingException(ctx, "Failed to define parsed_arg0");
				}
			}
			auto name = "arg" + std::to_string(i);
			if (JS_SetPropertyStr(ctx, global.Get(), name.c_str(), arg) < 0) {
				engine.ThrowPendingException(ctx, "Failed to define " + name);
			}
		}
	}

	// The code runs as a global script: its completion value (the last expression) is the result, so both
	// plain expressions and multi-statement scripts work
	engine.BeginEvaluation();
	QuickJSValue val(ctx, JS_Eval(ctx, bind_data.js_code.c_str(), bind_data.js_code.length(), "<eval>",
	                              JS_EVAL_TYPE_GLOBAL | JS_EVAL_FLAG_STRICT));
	if (val.IsException()) {
		engine.ThrowPendingException(ctx);
	}
	engine.SettlePromise(ctx, val);
	if (!JS_IsArray(val.Get())) {
		throw InvalidInputException("JavaScript code must return an array");
	}

	int64_t length = 0;
	if (JS_GetLength(ctx, val.Get(), &length) < 0) {
		engine.ThrowPendingException(ctx);
	}
	string json;
	for (int64_t i = 0; i < length; i++) {
		engine.BeginEvaluation();
		QuickJSValue element(ctx, JS_GetPropertyInt64(ctx, val.Get(), i));
		if (element.IsException()) {
			engine.ThrowPendingException(ctx);
		}
		if (JSValueToJSON(engine, ctx, element.Get(), json)) {
			result->results.push_back(std::move(json));
			result->valid.push_back(true);
		} else {
			result->results.emplace_back();
			result->valid.push_back(false);
		}
	}
	return std::move(result);
}

//===--------------------------------------------------------------------===//
// Registration
//===--------------------------------------------------------------------===//

static void LoadInternal(ExtensionLoader &loader) {
	loader.SetDescription("QuickJS embedded scripting language");

	auto &config = DBConfig::GetConfig(loader.GetDatabaseInstance());
	config.AddExtensionOption(MEMORY_LIMIT_SETTING,
	                          "Maximum heap size of each QuickJS runtime (one per thread and query), e.g. '256MB'. "
	                          "'none' disables the limit.",
	                          LogicalType::VARCHAR, Value(DEFAULT_MEMORY_LIMIT), ValidateMemoryLimit);
	config.AddExtensionOption(TIMEOUT_SETTING,
	                          "Maximum wall-clock time in milliseconds of a single QuickJS evaluation (one row of "
	                          "quickjs()/quickjs_eval(), or the whole quickjs table function). 0 disables the timeout.",
	                          LogicalType::BIGINT, Value::BIGINT(0), ValidateTimeout);

	//===--------------------------------------------------------------------===//
	// quickjs(code) - Scalar function that executes JavaScript and returns VARCHAR
	//===--------------------------------------------------------------------===//
	ScalarFunctionSet quickjs_set("quickjs");

	auto quickjs_scalar_function =
	    ScalarFunction({Positional("code", LogicalType::VARCHAR)}, LogicalType::VARCHAR, QuickJSExecute);
	quickjs_scalar_function.SetInitStateCallback(QuickJSExecuteInit);
	quickjs_scalar_function.SetVolatile();
	quickjs_scalar_function.SetFallible();
	quickjs_set.AddFunction(quickjs_scalar_function);

	CreateScalarFunctionInfo quickjs_info(quickjs_set);
	FunctionDescription quickjs_desc;
	quickjs_desc.description =
	    "Execute JavaScript code in a fresh QuickJS context and return the completion value (the value of the last "
	    "expression) converted to a string.";
	quickjs_desc.parameter_types = {LogicalType::VARCHAR};
	quickjs_desc.parameter_names = {"code"};
	quickjs_desc.examples = {"quickjs('1 + 2')", "quickjs('\"hello\".toUpperCase()')",
	                         "quickjs('let a = [3, 1, 2]; a.sort().join(\",\")')"};
	quickjs_desc.categories = {"scripting"};
	quickjs_info.descriptions.push_back(quickjs_desc);

	loader.RegisterFunction(quickjs_info);

	//===--------------------------------------------------------------------===//
	// quickjs_eval(function, ...args) - Scalar function that executes a JS function with arguments
	//===--------------------------------------------------------------------===//
	ScalarFunctionSet quickjs_eval_set("quickjs_eval");

	auto quickjs_eval_function =
	    ScalarFunction({Positional("function", LogicalType::VARCHAR)}, LogicalType::JSON(), QuickJSEval);
	quickjs_eval_function.GetSignature().AddArgs("args", LogicalType::ANY);
	quickjs_eval_function.SetInitStateCallback(QuickJSEvalInit);
	quickjs_eval_function.SetVolatile();
	quickjs_eval_function.SetFallible();
	// NULL arguments are passed to JavaScript as null instead of short-circuiting to a NULL result
	quickjs_eval_function.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	quickjs_eval_set.AddFunction(quickjs_eval_function);

	CreateScalarFunctionInfo quickjs_eval_info(quickjs_eval_set);
	FunctionDescription quickjs_eval_desc;
	quickjs_eval_desc.description =
	    "Call a JavaScript function with the provided arguments and return its result as JSON (SQL NULL when the "
	    "result is undefined). Arguments are converted to native JavaScript values: numbers, strings, booleans, "
	    "BigInt (integers beyond 2^53), Date (DATE/TIMESTAMP), Uint8Array (BLOB), arrays (LIST/ARRAY) and objects "
	    "(STRUCT/MAP).";
	quickjs_eval_desc.parameter_types = {LogicalType::VARCHAR};
	quickjs_eval_desc.parameter_names = {"function"};
	quickjs_eval_desc.examples = {"quickjs_eval('(a, b) => a + b', 1, 2)", "quickjs_eval('(x) => x * 2', 21)",
	                              "quickjs_eval('(l) => l.map(x => x * x)', [1, 2, 3])"};
	quickjs_eval_desc.categories = {"scripting"};
	quickjs_eval_info.descriptions.push_back(quickjs_eval_desc);

	loader.RegisterFunction(quickjs_eval_info);

	//===--------------------------------------------------------------------===//
	// quickjs(code, ...args) - Table function that returns array elements as rows
	//===--------------------------------------------------------------------===//
	TableFunctionSet quickjs_table_set("quickjs");

	auto quickjs_table_function =
	    TableFunction(FunctionSignature({Positional("code", LogicalType::VARCHAR)}, LogicalType::INVALID),
	                  QuickJSTableFunction, QuickJSTableBind, QuickJSTableInit);
	quickjs_table_function.GetSignature().AddArgs("args", LogicalType::ANY);
	quickjs_table_set.AddFunction(quickjs_table_function);

	CreateTableFunctionInfo quickjs_table_info(quickjs_table_set);
	FunctionDescription quickjs_table_desc;
	quickjs_table_desc.description =
	    "Execute JavaScript code that evaluates to an array; each element becomes a row of the JSON column "
	    "'result'. Extra arguments are available as arg0, arg1, ... and parsed_arg0 (arg0 parsed as JSON when it "
	    "is a JSON string).";
	quickjs_table_desc.parameter_types = {LogicalType::VARCHAR};
	quickjs_table_desc.parameter_names = {"code"};
	quickjs_table_desc.examples = {"SELECT * FROM quickjs('[1, 2, 3].map(x => x * 2)')",
	                               "SELECT * FROM quickjs('parsed_arg0.map(x => x * arg1)', '[1, 2, 3]', 10)"};
	quickjs_table_desc.categories = {"scripting"};
	quickjs_table_info.descriptions.push_back(quickjs_table_desc);

	loader.RegisterFunction(quickjs_table_info);

	QueryFarmSendTelemetry(loader, "quickjs", QUICKJS_EXTENSION_VERSION);
}

void QuickjsExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}

std::string QuickjsExtension::Name() {
	return "quickjs";
}

std::string QuickjsExtension::Version() const {
	return QUICKJS_EXTENSION_VERSION;
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(quickjs, loader) {
	duckdb::LoadInternal(loader);
}

DUCKDB_EXTENSION_API const char *quickjs_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}
