#define DUCKDB_EXTENSION_MAIN

#include "quickjs_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>
#include "duckdb/common/types/value.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"


#include "quickjs.h"

namespace duckdb {

//===--------------------------------------------------------------------===//
// RAII Wrappers for QuickJS Resources
//===--------------------------------------------------------------------===//

// RAII wrapper for JSRuntime - ensures proper cleanup including double GC
class QuickJSRuntime {
public:
	QuickJSRuntime() : rt(JS_NewRuntime()) {
		if (!rt) {
			throw IOException("Failed to create QuickJS runtime.");
		}
	}

	~QuickJSRuntime() {
		if (rt) {
			// Run GC twice to ensure complete cleanup of reference cycles
			JS_RunGC(rt);
			JS_RunGC(rt);
			JS_FreeRuntime(rt);
		}
	}

	// Non-copyable
	QuickJSRuntime(const QuickJSRuntime &) = delete;
	QuickJSRuntime &operator=(const QuickJSRuntime &) = delete;

	// Moveable
	QuickJSRuntime(QuickJSRuntime &&other) noexcept : rt(other.rt) {
		other.rt = nullptr;
	}

	QuickJSRuntime &operator=(QuickJSRuntime &&other) noexcept {
		if (this != &other) {
			if (rt) {
				JS_RunGC(rt);
				JS_RunGC(rt);
				JS_FreeRuntime(rt);
			}
			rt = other.rt;
			other.rt = nullptr;
		}
		return *this;
	}

	JSRuntime *Get() const {
		return rt;
	}

private:
	JSRuntime *rt;
};

// RAII wrapper for JSContext - ensures proper cleanup
class QuickJSContext {
public:
	explicit QuickJSContext(QuickJSRuntime &runtime) : ctx(JS_NewContext(runtime.Get())) {
		if (!ctx) {
			throw IOException("Failed to create QuickJS context.");
		}
	}

	~QuickJSContext() {
		if (ctx) {
			JS_FreeContext(ctx);
		}
	}

	// Non-copyable
	QuickJSContext(const QuickJSContext &) = delete;
	QuickJSContext &operator=(const QuickJSContext &) = delete;

	// Moveable
	QuickJSContext(QuickJSContext &&other) noexcept : ctx(other.ctx) {
		other.ctx = nullptr;
	}

	QuickJSContext &operator=(QuickJSContext &&other) noexcept {
		if (this != &other) {
			if (ctx) {
				JS_FreeContext(ctx);
			}
			ctx = other.ctx;
			other.ctx = nullptr;
		}
		return *this;
	}

	JSContext *Get() const {
		return ctx;
	}

private:
	JSContext *ctx;
};

// RAII wrapper for JSValue - ensures proper cleanup
class QuickJSValue {
public:
	QuickJSValue(JSContext *ctx, JSValue val) : ctx(ctx), val(val) {
	}

	~QuickJSValue() {
		if (ctx) {
			JS_FreeValue(ctx, val);
		}
	}

	// Non-copyable
	QuickJSValue(const QuickJSValue &) = delete;
	QuickJSValue &operator=(const QuickJSValue &) = delete;

	// Moveable
	QuickJSValue(QuickJSValue &&other) noexcept : ctx(other.ctx), val(other.val) {
		other.ctx = nullptr;
		other.val = JS_UNDEFINED;
	}

	QuickJSValue &operator=(QuickJSValue &&other) noexcept {
		if (this != &other) {
			if (ctx) {
				JS_FreeValue(ctx, val);
			}
			ctx = other.ctx;
			val = other.val;
			other.ctx = nullptr;
			other.val = JS_UNDEFINED;
		}
		return *this;
	}

	JSValue Get() const {
		return val;
	}

	JSValue Release() {
		ctx = nullptr;
		return val;
	}

	bool IsException() const {
		return JS_IsException(val);
	}

	bool IsFunction() const {
		return JS_IsFunction(ctx, val);
	}

	bool IsArray() const {
		return JS_IsArray(val);
	}

private:
	JSContext *ctx;
	JSValue val;
};

// Helper to extract and throw JS exception
static void ThrowJSException(JSContext *ctx, const std::string &prefix = "") {
	JSValue exception = JS_GetException(ctx);
	const char *exception_c_str = JS_ToCString(ctx, exception);
	std::string exception_str(exception_c_str);
	JS_FreeCString(ctx, exception_c_str);
	JS_FreeValue(ctx, exception);
	if (prefix.empty()) {
		throw InvalidInputException(exception_str);
	} else {
		throw InvalidInputException("%s: %s", prefix, exception_str);
	}
}

//===--------------------------------------------------------------------===//
// Bind Data and Global State Classes
//===--------------------------------------------------------------------===//

// Add the bind data and global state classes
class QuickJSTableBindData : public TableFunctionData {
public:
	QuickJSTableBindData() {
	}
	
	std::string js_code;
	vector<Value> parameters;
};

class QuickJSTableGlobalState : public GlobalTableFunctionState {
public:
	QuickJSTableGlobalState() : current_index(0) {
	}

	vector<string> results;
	idx_t current_index;
};

static JSValue DuckDBValueToJSValue(JSContext *ctx, const Value &val) {
	if (val.IsNull()) {
		return JS_NULL;
	}
	switch (val.type().id()) {
	case LogicalTypeId::BOOLEAN:
		return JS_NewBool(ctx, val.GetValue<bool>());
	case LogicalTypeId::INTEGER:
		return JS_NewInt32(ctx, val.GetValue<int32_t>());
	case LogicalTypeId::BIGINT:
		return JS_NewInt64(ctx, val.GetValue<int64_t>());
	case LogicalTypeId::FLOAT:
		return JS_NewFloat64(ctx, val.GetValue<float>());
	case LogicalTypeId::DOUBLE:
		return JS_NewFloat64(ctx, val.GetValue<double>());
	case LogicalTypeId::VARCHAR:
		return JS_NewString(ctx, val.GetValue<string>().c_str());
	default:
		return JS_NewString(ctx, val.ToString().c_str());
	}
}

static void QuickJSEval(DataChunk &args, ExpressionState &state, Vector &result) {
	auto count = args.size();

	Vector &script_vector = args.data[0];
	script_vector.Flatten(count);
	auto script_data = FlatVector::GetData<string_t>(script_vector);
	auto &script_validity = FlatVector::Validity(script_vector);

	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto result_data = FlatVector::GetData<string_t>(result);
	auto &result_validity = FlatVector::Validity(result);

	for (idx_t i = 0; i < count; i++) {
		if (!script_validity.RowIsValid(i)) {
			result_validity.SetInvalid(i);
			continue;
		}

		QuickJSRuntime runtime;
		QuickJSContext context(runtime);
		JSContext *ctx = context.Get();

		// Create a fresh global object to ensure isolation
		QuickJSValue global_obj(ctx, JS_NewObject(ctx));
		JS_SetPropertyStr(ctx, global_obj.Get(), "console", JS_NewObject(ctx));

		auto script = script_data[i];
		QuickJSValue func(ctx, JS_Eval(ctx, script.GetData(), script.GetSize(), "<eval>", JS_EVAL_TYPE_GLOBAL | JS_EVAL_FLAG_STRICT));

		if (func.IsException()) {
			ThrowJSException(ctx);
		}

		if (!func.IsFunction()) {
			throw InvalidInputException("First argument to quickjs_eval must be a function");
		}

		// Convert arguments to JS values
		idx_t n_js_args = args.ColumnCount() - 1;
		std::vector<QuickJSValue> js_args;
		std::vector<JSValue> js_arg_values;
		js_args.reserve(n_js_args);
		js_arg_values.reserve(n_js_args);
		for (idx_t j = 0; j < n_js_args; j++) {
			auto val = args.data[j + 1].GetValue(i);
			js_args.emplace_back(ctx, DuckDBValueToJSValue(ctx, val));
			js_arg_values.push_back(js_args.back().Get());
		}

		QuickJSValue js_result(ctx, JS_Call(ctx, func.Get(), global_obj.Get(), n_js_args, js_arg_values.data()));

		if (js_result.IsException()) {
			ThrowJSException(ctx);
		}

		QuickJSValue json_string_val(ctx, JS_JSONStringify(ctx, js_result.Get(), JS_UNDEFINED, JS_UNDEFINED));

		if (json_string_val.IsException()) {
			ThrowJSException(ctx, "Failed to stringify result to JSON");
		}

		size_t len;
		const char *json_c_str = JS_ToCStringLen(ctx, &len, json_string_val.Get());
		result_data[i] = StringVector::AddString(result, json_c_str, len);
		JS_FreeCString(ctx, json_c_str);
	}
}

static void QuickJSExecute(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &script_vector = args.data[0];
	UnaryExecutor::Execute<string_t, string_t>(script_vector, result, args.size(), [&](string_t script) {
		QuickJSRuntime runtime;
		QuickJSContext context(runtime);
		JSContext *ctx = context.Get();

		// Create a fresh global object to ensure isolation
		QuickJSValue global_obj(ctx, JS_NewObject(ctx));
		JS_SetPropertyStr(ctx, global_obj.Get(), "console", JS_NewObject(ctx));

		QuickJSValue val(ctx, JS_Eval(ctx, script.GetData(), script.GetSize(), "<eval>", JS_EVAL_TYPE_GLOBAL | JS_EVAL_FLAG_STRICT));

		if (val.IsException()) {
			ThrowJSException(ctx);
		}

		const char *c_str = JS_ToCString(ctx, val.Get());
		string_t result_str = StringVector::AddString(result, c_str);
		JS_FreeCString(ctx, c_str);

		return result_str;
	});
}

static void QuickJSTableFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &state = data_p.global_state->Cast<QuickJSTableGlobalState>();

	if (state.current_index >= state.results.size()) {
		output.SetCardinality(0);
		return;
	}

	idx_t chunk_size = MinValue<idx_t>(STANDARD_VECTOR_SIZE, state.results.size() - state.current_index);
	output.SetCardinality(chunk_size);

	// For now, we'll return a single column with the JSON string representation
	// In the future, we could parse the JSON and create multiple columns
	auto &result_vector = output.data[0];
	auto result_data = FlatVector::GetData<string_t>(result_vector);

	for (idx_t i = 0; i < chunk_size; i++) {
		result_data[i] = StringVector::AddString(result_vector, state.results[state.current_index + i]);
	}

	state.current_index += chunk_size;
}

static unique_ptr<FunctionData> QuickJSTableBind(ClientContext &context, TableFunctionBindInput &input,
                                                 vector<LogicalType> &return_types, vector<string> &names) {
	// Check that we have at least one argument (the JavaScript code)
	if (input.inputs.size() < 1) {
		throw BinderException("quickjs() expects at least one argument (JavaScript code)");
	}

	// For now, return a single JSON column
	return_types.push_back(LogicalType::JSON());
	names.push_back("result");

	auto bind_data = make_uniq<QuickJSTableBindData>();
	
	// Extract the JavaScript code from the first argument
	if (!input.inputs[0].IsNull()) {
		bind_data->js_code = input.inputs[0].GetValue<string>();
	} else {
		throw BinderException("quickjs() requires a non-null JavaScript code string");
	}

	// Store any additional parameters
	for (idx_t i = 1; i < input.inputs.size(); i++) {
		bind_data->parameters.push_back(input.inputs[i]);
	}

	return std::move(bind_data);
}

static unique_ptr<GlobalTableFunctionState> QuickJSTableInit(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<QuickJSTableBindData>();
	auto result = make_uniq<QuickJSTableGlobalState>();

	QuickJSRuntime runtime;
	QuickJSContext js_context(runtime);
	JSContext *ctx = js_context.Get();

	// Create a fresh global object to ensure isolation
	QuickJSValue global_obj(ctx, JS_NewObject(ctx));
	JS_SetPropertyStr(ctx, global_obj.Get(), "console", JS_NewObject(ctx));

	// Create a function that takes the parameters and returns the result
	std::string js_function_code = "(function(";
	for (idx_t i = 0; i < bind_data.parameters.size(); i++) {
		if (i > 0) js_function_code += ", ";
		js_function_code += "arg" + std::to_string(i);
	}
	js_function_code += ") { ";

	// Parse JSON strings for the first parameter if it's a string (likely an array/object)
	if (!bind_data.parameters.empty() && bind_data.parameters[0].type() == LogicalType::VARCHAR) {
		js_function_code += "let parsed_arg0 = JSON.parse(arg0); ";
	}

	js_function_code += "return " + bind_data.js_code + "; })";

	// Compile the function
	QuickJSValue func_val(ctx, JS_Eval(ctx, js_function_code.c_str(), js_function_code.length(), "<eval>", JS_EVAL_TYPE_GLOBAL | JS_EVAL_FLAG_STRICT));
	if (func_val.IsException()) {
		ThrowJSException(ctx, "Failed to compile JavaScript function");
	}

	// Convert parameters to JavaScript values
	std::vector<QuickJSValue> js_args;
	std::vector<JSValue> js_arg_values;
	for (const auto &param : bind_data.parameters) {
		js_args.emplace_back(ctx, DuckDBValueToJSValue(ctx, param));
		js_arg_values.push_back(js_args.back().Get());
	}

	// Call the function
	QuickJSValue val(ctx, JS_Call(ctx, func_val.Get(), global_obj.Get(), js_arg_values.size(), js_arg_values.data()));

	if (val.IsException()) {
		ThrowJSException(ctx);
	}

	// Check if the result is an array
	if (!val.IsArray()) {
		throw InvalidInputException("JavaScript code must return an array");
	}

	// Get array length
	JSAtom length_atom = JS_NewAtom(ctx, "length");
	JSValue length_val = JS_GetProperty(ctx, val.Get(), length_atom);
	int32_t length = JS_VALUE_GET_INT(length_val);
	JS_FreeValue(ctx, length_val);
	JS_FreeAtom(ctx, length_atom);

	// Extract each array element as a separate row
	for (int32_t i = 0; i < length; i++) {
		QuickJSValue element(ctx, JS_GetPropertyUint32(ctx, val.Get(), i));
		QuickJSValue json_string_val(ctx, JS_JSONStringify(ctx, element.Get(), JS_UNDEFINED, JS_UNDEFINED));

		if (json_string_val.IsException()) {
			ThrowJSException(ctx, "Failed to stringify result to JSON");
		}

		size_t len;
		const char *json_c_str = JS_ToCStringLen(ctx, &len, json_string_val.Get());
		std::string json_str(json_c_str, len);
		JS_FreeCString(ctx, json_c_str);

		result->results.push_back(json_str);
	}

	return std::move(result);
}

static void LoadInternal(ExtensionLoader &loader) {
	loader.SetDescription("QuickJS embedded scripting language");

	//===--------------------------------------------------------------------===//
	// quickjs(code) - Scalar function that executes JavaScript and returns VARCHAR
	//===--------------------------------------------------------------------===//
	ScalarFunctionSet quickjs_set("quickjs");

	auto quickjs_scalar_function =
	    ScalarFunction({LogicalType::VARCHAR}, LogicalType::VARCHAR, QuickJSExecute);
	quickjs_set.AddFunction(quickjs_scalar_function);

	CreateScalarFunctionInfo quickjs_info(quickjs_set);
	FunctionDescription quickjs_desc;
	quickjs_desc.description = "Execute JavaScript code and return the result as a string";
	quickjs_desc.parameter_types = {LogicalType::VARCHAR};
	quickjs_desc.parameter_names = {"code"};
	quickjs_desc.examples = {"quickjs('1 + 2')", "quickjs('\"hello\".toUpperCase()')"};
	quickjs_desc.categories = {"text"};
	quickjs_info.descriptions.push_back(quickjs_desc);

	loader.RegisterFunction(quickjs_info);

	//===--------------------------------------------------------------------===//
	// quickjs_eval(function, ...args) - Scalar function that executes a JS function with arguments
	//===--------------------------------------------------------------------===//
	ScalarFunctionSet quickjs_eval_set("quickjs_eval");

	auto quickjs_eval_function =
	    ScalarFunction({LogicalType::VARCHAR}, LogicalType::JSON(), QuickJSEval);
	quickjs_eval_function.varargs = LogicalType::ANY;
	quickjs_eval_set.AddFunction(quickjs_eval_function);

	CreateScalarFunctionInfo quickjs_eval_info(quickjs_eval_set);
	FunctionDescription quickjs_eval_desc;
	quickjs_eval_desc.description = "Execute a JavaScript function with the provided arguments and return the result as JSON";
	quickjs_eval_desc.parameter_types = {LogicalType::VARCHAR};
	quickjs_eval_desc.parameter_names = {"function"};
	quickjs_eval_desc.examples = {"quickjs_eval('(a, b) => a + b', 1, 2)", "quickjs_eval('(x) => x * 2', 21)"};
	quickjs_eval_desc.categories = {"text"};
	quickjs_eval_info.descriptions.push_back(quickjs_eval_desc);

	loader.RegisterFunction(quickjs_eval_info);

	//===--------------------------------------------------------------------===//
	// quickjs(code, ...args) - Table function that returns array elements as rows
	//===--------------------------------------------------------------------===//
	TableFunctionSet quickjs_table_set("quickjs");

	auto quickjs_table_function = TableFunction({LogicalType::VARCHAR}, QuickJSTableFunction, QuickJSTableBind, QuickJSTableInit);
	quickjs_table_function.varargs = LogicalType::ANY;
	quickjs_table_set.AddFunction(quickjs_table_function);

	CreateTableFunctionInfo quickjs_table_info(quickjs_table_set);
	FunctionDescription quickjs_table_desc;
	quickjs_table_desc.description = "Execute JavaScript code that returns an array, with each element becoming a row";
	quickjs_table_desc.parameter_types = {LogicalType::VARCHAR};
	quickjs_table_desc.parameter_names = {"code"};
	quickjs_table_desc.examples = {"SELECT * FROM quickjs('[1, 2, 3].map(x => x * 2)')"};
	quickjs_table_desc.categories = {"table"};
	quickjs_table_info.descriptions.push_back(quickjs_table_desc);

	loader.RegisterFunction(quickjs_table_info);
}

void QuickjsExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}

std::string QuickjsExtension::Name() {
	return "quickjs";
}

std::string QuickjsExtension::Version() const {
#ifdef EXT_VERSION_QUICKJS
	return EXT_VERSION_QUICKJS;
#else
	return "";
#endif
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
