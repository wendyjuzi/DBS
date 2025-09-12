#include "db_core.h"
#ifdef _MSC_VER
#pragma execution_character_set("utf-8")
#endif

// pybind11 绑定：将 C++ 类/函数暴露给 Python
PYBIND11_MODULE(db_core, m) {
	m.doc() = "Simplified Database Core (C++ Backend)";

	// 1. 绑定 DataType 枚举
	py::enum_<DataType>(m, "DataType")
		.value("INT", DataType::INT)
		.value("STRING", DataType::STRING)
		.value("DOUBLE", DataType::DOUBLE)
		.export_values();

	// 2. 绑定 Column 结构体
	py::class_<Column>(m, "Column")
		.def(py::init<std::string, DataType, bool>(), py::arg("name"), py::arg("type"), py::arg("is_primary_key"))
		.def_readonly("name", &Column::name)
		.def_readonly("type", &Column::type)
		.def_readonly("is_primary_key", &Column::is_primary_key);

	// 3. 绑定 Row 类
	py::class_<Row, std::shared_ptr<Row>>(m, "Row")
		.def(py::init<std::vector<std::string>>(), py::arg("values"))
		.def("get_values", &Row::get_values)
		.def("get_is_deleted", &Row::get_is_deleted)
		.def("mark_deleted", &Row::mark_deleted);

	// 4. 绑定 StorageEngine 类
	py::class_<StorageEngine>(m, "StorageEngine")
		.def(py::init<>())
		// 基础存储接口
		.def("flush_all_dirty_pages", &StorageEngine::flush_all_dirty_pages)
		.def("has_index", &StorageEngine::has_index)
		.def("get_table_columns", &StorageEngine::get_table_columns)
		.def("get_index_size", &StorageEngine::get_index_size)
		// 复合索引接口
		.def("enable_composite_index", &StorageEngine::enable_composite_index, py::arg("table_name"), py::arg("indices"))
		.def("composite_index_range_row_values", &StorageEngine::composite_index_range_row_values, py::arg("table_name"), py::arg("min_key"), py::arg("max_key"))
		.def("drop_composite_index", &StorageEngine::drop_composite_index, py::arg("table_name"))
		.def("get_composite_index_columns", &StorageEngine::get_composite_index_columns, py::arg("table_name"))
		// MVCC helpers
		.def("mvcc_insert_uncommitted", &StorageEngine::mvcc_insert_uncommitted,
			py::arg("table_name"), py::arg("row_values"), py::arg("txid"), py::arg("pk_index"))
		.def("mvcc_commit_insert", &StorageEngine::mvcc_commit_insert,
			py::arg("table_name"), py::arg("pk"), py::arg("txid"))
		.def("mvcc_rollback_insert", &StorageEngine::mvcc_rollback_insert,
			py::arg("table_name"), py::arg("pk"), py::arg("txid"))
		.def("mvcc_mark_delete_commit", &StorageEngine::mvcc_mark_delete_commit,
			py::arg("table_name"), py::arg("pk"), py::arg("txid"))
		.def("mvcc_lookup_visible", &StorageEngine::mvcc_lookup_visible,
			py::arg("table_name"), py::arg("pk"), py::arg("reader_txid"), py::arg("active_txids"));

	// 模块属性：能力标识
	m.attr("_has_composite_persist") = py::bool_(true);

	// 5. 绑定 ExecutionEngine 类
	py::class_<ExecutionEngine>(m, "ExecutionEngine")
		.def(py::init<StorageEngine&>(), py::arg("storage"))
		// CreateTable
		.def("create_table", &ExecutionEngine::create_table, py::arg("table_name"), py::arg("columns"))
		// Insert
		.def("insert", &ExecutionEngine::insert, py::arg("table_name"), py::arg("row_values"))
		// SeqScan
		.def("seq_scan", &ExecutionEngine::seq_scan, py::arg("table_name"))
		// Filter
		.def("filter", &ExecutionEngine::filter, py::arg("table_name"), py::arg("predicate"))
		// Project
		.def("project", &ExecutionEngine::project, py::arg("table_name"), py::arg("input_rows"), py::arg("target_columns"))
		// Delete
		.def("delete_rows", &ExecutionEngine::delete_rows, py::arg("table_name"), py::arg("predicate"))
		// Index
		.def("index_scan", &ExecutionEngine::index_scan, py::arg("table_name"), py::arg("pk_value"))
		.def("index_range_scan", &ExecutionEngine::index_range_scan, py::arg("table_name"), py::arg("min_pk"), py::arg("max_pk"))
		.def("composite_index_range_scan", &ExecutionEngine::composite_index_range_scan, py::arg("table_name"), py::arg("min_key"), py::arg("max_key"))
		// Filter pushdown & batch insert
		.def("filter_conditions", &ExecutionEngine::filter_conditions, py::arg("table_name"), py::arg("conditions"))
		.def("insert_many", &ExecutionEngine::insert_many, py::arg("table_name"), py::arg("rows"))
		// UPDATE / JOIN / ORDER BY / GROUP BY / DROP TABLE
		.def("update_rows", &ExecutionEngine::update_rows, py::arg("table_name"), py::arg("set_clauses"), py::arg("where_predicate"))
		.def("inner_join", &ExecutionEngine::inner_join, py::arg("left_table"), py::arg("right_table"), py::arg("left_col"), py::arg("right_col"))
		.def("merge_join", &ExecutionEngine::merge_join, py::arg("left_table"), py::arg("right_table"), py::arg("left_col"), py::arg("right_col"))
		.def("order_by", &ExecutionEngine::order_by, py::arg("table_name"), py::arg("order_clauses"))
		.def("group_by", &ExecutionEngine::group_by, py::arg("table_name"), py::arg("group_columns"), py::arg("agg_functions"))
		.def("drop_table", &ExecutionEngine::drop_table, py::arg("table_name"));

	// 6. 绑定 GroupByResult
	py::class_<ExecutionEngine::GroupByResult>(m, "GroupByResult")
		.def_readonly("group_keys", &ExecutionEngine::GroupByResult::group_keys)
		.def_readonly("aggregates", &ExecutionEngine::GroupByResult::aggregates);
}
