#include "db_core.h"

// pybind11绑定：将C++类/函数暴露给Python
PYBIND11_MODULE(db_core, m) {
    m.doc() = "Simplified Database Core (C++ Backend)";

    // 1. 绑定DataType枚举
    py::enum_<DataType>(m, "DataType")
        .value("INT", DataType::INT)
        .value("STRING", DataType::STRING)
        .value("DOUBLE", DataType::DOUBLE)
        .export_values();

    // 2. 绑定Column结构体
    py::class_<Column>(m, "Column")
        .def(py::init<std::string, DataType, bool>(), 
             py::arg("name"), py::arg("type"), py::arg("is_primary_key"))
        .def_readonly("name", &Column::name)
        .def_readonly("type", &Column::type)
        .def_readonly("is_primary_key", &Column::is_primary_key);

    // 3. 绑定Row类（仅暴露Getter，避免Python直接修改）
    py::class_<Row, std::shared_ptr<Row>>(m, "Row")
        .def(py::init<std::vector<std::string>>(), py::arg("values"))
        .def("get_values", &Row::get_values)
        .def("get_is_deleted", &Row::get_is_deleted)
        .def("mark_deleted", &Row::mark_deleted);

    // 4. 绑定StorageEngine类
    py::class_<StorageEngine>(m, "StorageEngine")
        .def(py::init<>())
        .def("flush_all_dirty_pages", &StorageEngine::flush_all_dirty_pages)
        .def("has_index", &StorageEngine::has_index)
        .def("get_table_columns", &StorageEngine::get_table_columns)
        .def("get_index_size", &StorageEngine::get_index_size);

    // 5. 绑定ExecutionEngine类（核心接口）
    py::class_<ExecutionEngine>(m, "ExecutionEngine")
        .def(py::init<StorageEngine&>(), py::arg("storage"))
        // CreateTable：接收表名+Column列表
        .def("create_table", &ExecutionEngine::create_table, 
             py::arg("table_name"), py::arg("columns"))
        // Insert：接收表名+行数据（字符串列表）
        .def("insert", &ExecutionEngine::insert, 
             py::arg("table_name"), py::arg("row_values"))
        // SeqScan：返回Row列表
        .def("seq_scan", &ExecutionEngine::seq_scan, py::arg("table_name"))
        // Filter：接收Python函数作为过滤条件
        .def("filter", &ExecutionEngine::filter, 
             py::arg("table_name"), py::arg("predicate"))
        // Project：接收Row列表+目标列名列表，返回投影后的数据
        .def("project", &ExecutionEngine::project, 
             py::arg("table_name"), py::arg("input_rows"), py::arg("target_columns"))
        // Delete：返回删除行数
        .def("delete_rows", &ExecutionEngine::delete_rows, 
             py::arg("table_name"), py::arg("predicate"))
        // Index 接口（简化版）
        .def("index_scan", &ExecutionEngine::index_scan, 
             py::arg("table_name"), py::arg("pk_value"))
        .def("index_range_scan", &ExecutionEngine::index_range_scan, 
             py::arg("table_name"), py::arg("min_pk"), py::arg("max_pk"))
        // 额外接口：下推过滤与批量插入（合并到同一个定义链，避免重复定义类）
        .def("filter_conditions", &ExecutionEngine::filter_conditions,
             py::arg("table_name"), py::arg("conditions"))
        .def("insert_many", &ExecutionEngine::insert_many,
             py::arg("table_name"), py::arg("rows"));
}
