#ifndef DB_CORE_H
#define DB_CORE_H

#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <pybind11/functional.h>
#include <vector>
#include <map>
#include <string>
#include <cstring>
#include <fstream>
#include <memory>
#include <functional>
#include <optional>
#include <iostream>
#include <tuple>

namespace py = pybind11;

constexpr size_t PAGE_SIZE = 4096;  // 4KB页大小（磁盘IO最优单位）

// 1. 数据类型枚举（对应SQL类型）
enum class DataType {
    INT,
    STRING,
    DOUBLE
};

// 2. 列结构（元数据）
struct Column {
    std::string name;       // 列名
    DataType type;          // 列类型
    bool is_primary_key;    // 是否主键（用于后续扩展索引）

    Column(std::string n, DataType t, bool pk) 
        : name(std::move(n)), type(t), is_primary_key(pk) {}
};

// 3. 表结构（元数据）
struct TableSchema {
    std::string name;               // 表名
    std::vector<Column> columns;    // 列列表
    size_t column_count;            // 列数（缓存，避免重复计算）

    // 默认构造函数
    TableSchema() : name(""), column_count(0) {}
    
    TableSchema(std::string n, std::vector<Column> cols)
        : name(std::move(n)), columns(std::move(cols)), 
          column_count(this->columns.size()) {}
};

// 4. 数据行（Row）：支持序列化/反序列化
class Row {
private:
    std::vector<std::string> values;  // 行数据（统一转字符串存储，简化类型处理）
    bool is_deleted = false;          // 逻辑删除标记（避免物理删除碎片）

public:
    // 构造函数：接收Python传入的字符串列表（如["1", "Alice", "20.5"]）
    Row(std::vector<std::string> vals) : values(std::move(vals)) {}

    // 序列化：转为二进制（用于写入页）
    std::vector<char> serialize() const {
        std::vector<char> data;
        // 1. 写入删除标记（1字节）
        data.push_back(is_deleted ? 1 : 0);
        // 2. 写入字段数量（4字节，size_t）
        size_t val_count = values.size();
        data.insert(data.end(), reinterpret_cast<const char*>(&val_count), 
                   reinterpret_cast<const char*>(&val_count) + sizeof(val_count));
        // 3. 写入每个字段（长度+内容）
        for (const auto& val : values) {
            size_t val_len = val.size();
            // 字段长度（4字节）
            data.insert(data.end(), reinterpret_cast<const char*>(&val_len), 
                       reinterpret_cast<const char*>(&val_len) + sizeof(val_len));
            // 字段内容
            data.insert(data.end(), val.begin(), val.end());
        }
        return data;
    }

    // 反序列化：从二进制恢复Row
    static std::shared_ptr<Row> deserialize(const std::vector<char>& data) {
        size_t pos = 0;
        // 1. 读取删除标记
        bool deleted = data[pos++] == 1;
        // 2. 读取字段数量
        size_t val_count;
        std::memcpy(&val_count, &data[pos], sizeof(val_count));
        pos += sizeof(val_count);
        // 3. 读取每个字段
        std::vector<std::string> vals;
        for (size_t i = 0; i < val_count; ++i) {
            size_t val_len;
            std::memcpy(&val_len, &data[pos], sizeof(val_len));
            pos += sizeof(val_len);
            vals.emplace_back(data.begin() + pos, data.begin() + pos + val_len);
            pos += val_len;
        }
        // 4. 恢复删除状态
        auto row = std::make_shared<Row>(vals);
        row->is_deleted = deleted;
        return row;
    }

    // Getter：供Python访问行数据
    const std::vector<std::string>& get_values() const { return values; }
    bool get_is_deleted() const { return is_deleted; }
    void mark_deleted() { is_deleted = true; }
};

// 5. 数据页（Page）：管理Row存储与IO
class Page {
private:
    size_t page_id;          // 页ID（唯一标识）
    std::vector<char> data;  // 页数据缓冲区（4KB）
    bool is_dirty;           // 脏页标记（是否需写入磁盘）

public:
    Page(size_t id) : page_id(id), is_dirty(false) {
        data.resize(PAGE_SIZE, 0);  // 初始化4KB空数据
    }

    // 插入Row：返回是否成功（空间不足则失败）
    bool insert_row(const Row& row) {
        auto row_bin = row.serialize();
        size_t row_len = row_bin.size() + sizeof(size_t);  // 额外存Row长度（4字节）
        
        // 查找页内空闲空间（简化实现：从头部遍历，跳过已用空间）
        size_t pos = 0;
        while (pos + sizeof(size_t) <= PAGE_SIZE) {
            size_t existing_row_len;
            std::memcpy(&existing_row_len, &data[pos], sizeof(existing_row_len));
            if (existing_row_len == 0) break;  // 找到空闲位置
            pos += sizeof(size_t) + existing_row_len;  // 跳过已用Row
        }

        // 检查剩余空间是否足够
        if (pos + row_len > PAGE_SIZE) return false;

        // 写入Row（先写长度，再写二进制数据）
        std::memcpy(&data[pos], &row_len, sizeof(row_len));
        pos += sizeof(row_len);
        std::memcpy(&data[pos], row_bin.data(), row_bin.size());
        
        is_dirty = true;
        return true;
    }

    // 读取页内所有有效Row（过滤已删除行）
    std::vector<std::shared_ptr<Row>> get_rows() const {
        std::vector<std::shared_ptr<Row>> rows;
        size_t pos = 0;

        while (pos + sizeof(size_t) <= PAGE_SIZE) {
            size_t row_len;
            std::memcpy(&row_len, &data[pos], sizeof(row_len));
            if (row_len == 0) break;  // 无更多Row

            // 读取Row二进制数据
            pos += sizeof(row_len);
            std::vector<char> row_bin(&data[pos], &data[pos] + row_len);
            pos += row_len;

            // 反序列化并过滤已删除行
            auto row = Row::deserialize(row_bin);
            if (!row->get_is_deleted()) {
                rows.push_back(row);
            }
        }
        return rows;
    }

    // 页IO：写入磁盘/从磁盘加载（按表名+页ID命名文件，如"student_1.page"）
    bool write_to_disk(const std::string& table_name) {
        if (!is_dirty) return true;  // 非脏页无需写入
        std::string file_path = table_name + "_page_" + std::to_string(page_id) + ".bin";
        std::ofstream file(file_path, std::ios::binary);
        if (!file.is_open()) return false;
        file.write(data.data(), PAGE_SIZE);
        is_dirty = false;
        return true;
    }

    bool load_from_disk(const std::string& table_name) {
        std::string file_path = table_name + "_page_" + std::to_string(page_id) + ".bin";
        std::ifstream file(file_path, std::ios::binary);
        if (!file.is_open()) return false;
        file.read(data.data(), PAGE_SIZE);
        is_dirty = false;
        return true;
    }

    // Getter/Setter：供存储引擎调用
    size_t get_page_id() const { return page_id; }
    bool is_dirty_page() const { return is_dirty; }
    void set_dirty(bool dirty) { is_dirty = dirty; }
};

// 6. 系统目录：管理元数据（特殊表"sys_catalog"存储）
class SystemCatalog {
private:
    std::map<std::string, TableSchema> schema_cache;  // 内存缓存：表名→表结构
    std::unique_ptr<Page> catalog_page;               // 元数据页（持久化到磁盘）

public:
    SystemCatalog() {
        // 初始化元数据页（固定页ID=0，文件"sys_catalog_page_0.bin"）
        catalog_page = std::make_unique<Page>(0);
        // 从磁盘加载元数据（若文件存在）
        catalog_page->load_from_disk("sys_catalog");
        // 反序列化元数据到内存缓存（简化：假设元数据仅1页）
        auto rows = catalog_page->get_rows();
        for (const auto& row : rows) {
            auto vals = row->get_values();
            if (vals.size() < 3) continue;  // 异常数据跳过（表名+列数+列信息）
            
            // 解析表结构（vals格式：[表名, 列数, 列1名:类型:是否主键, 列2名:类型:是否主键, ...]）
            std::string table_name = vals[0];
            size_t col_count = std::stoul(vals[1]);
            std::vector<Column> columns;
            
            for (size_t i = 0; i < col_count; ++i) {
                std::string col_info = vals[2 + i];
                size_t sep1 = col_info.find(':');
                size_t sep2 = col_info.find(':', sep1 + 1);
                if (sep1 == std::string::npos || sep2 == std::string::npos) continue;
                
                std::string col_name = col_info.substr(0, sep1);
                std::string col_type_str = col_info.substr(sep1 + 1, sep2 - sep1 - 1);
                bool is_pk = (col_info.substr(sep2 + 1) == "1");
                
                DataType col_type = DataType::INT;
                if (col_type_str == "STRING") col_type = DataType::STRING;
                else if (col_type_str == "DOUBLE") col_type = DataType::DOUBLE;
                
                columns.emplace_back(col_name, col_type, is_pk);
            }
            
            schema_cache[table_name] = TableSchema(table_name, columns);
        }
    }

    // 注册新表元数据（CREATE TABLE时调用）
    bool register_table(const TableSchema& schema) {
        if (schema_cache.count(schema.name)) return false;  // 表已存在
        
        // 1. 缓存到内存
        schema_cache[schema.name] = schema;
        
        // 2. 序列化到元数据页（持久化）
        std::vector<std::string> catalog_row_vals;
        catalog_row_vals.push_back(schema.name);
        catalog_row_vals.push_back(std::to_string(schema.column_count));
        
        // 列信息格式："列名:类型:是否主键"（类型用字符串标识）
        for (const auto& col : schema.columns) {
            std::string col_type_str = (col.type == DataType::INT) ? "INT" 
                                     : (col.type == DataType::STRING) ? "STRING" : "DOUBLE";
            std::string col_info = col.name + ":" + col_type_str + ":" + (col.is_primary_key ? "1" : "0");
            catalog_row_vals.push_back(col_info);
        }
        
        Row catalog_row(catalog_row_vals);
        if (!catalog_page->insert_row(catalog_row)) return false;  // 元数据页空间不足
        catalog_page->write_to_disk("sys_catalog");  // 立即持久化
        return true;
    }

    // 查询表结构（SELECT/INSERT时校验表是否存在）
    std::optional<TableSchema> get_table_schema(const std::string& table_name) {
        if (schema_cache.count(table_name)) {
            return schema_cache[table_name];
        }
        return std::nullopt;  // 表不存在
    }

    // 检查列是否存在（Filter/Project算子用）
    bool column_exists(const std::string& table_name, const std::string& col_name) {
        auto schema_opt = get_table_schema(table_name);
        if (!schema_opt) return false;
        for (const auto& col : schema_opt->columns) {
            if (col.name == col_name) return true;
        }
        return false;
    }

    // 获取列索引（用于Row.values的下标映射）
    std::optional<size_t> get_column_index(const std::string& table_name, const std::string& col_name) {
        auto schema_opt = get_table_schema(table_name);
        if (!schema_opt) return std::nullopt;
        for (size_t i = 0; i < schema_opt->columns.size(); ++i) {
            if (schema_opt->columns[i].name == col_name) {
                return i;
            }
        }
        return std::nullopt;
    }
    
    // 获取所有表名（供StorageEngine使用）
    std::vector<std::string> get_table_names() const {
        std::vector<std::string> names;
        for (const auto& pair : schema_cache) {
            names.push_back(pair.first);
        }
        return names;
    }
};

// 7. 存储引擎：管理页缓存、Row-Page映射、磁盘IO
class StorageEngine {
private:
    SystemCatalog catalog;  // 系统目录（元数据管理）
    // 页缓存：(表名, 页ID) → Page对象（避免重复加载磁盘页）
    std::map<std::pair<std::string, size_t>, std::unique_ptr<Page>> page_cache;
    // 表的最大页ID：表名 → 最大页ID（用于创建新页）
    std::map<std::string, size_t> table_max_page_id;

    // 内存主键索引（简化B+树行为）：表名 → { pk列下标, 有效标志, 有序映射pk→整行值 }
    struct TableIndex {
        bool enabled = false;
        size_t pk_index = 0;
        std::map<std::string, std::vector<std::string>> pk_to_row_values;
    };
    std::map<std::string, TableIndex> primary_indexes;

public:
    StorageEngine() {
        // 初始化表的最大页ID（从磁盘读取已存在的页）
        auto table_names = catalog.get_table_names();
        for (const auto& table_name : table_names) {
            size_t max_id = 0;
            while (true) {
                std::string page_path = table_name + "_page_" + std::to_string(max_id + 1) + ".bin";
                if (std::ifstream(page_path).good()) {
                    max_id++;
                } else {
                    break;
                }
            }
            table_max_page_id[table_name] = max_id;
        }

        // 为已存在的表初始化主键索引（内存）
        for (const auto& table_name : table_names) {
            auto schema_opt = catalog.get_table_schema(table_name);
            if (schema_opt) {
                init_primary_index(*schema_opt);
            }
        }
    }

    // 获取页（缓存优先，无则从磁盘加载，再无则创建新页）
    std::shared_ptr<Page> get_page(const std::string& table_name, size_t page_id) {
        auto key = std::make_pair(table_name, page_id);
        // 1. 检查缓存
        if (page_cache.count(key)) {
            return std::shared_ptr<Page>(page_cache[key].get(), [](Page*){}); // 非拥有型shared_ptr，避免与unique_ptr重复释放
        }
        // 2. 从磁盘加载
        auto page = std::make_unique<Page>(page_id);
        if (page->load_from_disk(table_name)) {
            page_cache[key] = std::move(page);
            return std::shared_ptr<Page>(page_cache[key].get(), [](Page*){});
        }
        // 3. 磁盘无此页（仅允许创建新页，不允许加载不存在的页）
        return nullptr;
    }

    // 创建新页（Insert时无空闲页可用）
    std::shared_ptr<Page> create_new_page(const std::string& table_name) {
        size_t new_page_id = table_max_page_id[table_name] + 1;
        auto page = std::make_unique<Page>(new_page_id);
        auto key = std::make_pair(table_name, new_page_id);
        page_cache[key] = std::move(page);
        table_max_page_id[table_name] = new_page_id;
        return std::shared_ptr<Page>(page_cache[key].get(), [](Page*){});
    }

    // 写入页到磁盘（脏页刷盘）
    bool write_page(const std::string& table_name, const std::shared_ptr<Page>& page) {
        if (!page) return false;
        return page->write_to_disk(table_name);
    }

    // 刷盘所有脏页（系统退出前调用）
    void flush_all_dirty_pages() {
        for (const auto& pair : page_cache) {
            const auto& key = pair.first;
            const auto& page = pair.second;
            if (page->is_dirty_page()) {
                page->write_to_disk(key.first);
            }
        }
    }

    // 系统目录接口（暴露给执行引擎）
    SystemCatalog& get_catalog() { return catalog; }
    
    size_t get_table_max_page_id(const std::string& table_name) {
        if (table_max_page_id.count(table_name)) {
            return table_max_page_id[table_name];
        }
        
        // 动态发现页文件
        size_t max_id = 0;
        while (true) {
            std::string page_path = table_name + "_page_" + std::to_string(max_id + 1) + ".bin";
            if (std::ifstream(page_path).good()) {
                max_id++;
            } else {
                break;
            }
        }
        table_max_page_id[table_name] = max_id;
        return max_id;
    }

    // 简化索引存在性判断：如果存在主键列，则认为有聚簇索引
    bool has_index(const std::string& table_name) {
        auto schema_opt = catalog.get_table_schema(table_name);
        if (!schema_opt) return false;
        for (const auto& col : schema_opt->columns) {
            if (col.is_primary_key) return true;
        }
        return false;
    }

    // 暴露列名列表，便于 Python 侧加载表结构
    std::vector<std::string> get_table_columns(const std::string& table_name) {
        std::vector<std::string> cols;
        auto schema_opt = catalog.get_table_schema(table_name);
        if (!schema_opt) return cols;
        for (const auto& col : schema_opt->columns) {
            cols.push_back(col.name);
        }
        return cols;
    }

    // 调试：返回主键索引当前条目数量
    size_t get_index_size(const std::string& table_name) const {
        auto it = primary_indexes.find(table_name);
        if (it == primary_indexes.end() || !it->second.enabled) return 0;
        return it->second.pk_to_row_values.size();
    }

    // 初始化主键索引（根据表结构决定是否启用）
    void init_primary_index(const TableSchema& schema) {
        TableIndex idx;
        idx.enabled = false;
        idx.pk_index = 0;
        for (size_t i = 0; i < schema.columns.size(); ++i) {
            if (schema.columns[i].is_primary_key) {
                idx.enabled = true;
                idx.pk_index = i;
                break;
            }
        }
        primary_indexes[schema.name] = std::move(idx);
        if (primary_indexes[schema.name].enabled) {
            std::cout << "[CPP] init_primary_index enabled table=" << schema.name
                      << " pk_idx=" << primary_indexes[schema.name].pk_index << std::endl;
        } else {
            std::cout << "[CPP] init_primary_index disabled table=" << schema.name << std::endl;
        }
    }

    // 向主键索引写入一行（插入后调用）
    void insert_index_row(const std::string& table_name, const std::vector<std::string>& row_values) {
        auto it = primary_indexes.find(table_name);
        if (it == primary_indexes.end()) return;
        auto& idx = it->second;
        if (!idx.enabled) return;
        if (idx.pk_index >= row_values.size()) return;
        const std::string& key = row_values[idx.pk_index];
        idx.pk_to_row_values[key] = row_values;  // 覆盖式插入（主键唯一）
        std::cout << "[CPP] index_insert table=" << table_name
                  << " key=" << key << " size=" << idx.pk_to_row_values.size() << std::endl;
    }

    // 点查：返回行值（若存在）
    std::optional<std::vector<std::string>> index_get_row_values(const std::string& table_name, const std::string& key) {
        auto it = primary_indexes.find(table_name);
        if (it == primary_indexes.end() || !it->second.enabled) return std::nullopt;
        auto mit = it->second.pk_to_row_values.find(key);
        std::cout << "[CPP] index_get table=" << table_name << " key=" << key
                  << " found=" << (mit != it->second.pk_to_row_values.end()) << std::endl;
        if (mit == it->second.pk_to_row_values.end()) return std::nullopt;
        return mit->second;
    }

    // 范围查：返回[min_key, max_key] 闭区间内的所有行值
    std::vector<std::vector<std::string>> index_range_row_values(
        const std::string& table_name,
        const std::string& min_key,
        const std::string& max_key
    ) {
        std::vector<std::vector<std::string>> out;
        auto it = primary_indexes.find(table_name);
        if (it == primary_indexes.end() || !it->second.enabled) return out;
        auto& m = it->second.pk_to_row_values;
        for (auto iter = m.lower_bound(min_key); iter != m.end() && iter->first <= max_key; ++iter) {
            out.push_back(iter->second);
        }
        std::cout << "[CPP] index_range table=" << table_name << " min=" << min_key
                  << " max=" << max_key << " count=" << out.size() << std::endl;
        return out;
    }
};

// 8. 执行引擎：实现核心算子（CreateTable/Insert/SeqScan/Filter/Project）
class ExecutionEngine {
private:
    StorageEngine& storage;  // 依赖存储引擎

public:
    ExecutionEngine(StorageEngine& s) : storage(s) {}

    // 1. CreateTable算子：创建表结构并注册元数据
    bool create_table(const std::string& table_name, const std::vector<Column>& columns) {
        // 校验：表名非空、列非空
        if (table_name.empty() || columns.empty()) return false;
        // 注册到系统目录
        TableSchema schema(table_name, columns);
        bool ok = storage.get_catalog().register_table(schema);
        if (ok) {
            storage.init_primary_index(schema);
        }
        return ok;
    }

    // 2. Insert算子：将Row写入存储引擎
    bool insert(const std::string& table_name, const std::vector<std::string>& row_values) {
        // 校验：表存在、列数匹配
        auto schema_opt = storage.get_catalog().get_table_schema(table_name);
        if (!schema_opt || row_values.size() != schema_opt->column_count) return false;
        
        // 1. 创建Row对象
        Row row(row_values);
        
        // 2. 尝试写入已有页（从最后一页开始，减少遍历）
        size_t max_page_id = storage.get_table_max_page_id(table_name);
        for (size_t page_id = max_page_id; page_id >= 1; page_id--) {
            auto page = storage.get_page(table_name, page_id);
            if (page && page->insert_row(row)) {
                storage.write_page(table_name, page);
                // 维护主键索引（内存）
                storage.insert_index_row(table_name, row_values);
                return true;
            }
            if (page_id == 1) break; // 防止 size_t 下溢
        }
        
        // 3. 已有页无空间，创建新页写入
        auto new_page = storage.create_new_page(table_name);
        if (new_page->insert_row(row)) {
            storage.write_page(table_name, new_page);
            // 维护主键索引（内存）
            storage.insert_index_row(table_name, row_values);
            return true;
        }
        
        return false;  // Row太大（超过4KB），插入失败
    }

    // 3. SeqScan算子：全表扫描（读取所有页的Row）
    std::vector<std::shared_ptr<Row>> seq_scan(const std::string& table_name) {
        std::vector<std::shared_ptr<Row>> all_rows;
        // 校验：表存在
        auto schema_opt = storage.get_catalog().get_table_schema(table_name);
        if (!schema_opt) return all_rows;
        
        // 读取所有页的Row
        size_t max_page_id = storage.get_table_max_page_id(table_name);
        for (size_t page_id = 1; page_id <= max_page_id; page_id++) {
            auto page = storage.get_page(table_name, page_id);
            if (!page) continue;
            auto page_rows = page->get_rows();
            all_rows.insert(all_rows.end(), page_rows.begin(), page_rows.end());
        }
        
        return all_rows;
    }

    // 4. Filter算子：按条件过滤Row（接收Python传入的过滤函数）
    std::vector<std::shared_ptr<Row>> filter(
        const std::string& table_name,
        const std::function<bool(const std::vector<std::string>&)>& predicate
    ) {
        // 1. 先全表扫描
        auto all_rows = seq_scan(table_name);
        // 2. 应用过滤条件
        std::vector<std::shared_ptr<Row>> filtered_rows;
        for (const auto& row : all_rows) {
            if (predicate(row->get_values())) {
                filtered_rows.push_back(row);
            }
        }
        return filtered_rows;
    }

    // 5. Project算子：选择指定列（按列名过滤Row values）
    std::vector<std::vector<std::string>> project(
        const std::string& table_name,
        const std::vector<std::shared_ptr<Row>>& input_rows,
        const std::vector<std::string>& target_columns
    ) {
        std::vector<std::vector<std::string>> projected_rows;
        // 校验：目标列存在
        std::vector<size_t> col_indices;
        for (const auto& col_name : target_columns) {
            auto col_idx_opt = storage.get_catalog().get_column_index(table_name, col_name);
            if (!col_idx_opt) return projected_rows;  // 列不存在，返回空
            col_indices.push_back(*col_idx_opt);
        }
        
        // 提取目标列数据
        for (const auto& row : input_rows) {
            std::vector<std::string> projected_vals;
            auto row_vals = row->get_values();
            for (size_t col_idx : col_indices) {
                projected_vals.push_back(row_vals[col_idx]);
            }
            projected_rows.push_back(projected_vals);
        }
        
        return projected_rows;
    }

    // 6. Delete算子：逻辑删除满足条件的Row
    size_t delete_rows(
        const std::string& table_name,
        const std::function<bool(const std::vector<std::string>&)>& predicate
    ) {
        size_t deleted_count = 0;
        // 校验：表存在
        auto schema_opt = storage.get_catalog().get_table_schema(table_name);
        if (!schema_opt) return 0;
        
        // 遍历所有页，标记符合条件的Row
        size_t max_page_id = storage.get_table_max_page_id(table_name);
        for (size_t page_id = 1; page_id <= max_page_id; page_id++) {
            auto page = storage.get_page(table_name, page_id);
            if (!page) continue;
            
            auto page_rows = page->get_rows();
            for (const auto& row : page_rows) {
                if (predicate(row->get_values()) && !row->get_is_deleted()) {
                    row->mark_deleted();
                    page->set_dirty(true);
                    deleted_count++;
                }
            }
            
            // 刷盘脏页
            storage.write_page(table_name, page);
        }
        
        return deleted_count;
    }

    // 7. IndexScan（点查询，基于主键，简化版：回退为过滤）
    std::shared_ptr<Row> index_scan(const std::string& table_name, const std::string& pk_value) {
        auto vals_opt = storage.index_get_row_values(table_name, pk_value);
        if (!vals_opt) return nullptr;
        return std::make_shared<Row>(*vals_opt);
    }

    // 8. IndexRangeScan（范围查询，基于主键，简化版：线性过滤）
    std::vector<std::shared_ptr<Row>> index_range_scan(
        const std::string& table_name,
        const std::string& min_pk,
        const std::string& max_pk
    ) {
        std::vector<std::shared_ptr<Row>> results;
        auto vecs = storage.index_range_row_values(table_name, min_pk, max_pk);
        for (auto& vals : vecs) {
            results.push_back(std::make_shared<Row>(vals));
        }
        return results;
    }

    // 9. Filter（条件下推，避免 Python 回调开销）
    // conditions: 各元素为 (列索引, 操作符, 比较值)，AND 连接
    std::vector<std::shared_ptr<Row>> filter_conditions(
        const std::string& table_name,
        const std::vector<std::tuple<int, std::string, std::string>>& conditions
    ) {
        auto all_rows = seq_scan(table_name);
        if (conditions.empty()) return all_rows;

        auto eval_cond = [](const std::string& lhs, const std::string& op, const std::string& rhs) -> bool {
            auto to_num = [](const std::string& s, bool& ok) -> double {
                try { ok = true; return std::stod(s); } catch (...) { ok = false; return 0.0; }
            };
            bool l_ok=false, r_ok=false; double lnum=to_num(lhs,l_ok), rnum=to_num(rhs,r_ok);
            if (l_ok && r_ok) {
                if (op == "=") return lnum == rnum;
                if (op == ">") return lnum > rnum;
                if (op == "<") return lnum < rnum;
                if (op == ">=") return lnum >= rnum;
                if (op == "<=") return lnum <= rnum;
                if (op == "!=") return lnum != rnum;
                return false;
            }
            if (op == "=") return lhs == rhs;
            if (op == ">") return lhs > rhs;
            if (op == "<") return lhs < rhs;
            if (op == ">=") return lhs >= rhs;
            if (op == "<=") return lhs <= rhs;
            if (op == "!=") return lhs != rhs;
            return false;
        };

        std::vector<std::shared_ptr<Row>> out;
        out.reserve(all_rows.size());
        for (const auto& row : all_rows) {
            const auto& vals = row->get_values();
            bool ok = true;
            for (const auto& cond : conditions) {
                int idx; std::string op; std::string rhs;
                std::tie(idx, op, rhs) = cond;
                if (idx < 0 || static_cast<size_t>(idx) >= vals.size()) { ok = false; break; }
                if (!eval_cond(vals[static_cast<size_t>(idx)], op, rhs)) { ok = false; break; }
            }
            if (ok) out.push_back(row);
        }
        return out;
    }

    // 10. 批量插入（简单循环封装）
    size_t insert_many(const std::string& table_name, const std::vector<std::vector<std::string>>& rows) {
        size_t ok_count = 0;
        for (const auto& r : rows) {
            if (insert(table_name, r)) ok_count++;
        }
        return ok_count;
    }
};

#endif // DB_CORE_H
