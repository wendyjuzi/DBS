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

    // 删除表元数据（DROP TABLE时调用）
    bool unregister_table(const std::string& table_name) {
        if (!schema_cache.count(table_name)) return false;  // 表不存在
        
        // 1. 从内存缓存中删除
        schema_cache.erase(table_name);
        
        // 2. 从元数据页中删除（重新构建元数据页）
        // 简化实现：清空元数据页，重新写入剩余表的元数据
        catalog_page = std::make_unique<Page>(0);
        
        // 重新序列化所有剩余表的元数据
        for (const auto& pair : schema_cache) {
            const TableSchema& schema = pair.second;
            std::vector<std::string> catalog_row_vals;
            catalog_row_vals.push_back(schema.name);
            catalog_row_vals.push_back(std::to_string(schema.column_count));
            
            // 列信息格式："列名:类型:是否主键"
            for (const auto& col : schema.columns) {
                std::string col_type_str = (col.type == DataType::INT) ? "INT" 
                                         : (col.type == DataType::STRING) ? "STRING" : "DOUBLE";
                std::string col_info = col.name + ":" + col_type_str + ":" + (col.is_primary_key ? "1" : "0");
                catalog_row_vals.push_back(col_info);
            }
            
            Row catalog_row(catalog_row_vals);
            if (!catalog_page->insert_row(catalog_row)) {
                std::cout << "[CPP] 警告：元数据页空间不足，无法重新写入表 " << schema.name << std::endl;
                return false;
            }
        }
        
        // 3. 持久化到磁盘
        catalog_page->write_to_disk("sys_catalog");
        std::cout << "[CPP] 表元数据删除成功: " << table_name << std::endl;
        return true;
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

    // 复合索引（内存版，雏形）：表名 → { 启用标志, 参与列下标序列, 有序映射compositeKey→整行值 }
    struct CompositeIndexInfo {
        bool enabled = false;
        std::vector<size_t> key_indices;  // 参与复合键的列下标，按顺序
        std::map<std::string, std::vector<std::string>> key_to_row_values;
        // 简易持久化文件路径
        std::string meta_path;  // table_cidx.meta
        std::string data_path;  // table_cidx.bin
        std::string wal_path;   // table_cidx.wal
    };
    std::map<std::string, CompositeIndexInfo> composite_indexes;

    // MVCC version chains: (table, pk) -> head pointer
    struct VersionNode {
        std::vector<std::string> values;
        std::string xmin;
        std::optional<std::string> xmax; // None means live
        bool committed;
        VersionNode* next;
        VersionNode(const std::vector<std::string>& v, std::string tx, bool c)
            : values(v), xmin(std::move(tx)), xmax(std::nullopt), committed(c), next(nullptr) {}
    };
    std::map<std::pair<std::string, std::string>, VersionNode*> mvcc_heads;

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
            // 加载复合索引（如果存在）
            try { load_composite_index_if_exists(table_name); } catch (...) {}
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
        // 默认复合索引未启用
        composite_indexes.erase(schema.name);
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

        // 同步维护复合索引（若存在）
        auto cit = composite_indexes.find(table_name);
        if (cit != composite_indexes.end() && cit->second.enabled) {
            const auto& indices = cit->second.key_indices;
            if (!indices.empty()) {
                std::string ckey;
                for (size_t i = 0; i < indices.size(); ++i) {
                    size_t col_idx = indices[i];
                    if (col_idx >= row_values.size()) { ckey.clear(); break; }
                    if (i > 0) ckey.push_back('\x1F'); // 使用不可见分隔符
                    ckey += row_values[col_idx];
                }
                if (!ckey.empty()) {
                    cit->second.key_to_row_values[ckey] = row_values;
                    // 追加WAL
                    append_cidx_wal(table_name, ckey, row_values);
                }
            }
        }
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

    // 启用复合索引（内存版，雏形）。indices 为参与列下标序列。
    bool enable_composite_index(const std::string& table_name, const std::vector<size_t>& indices) {
        if (indices.empty()) return false;
        CompositeIndexInfo info;
        info.enabled = true;
        info.key_indices = indices;
        info.meta_path = table_name + "_cidx.meta";
        info.data_path = table_name + "_cidx.bin";
        info.wal_path  = table_name + "_cidx.wal";
        composite_indexes[table_name] = std::move(info);
        // 回填历史数据并落盘快照
        rebuild_and_save_composite_index(table_name);
        std::cout << "[CPP] composite index enabled table=" << table_name << " cols=" << indices.size() << std::endl;
        return true;
    }

    // 复合索引点查
    std::optional<std::vector<std::string>> composite_index_get_row_values(const std::string& table_name, const std::string& composite_key) {
        auto it = composite_indexes.find(table_name);
        if (it == composite_indexes.end() || !it->second.enabled) return std::nullopt;
        auto mit = it->second.key_to_row_values.find(composite_key);
        if (mit == it->second.key_to_row_values.end()) return std::nullopt;
        return mit->second;
    }

    // 复合索引范围查（字典序）
    std::vector<std::vector<std::string>> composite_index_range_row_values(
        const std::string& table_name,
        const std::string& min_key,
        const std::string& max_key
    ) {
        std::vector<std::vector<std::string>> out;
        auto it = composite_indexes.find(table_name);
        if (it == composite_indexes.end() || !it->second.enabled) return out;
        auto& m = it->second.key_to_row_values;
        for (auto iter = m.lower_bound(min_key); iter != m.end() && iter->first <= max_key; ++iter) {
            out.push_back(iter->second);
        }
        return out;
    }

    // Drop复合索引：内存清理并删除持久化文件
    bool drop_composite_index(const std::string& table_name) {
        auto it = composite_indexes.find(table_name);
        if (it == composite_indexes.end()) return false;
        auto meta = it->second.meta_path; auto data = it->second.data_path; auto wal = it->second.wal_path;
        composite_indexes.erase(it);
        if (!meta.empty()) std::remove(meta.c_str());
        if (!data.empty()) std::remove(data.c_str());
        if (!wal.empty()) std::remove(wal.c_str());
        std::cout << "[CPP] composite index dropped table=" << table_name << std::endl;
        return true;
    }

    // 展示复合索引信息
    std::vector<size_t> get_composite_index_columns(const std::string& table_name) const {
        auto it = composite_indexes.find(table_name);
        if (it == composite_indexes.end() || !it->second.enabled) return {};
        return it->second.key_indices;
    }

private:
    void load_composite_index_if_exists(const std::string& table_name) {
        std::string meta = table_name + "_cidx.meta";
        if (!std::ifstream(meta).good()) return;
        CompositeIndexInfo info; info.enabled = true; info.meta_path = meta;
        info.data_path = table_name + "_cidx.bin"; info.wal_path = table_name + "_cidx.wal";
        // 读取列下标
        std::ifstream mf(meta);
        std::string line; if (std::getline(mf, line)) {
            info.key_indices.clear();
            size_t pos = 0; while (pos < line.size()) {
                size_t comma = line.find(',', pos);
                std::string tok = line.substr(pos, comma == std::string::npos ? std::string::npos : comma - pos);
                if (!tok.empty()) info.key_indices.push_back(static_cast<size_t>(std::stoul(tok)));
                if (comma == std::string::npos) break; pos = comma + 1;
            }
        }
        // 读取快照
        std::ifstream df(info.data_path, std::ios::binary);
        if (df.good()) {
            std::string k; size_t nvals = 0;
            while (true) {
                uint32_t klen = 0; if (!df.read(reinterpret_cast<char*>(&klen), sizeof(klen))) break;
                k.resize(klen); if (!df.read(&k[0], klen)) break;
                uint32_t cnt = 0; if (!df.read(reinterpret_cast<char*>(&cnt), sizeof(cnt))) break; nvals = cnt;
                std::vector<std::string> vals; vals.reserve(nvals);
                for (uint32_t i = 0; i < cnt; ++i) {
                    uint32_t sl = 0; if (!df.read(reinterpret_cast<char*>(&sl), sizeof(sl))) { vals.clear(); break; }
                    std::string s; s.resize(sl); if (!df.read(&s[0], sl)) { vals.clear(); break; }
                    vals.push_back(std::move(s));
                }
                if (!vals.empty()) info.key_to_row_values[k] = std::move(vals);
            }
        }
        // 回放WAL
        replay_cidx_wal(table_name, info);
        composite_indexes[table_name] = std::move(info);
    }

    void rebuild_and_save_composite_index(const std::string& table_name) {
        auto it = composite_indexes.find(table_name);
        if (it == composite_indexes.end()) return;
        auto& info = it->second;
        info.key_to_row_values.clear();
        // 以主键索引为基础回填
        auto pit = primary_indexes.find(table_name);
        if (pit != primary_indexes.end() && pit->second.enabled) {
            for (const auto& kv : pit->second.pk_to_row_values) {
                const auto& row_values = kv.second;
                std::string ckey;
                for (size_t i = 0; i < info.key_indices.size(); ++i) {
                    size_t col_idx = info.key_indices[i];
                    if (col_idx >= row_values.size()) { ckey.clear(); break; }
                    if (i > 0) ckey.push_back('\x1F');
                    ckey += row_values[col_idx];
                }
                if (!ckey.empty()) info.key_to_row_values[ckey] = row_values;
            }
        }
        save_cidx_snapshot(table_name, info);
        // 清空旧WAL
        if (!info.wal_path.empty()) std::remove(info.wal_path.c_str());
        // 保存meta
        std::ofstream mf(info.meta_path, std::ios::trunc);
        for (size_t i = 0; i < info.key_indices.size(); ++i) {
            if (i) mf << ",";
            mf << info.key_indices[i];
        }
    }

    void save_cidx_snapshot(const std::string& table_name, const CompositeIndexInfo& info) {
        std::ofstream df(info.data_path, std::ios::binary | std::ios::trunc);
        for (const auto& kv : info.key_to_row_values) {
            const std::string& k = kv.first; const auto& vals = kv.second;
            uint32_t klen = static_cast<uint32_t>(k.size());
            df.write(reinterpret_cast<const char*>(&klen), sizeof(klen));
            df.write(k.data(), k.size());
            uint32_t cnt = static_cast<uint32_t>(vals.size());
            df.write(reinterpret_cast<const char*>(&cnt), sizeof(cnt));
            for (const auto& s : vals) {
                uint32_t sl = static_cast<uint32_t>(s.size());
                df.write(reinterpret_cast<const char*>(&sl), sizeof(sl));
                df.write(s.data(), s.size());
            }
        }
    }

    void append_cidx_wal(const std::string& table_name, const std::string& key, const std::vector<std::string>& vals) {
        auto it = composite_indexes.find(table_name); if (it == composite_indexes.end()) return;
        const auto& info = it->second; if (info.wal_path.empty()) return;
        std::ofstream wf(info.wal_path, std::ios::binary | std::ios::app);
        uint32_t klen = static_cast<uint32_t>(key.size());
        wf.write(reinterpret_cast<const char*>(&klen), sizeof(klen));
        wf.write(key.data(), key.size());
        uint32_t cnt = static_cast<uint32_t>(vals.size());
        wf.write(reinterpret_cast<const char*>(&cnt), sizeof(cnt));
        for (const auto& s : vals) {
            uint32_t sl = static_cast<uint32_t>(s.size());
            wf.write(reinterpret_cast<const char*>(&sl), sizeof(sl));
            wf.write(s.data(), s.size());
        }
    }

    void replay_cidx_wal(const std::string& table_name, CompositeIndexInfo& info) {
        std::ifstream wf(info.wal_path, std::ios::binary);
        if (!wf.good()) return;
        while (true) {
            uint32_t klen = 0; if (!wf.read(reinterpret_cast<char*>(&klen), sizeof(klen))) break;
            std::string key; key.resize(klen); if (!wf.read(&key[0], klen)) break;
            uint32_t cnt = 0; if (!wf.read(reinterpret_cast<char*>(&cnt), sizeof(cnt))) break;
            std::vector<std::string> vals; vals.reserve(cnt);
            for (uint32_t i = 0; i < cnt; ++i) {
                uint32_t sl = 0; if (!wf.read(reinterpret_cast<char*>(&sl), sizeof(sl))) { vals.clear(); break; }
                std::string s; s.resize(sl); if (!wf.read(&s[0], sl)) { vals.clear(); break; }
                vals.push_back(std::move(s));
            }
            if (!vals.empty()) info.key_to_row_values[key] = std::move(vals);
        }
    }

public:
    // 删除表的所有数据文件（DROP TABLE时调用）
    bool drop_table_data(const std::string& table_name) {
        bool success = true;
        
        // 1. 清理页缓存
        auto it = page_cache.begin();
        while (it != page_cache.end()) {
            if (it->first.first == table_name) {
                // 先刷盘脏页
                if (it->second->is_dirty_page()) {
                    it->second->write_to_disk(table_name);
                }
                it = page_cache.erase(it);
            } else {
                ++it;
            }
        }
        
        // 2. 删除磁盘上的所有页文件
        size_t max_page_id = table_max_page_id[table_name];
        for (size_t page_id = 1; page_id <= max_page_id; page_id++) {
            std::string page_path = table_name + "_page_" + std::to_string(page_id) + ".bin";
            if (std::remove(page_path.c_str()) != 0) {
                std::cout << "[CPP] 警告：无法删除页文件 " << page_path << std::endl;
                success = false;
            } else {
                std::cout << "[CPP] 删除页文件: " << page_path << std::endl;
            }
        }
        
        // 3. 清理表的最大页ID记录
        table_max_page_id.erase(table_name);
        
        // 4. 清理主键索引
        primary_indexes.erase(table_name);
        
        std::cout << "[CPP] 表数据清理完成: " << table_name << std::endl;
        return success;
    }

    // --- MVCC helpers ---
    bool mvcc_insert_uncommitted(const std::string& table_name,
                                 const std::vector<std::string>& row_values,
                                 const std::string& txid,
                                 size_t pk_index) {
        if (pk_index >= row_values.size()) return false;
        const std::string& pk = row_values[pk_index];
        auto key = std::make_pair(table_name, pk);
        VersionNode* head = nullptr;
        auto it = mvcc_heads.find(key);
        if (it != mvcc_heads.end()) head = it->second;
        auto* node = new VersionNode(row_values, txid, false);
        node->next = head;
        mvcc_heads[key] = node;
        return true;
    }

    bool mvcc_commit_insert(const std::string& table_name,
                            const std::string& pk,
                            const std::string& txid) {
        auto it = mvcc_heads.find({table_name, pk});
        if (it == mvcc_heads.end() || it->second == nullptr) return false;
        VersionNode* head = it->second;
        if (head->xmin == txid && !head->committed) {
            head->committed = true;
            return true;
        }
        return false;
    }

    bool mvcc_rollback_insert(const std::string& table_name,
                               const std::string& pk,
                               const std::string& txid) {
        auto it = mvcc_heads.find({table_name, pk});
        if (it == mvcc_heads.end() || it->second == nullptr) return false;
        VersionNode* head = it->second;
        if (head->xmin == txid && !head->committed) {
            mvcc_heads[{table_name, pk}] = head->next;
            delete head;
            return true;
        }
        return false;
    }

    bool mvcc_mark_delete_commit(const std::string& table_name,
                                 const std::string& pk,
                                 const std::string& txid) {
        auto it = mvcc_heads.find({table_name, pk});
        if (it == mvcc_heads.end()) return false;
        VersionNode* cur = it->second;
        while (cur) {
            if (cur->committed && !cur->xmax.has_value()) {
                cur->xmax = txid;
                return true;
            }
            cur = cur->next;
        }
        return false;
    }

    std::optional<std::vector<std::string>> mvcc_lookup_visible(
        const std::string& table_name,
        const std::string& pk,
        const std::string& reader_txid,
        const std::vector<std::string>& active_txids) {
        auto it = mvcc_heads.find({table_name, pk});
        if (it == mvcc_heads.end()) return std::nullopt;
        VersionNode* cur = it->second;
        auto is_active = [&](const std::string& x){
            return std::find(active_txids.begin(), active_txids.end(), x) != active_txids.end();
        };
        while (cur) {
            if (!cur->committed) {
                if (cur->xmin == reader_txid) return cur->values;
            } else {
                if (!cur->xmax.has_value() && !is_active(cur->xmin)) {
                    return cur->values;
                }
            }
            cur = cur->next;
        }
        return std::nullopt;
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

    // 8b. CompositeIndexRangeScan（范围查询，基于复合键，内存雏形）
    std::vector<std::shared_ptr<Row>> composite_index_range_scan(
        const std::string& table_name,
        const std::string& min_key,
        const std::string& max_key
    ) {
        std::vector<std::shared_ptr<Row>> results;
        auto vecs = storage.composite_index_range_row_values(table_name, min_key, max_key);
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

    // 11. UPDATE算子：更新满足条件的行
    size_t update_rows(
        const std::string& table_name,
        const std::vector<std::pair<std::string, std::string>>& set_clauses,  // 列名->新值
        const std::function<bool(const std::vector<std::string>&)>& where_predicate
    ) {
        size_t updated_count = 0;
        auto schema_opt = storage.get_catalog().get_table_schema(table_name);
        if (!schema_opt) return 0;

        // 获取列索引映射
        std::map<std::string, size_t> col_name_to_idx;
        for (size_t i = 0; i < schema_opt->columns.size(); ++i) {
            col_name_to_idx[schema_opt->columns[i].name] = i;
        }

        // 遍历所有页，更新符合条件的行
        size_t max_page_id = storage.get_table_max_page_id(table_name);
        for (size_t page_id = 1; page_id <= max_page_id; page_id++) {
            auto page = storage.get_page(table_name, page_id);
            if (!page) continue;

            auto page_rows = page->get_rows();
            for (const auto& row : page_rows) {
                if (where_predicate(row->get_values()) && !row->get_is_deleted()) {
                    // 创建新行数据
                    auto new_values = row->get_values();
                    bool has_update = false;
                    
                    // 应用SET子句
                    for (const auto& set_clause : set_clauses) {
                        const std::string& col_name = set_clause.first;
                        const std::string& new_value = set_clause.second;
                        
                        auto it = col_name_to_idx.find(col_name);
                        if (it != col_name_to_idx.end()) {
                            new_values[it->second] = new_value;
                            has_update = true;
                        }
                    }
                    
                    if (has_update) {
                        // 删除旧行，插入新行
                        row->mark_deleted();
                        Row new_row(new_values);
                        if (page->insert_row(new_row)) {
                            // 更新主键索引
                            storage.insert_index_row(table_name, new_values);
                            updated_count++;
                        }
                        page->set_dirty(true);
                    }
                }
            }
            
            storage.write_page(table_name, page);
        }
        
        return updated_count;
    }

    // 12. JOIN算子：内连接两个表
    std::vector<std::vector<std::string>> inner_join(
        const std::string& left_table,
        const std::string& right_table,
        const std::string& left_col,
        const std::string& right_col
    ) {
        std::vector<std::vector<std::string>> result;
        
        // 获取表结构
        auto left_schema = storage.get_catalog().get_table_schema(left_table);
        auto right_schema = storage.get_catalog().get_table_schema(right_table);
        if (!left_schema || !right_schema) return result;

        // 获取连接列索引
        auto left_col_idx = storage.get_catalog().get_column_index(left_table, left_col);
        auto right_col_idx = storage.get_catalog().get_column_index(right_table, right_col);
        if (!left_col_idx || !right_col_idx) return result;

        // 扫描左表
        auto left_rows = seq_scan(left_table);
        
        // 为右表建立哈希索引（简化实现）
        std::map<std::string, std::vector<std::shared_ptr<Row>>> right_index;
        auto right_rows = seq_scan(right_table);
        for (const auto& right_row : right_rows) {
            const auto& vals = right_row->get_values();
            if (*right_col_idx < vals.size()) {
                right_index[vals[*right_col_idx]].push_back(right_row);
            }
        }

        // 执行连接
        for (const auto& left_row : left_rows) {
            const auto& left_vals = left_row->get_values();
            if (*left_col_idx < left_vals.size()) {
                const std::string& join_key = left_vals[*left_col_idx];
                auto it = right_index.find(join_key);
                if (it != right_index.end()) {
                    // 找到匹配，生成连接结果
                    for (const auto& right_row : it->second) {
                        std::vector<std::string> joined_row;
                        // 左表所有列
                        joined_row.insert(joined_row.end(), left_vals.begin(), left_vals.end());
                        // 右表所有列
                        const auto& right_vals = right_row->get_values();
                        joined_row.insert(joined_row.end(), right_vals.begin(), right_vals.end());
                        result.push_back(joined_row);
                    }
                }
            }
        }
        
        return result;
    }

    // 12b. MERGE JOIN（基于排序的内连接，适用于已排序或可排序的中等规模数据）
    std::vector<std::vector<std::string>> merge_join(
        const std::string& left_table,
        const std::string& right_table,
        const std::string& left_col,
        const std::string& right_col
    ) {
        std::vector<std::vector<std::string>> result;
        // 获取列索引
        auto lidx_opt = storage.get_catalog().get_column_index(left_table, left_col);
        auto ridx_opt = storage.get_catalog().get_column_index(right_table, right_col);
        if (!lidx_opt || !ridx_opt) return result;
        size_t lidx = *lidx_opt, ridx = *ridx_opt;
        // 扫描两表
        auto lrows = seq_scan(left_table);
        auto rrows = seq_scan(right_table);
        // 提取键并排序
        auto key_of = [](const std::shared_ptr<Row>& r, size_t idx)->std::string {
            const auto& v = r->get_values(); return idx < v.size() ? v[idx] : std::string();
        };
        std::sort(lrows.begin(), lrows.end(), [&](const std::shared_ptr<Row>& a, const std::shared_ptr<Row>& b){
            return key_of(a, lidx) < key_of(b, lidx);
        });
        std::sort(rrows.begin(), rrows.end(), [&](const std::shared_ptr<Row>& a, const std::shared_ptr<Row>& b){
            return key_of(a, ridx) < key_of(b, ridx);
        });
        // 归并连接（处理重复键）
        size_t i = 0, j = 0;
        while (i < lrows.size() && j < rrows.size()) {
            const auto& lv = lrows[i]->get_values();
            const auto& rv = rrows[j]->get_values();
            std::string lk = lidx < lv.size() ? lv[lidx] : std::string();
            std::string rk = ridx < rv.size() ? rv[ridx] : std::string();
            if (lk < rk) { ++i; continue; }
            if (rk < lk) { ++j; continue; }
            // 相等：收集相同键的区间
            size_t i2 = i; while (i2 < lrows.size()) {
                const auto& lv2 = lrows[i2]->get_values();
                std::string k2 = lidx < lv2.size() ? lv2[lidx] : std::string();
                if (k2 != lk) break; ++i2;
            }
            size_t j2 = j; while (j2 < rrows.size()) {
                const auto& rv2 = rrows[j2]->get_values();
                std::string k2 = ridx < rv2.size() ? rv2[ridx] : std::string();
                if (k2 != rk) break; ++j2;
            }
            for (size_t a = i; a < i2; ++a) {
                const auto& la = lrows[a]->get_values();
                for (size_t b = j; b < j2; ++b) {
                    const auto& rb = rrows[b]->get_values();
                    std::vector<std::string> joined; joined.reserve(la.size()+rb.size());
                    joined.insert(joined.end(), la.begin(), la.end());
                    joined.insert(joined.end(), rb.begin(), rb.end());
                    result.push_back(std::move(joined));
                }
            }
            i = i2; j = j2;
        }
        return result;
    }

    // 13. ORDER BY算子：按指定列排序
    std::vector<std::shared_ptr<Row>> order_by(
        const std::string& table_name,
        const std::vector<std::pair<std::string, bool>>& order_clauses  // 列名, ASC(true)/DESC(false)
    ) {
        auto rows = seq_scan(table_name);
        if (order_clauses.empty()) return rows;

        // 获取列索引
        std::vector<std::pair<size_t, bool>> order_indices;  // 列索引, 升序标志
        for (const auto& clause : order_clauses) {
            auto col_idx = storage.get_catalog().get_column_index(table_name, clause.first);
            if (col_idx) {
                order_indices.push_back({*col_idx, clause.second});
            }
        }
        if (order_indices.empty()) return rows;

        // 排序比较函数
        auto compare_rows = [&order_indices](const std::shared_ptr<Row>& a, const std::shared_ptr<Row>& b) -> bool {
            const auto& vals_a = a->get_values();
            const auto& vals_b = b->get_values();
            
            for (const auto& order_idx : order_indices) {
                size_t col_idx = order_idx.first;
                bool ascending = order_idx.second;
                
                if (col_idx >= vals_a.size() || col_idx >= vals_b.size()) continue;
                
                const std::string& val_a = vals_a[col_idx];
                const std::string& val_b = vals_b[col_idx];
                
                // 尝试数值比较
                bool a_is_num = false, b_is_num = false;
                double num_a = 0.0, num_b = 0.0;
                try {
                    num_a = std::stod(val_a);
                    a_is_num = true;
                } catch (...) {}
                try {
                    num_b = std::stod(val_b);
                    b_is_num = true;
                } catch (...) {}
                
                if (a_is_num && b_is_num) {
                    if (num_a != num_b) {
                        return ascending ? (num_a < num_b) : (num_a > num_b);
                    }
                } else {
                    if (val_a != val_b) {
                        return ascending ? (val_a < val_b) : (val_a > val_b);
                    }
                }
            }
            return false;  // 相等
        };

        std::sort(rows.begin(), rows.end(), compare_rows);
        return rows;
    }

    // 14. GROUP BY算子：分组聚合
    struct GroupByResult {
        std::vector<std::string> group_keys;  // 分组键值
        std::map<std::string, double> aggregates;  // 聚合结果：函数名->值
    };

    std::vector<GroupByResult> group_by(
        const std::string& table_name,
        const std::vector<std::string>& group_columns,
        const std::vector<std::pair<std::string, std::string>>& agg_functions  // 列名, 函数名(COUNT/SUM/AVG/MAX/MIN)
    ) {
        std::vector<GroupByResult> result;
        auto rows = seq_scan(table_name);
        if (rows.empty()) return result;

        // 获取分组列索引
        std::vector<size_t> group_indices;
        for (const auto& col_name : group_columns) {
            auto col_idx = storage.get_catalog().get_column_index(table_name, col_name);
            if (col_idx) {
                group_indices.push_back(*col_idx);
            }
        }

        // 获取聚合列索引
        std::vector<std::pair<size_t, std::string>> agg_indices;  // 列索引, 函数名
        for (const auto& agg : agg_functions) {
            auto col_idx = storage.get_catalog().get_column_index(table_name, agg.first);
            if (col_idx) {
                agg_indices.push_back({*col_idx, agg.second});
            }
        }

        // 分组聚合
        std::map<std::string, std::vector<std::shared_ptr<Row>>> groups;
        
        for (const auto& row : rows) {
            const auto& vals = row->get_values();
            
            // 构建分组键
            std::string group_key;
            for (size_t i = 0; i < group_indices.size(); ++i) {
                if (i > 0) group_key += "|";
                if (group_indices[i] < vals.size()) {
                    group_key += vals[group_indices[i]];
                }
            }
            
            groups[group_key].push_back(row);
        }

        // 计算每个组的聚合值
        for (const auto& group : groups) {
            GroupByResult group_result;
            group_result.group_keys = group_columns;
            
            // 设置分组键值
            std::vector<std::string> key_parts;
            size_t pos = 0;
            for (size_t i = 0; i < group_columns.size(); ++i) {
                size_t next_pos = group.first.find('|', pos);
                if (next_pos == std::string::npos) next_pos = group.first.length();
                key_parts.push_back(group.first.substr(pos, next_pos - pos));
                pos = next_pos + 1;
            }
            group_result.group_keys = key_parts;

            // 计算聚合函数
            for (const auto& agg_idx : agg_indices) {
                size_t col_idx = agg_idx.first;
                const std::string& func_name = agg_idx.second;
                
                if (func_name == "COUNT") {
                    group_result.aggregates[func_name] = static_cast<double>(group.second.size());
                } else {
                    // 数值聚合
                    std::vector<double> values;
                    for (const auto& row : group.second) {
                        const auto& vals = row->get_values();
                        if (col_idx < vals.size()) {
                            try {
                                values.push_back(std::stod(vals[col_idx]));
                            } catch (...) {
                                // 忽略非数值
                            }
                        }
                    }
                    
                    if (!values.empty()) {
                        if (func_name == "SUM") {
                            double sum = 0.0;
                            for (double v : values) sum += v;
                            group_result.aggregates[func_name] = sum;
                        } else if (func_name == "AVG") {
                            double sum = 0.0;
                            for (double v : values) sum += v;
                            group_result.aggregates[func_name] = sum / values.size();
                        } else if (func_name == "MAX") {
                            group_result.aggregates[func_name] = *std::max_element(values.begin(), values.end());
                        } else if (func_name == "MIN") {
                            group_result.aggregates[func_name] = *std::min_element(values.begin(), values.end());
                        }
                    }
                }
            }
            
            result.push_back(group_result);
        }
        
        return result;
    }

    // 15. DROP TABLE算子：删除表及其所有数据
    bool drop_table(const std::string& table_name) {
        // Validate: table name not empty
        if (table_name.empty()) return false;
        
        // Validate: table exists
        auto schema_opt = storage.get_catalog().get_table_schema(table_name);
        if (!schema_opt) return false;  // Table does not exist
        
        // 1. Delete table metadata from system catalog
        bool catalog_success = storage.get_catalog().unregister_table(table_name);
        if (!catalog_success) return false;
        
        // 2. Clean up table data in storage engine
        bool storage_success = storage.drop_table_data(table_name);
        if (!storage_success) {
            // If storage cleanup fails, try to restore metadata (simplified handling)
            std::cout << "[CPP] Warning: Storage cleanup failed, but metadata deleted" << std::endl;
        }
        
        std::cout << "[CPP] DROP TABLE success: " << table_name << std::endl;
        return true;
    }
};

#endif // DB_CORE_H
