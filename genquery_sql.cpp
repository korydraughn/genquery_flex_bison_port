#include "genquery_ast_types.hpp"

#include "vertex_property.hpp"
#include "edge_property.hpp"
#include "table_column_key_maps.hpp"
#include "genquery_utilities.hpp"

#include <boost/graph/graph_traits.hpp>
#include <boost/graph/adjacency_list.hpp>
#include <boost/graph/breadth_first_search.hpp>
#include <boost/iterator/function_input_iterator.hpp>
#include <fmt/format.h>

#include <iostream>
#include <stdexcept>
#include <map>
#include <array>
#include <vector>
#include <set>
#include <utility>
#include <string_view>
#include <fstream>

namespace
{
    // clang-format off
    using graph_type         = boost::adjacency_list<boost::vecS, boost::vecS, boost::undirectedS,
                                                     irods::experimental::genquery::vertex_property,
                                                     irods::experimental::genquery::edge_property>;
    using vertex_type        = boost::graph_traits<graph_type>::vertex_descriptor;
    using vertices_size_type = boost::graph_traits<graph_type>::vertices_size_type;
    using edge_type          = std::pair<vertex_type, vertex_type>;
    // clang-format on

    constinit const auto table_names = std::to_array({
        "R_COLL_MAIN",                  // 0
        "R_DATA_MAIN",                  // 1
        "R_META_MAIN",                  // 2
        "R_OBJT_ACCESS",                // 3
        "R_OBJT_METAMAP",               // 4
        "R_RESC_MAIN",                  // 5
        "R_RULE_EXEC",                  // 6
        "R_SPECIFIC_QUERY",             // 7
        "R_TICKET_ALLOWED_GROUPS",      // 8
        "R_TICKET_ALLOWED_HOSTS",       // 9
        "R_TICKET_ALLOWED_USERS",       // 10
        "R_TICKET_MAIN",                // 11
        "R_TOKN_MAIN",                  // 12
        "R_USER_AUTH",                  // 13
        "R_USER_GROUP",                 // 14
        "R_USER_MAIN",                  // 15
        "R_USER_PASSWORD",              // 16
        "R_USER_SESSION_KEY",           // 17
        "R_ZONE_MAIN",                  // 18
    }); // table_names

    // TODO Consider using the lookup function to resolve table names to indices.
    // TODO R_DATA_MAIN and R_USER_MAIN are joinable via data_owner_name (same as data_owner_zone).
    // TODO R_COLL_MAIN and R_USER_MAIN are joinable via coll_owner_name (same as coll_owner_zone).
    constinit const auto table_edges = std::to_array<edge_type>({
        {0, 1},    // R_COLL_MAIN.coll_id = R_DATA_MAIN.coll_id
        {0, 3},    // R_COLL_MAIN.coll_id = R_OBJT_ACCESS.object_id
        {0, 4},    // R_COLL_MAIN.coll_id = R_OBJT_METAMAP.object_id
        {0, 11},   // R_COLL_MAIN.coll_id = R_TICKET_MAIN.object_id

        {1, 3},    // R_DATA_MAIN.data_id = R_OBJT_ACCESS.object_id
        {1, 4},    // R_DATA_MAIN.data_id = R_OBJT_METAMAP.object_id
        {1, 5},    // R_DATA_MAIN.resc_id = R_RESC_MAIN.resc_id
        {1, 11},   // R_DATA_MAIN.data_id = R_TICKET_MAIN.object_id

        {2, 4},    // R_META_MAIN.meta_id = R_OBJT_METAMAP.meta_id

        {3, 12},   // R_OBJT_ACCESS.access_type_id = R_TOKN_MAIN.token_id

        {4, 5},    // R_OBJT_METAMAP.object_id = R_RESC_MAIN.resc_id
        {4, 15},   // R_OBJT_METAMAP.object_id = R_USER_MAIN.user_id

        {11, 15},  // R_TICKET_MAIN.user_id = R_USER_MAIN.user_id

        {15, 13},  // R_USER_MAIN.user_id = R_USER_AUTH.user_id
        {15, 14},  // R_USER_MAIN.user_id = R_USER_GROUP.group_user_id
        {15, 16},  // R_USER_MAIN.user_id = R_USER_PASSWORD.user_id
        {15, 17},  // R_USER_MAIN.user_id = R_USER_SESSION_KEY.user_id

        // TODO Handle R_USER_GROUP?
        // TODO Handle R_QUOTA_MAIN
        // TODO Handle R_QUOTA_USAGE
        // TODO Handle R_TICKET_MAIN
        // TODO Handle R_TICKET_ALLOWED_HOSTS
        // TODO Handle R_TICKET_ALLOWED_USERS
        // TODO Handle R_TICKET_ALLOWED_GROUPS
    }); // table_edges

    constinit const auto table_joins = std::to_array({
        "R_COLL_MAIN.coll_id = R_DATA_MAIN.coll_id",
        "R_COLL_MAIN.coll_id = R_OBJT_ACCESS.object_id",
        "R_COLL_MAIN.coll_id = R_OBJT_METAMAP.object_id",
        "R_COLL_MAIN.coll_id = R_TICKET_MAIN.object_id",

        "R_DATA_MAIN.data_id = R_OBJT_ACCESS.object_id",
        "R_DATA_MAIN.data_id = R_OBJT_METAMAP.object_id",
        "R_DATA_MAIN.resc_id = R_RESC_MAIN.resc_id",
        "R_DATA_MAIN.data_id = R_TICKET_MAIN.object_id",

        "R_META_MAIN.meta_id = R_OBJT_METAMAP.meta_id",

        "R_OBJT_ACCESS.access_type_id = R_TOKN_MAIN.token_id",

        "R_OBJT_METAMAP.object_id = R_RESC_MAIN.resc_id",
        "R_OBJT_METAMAP.object_id = R_USER_MAIN.user_id",

        "R_TICKET_MAIN.user_id = R_USER_MAIN.user_id",

        "R_USER_MAIN.user_id = R_USER_AUTH.user_id",
        "R_USER_MAIN.user_id = R_USER_GROUP.group_user_id",
        "R_USER_MAIN.user_id = R_USER_PASSWORD.user_id",
        "R_USER_MAIN.user_id = R_USER_SESSION_KEY.user_id",
    }); // table_joins

    constexpr auto table_name_index(const std::string_view _table_name) -> std::size_t
    {
        for (auto i = 0ull; i < table_names.size(); ++i) {
            if (table_names[i] == _table_name) {
                return i;
            }
        }

        throw std::invalid_argument{fmt::format("table [{}] not supported", _table_name)};
    } // table_name_index
} // anonymous namespace

namespace irods::experimental::api::genquery
{
    struct db_table
    {
        std::string name;
        std::string alias;
        int id;
    };

    struct db_column
    {
        std::string table_alias;
        std::string name;
    };

    auto operator==(const db_table& _lhs, const db_table& _rhs) -> bool
    {
        return _lhs.name == _rhs.name && _lhs.alias == _rhs.alias && _lhs.id == _rhs.id;
    }

    auto operator==(const db_column& _lhs, const db_column& _rhs) -> bool
    {
        return _lhs.table_alias == _rhs.table_alias && _lhs.name == _rhs.name;
    }

    auto no_distinct_flag = false;
    auto in_select_clause = false;

    std::vector<std::string> select_columns;
    std::vector<db_column> columns;
    std::vector<db_table> tables;
    std::vector<std::string> values;
    std::vector<std::string_view> resolved_columns;

    template <typename Iterable>
    std::string to_string(const Iterable& iterable)
    {
        return fmt::format("({}) ", fmt::join(iterable, ", "));
    }

    struct sql_visitor
        : public boost::static_visitor<std::string>
    {
        template <typename T>
        std::string operator()(const T& arg) const
        {
            return sql(arg);
        }
    };

    void add_table(std::vector<db_table>& _v, const db_table& _table)
    {
        if (auto iter = std::find(std::begin(_v), std::end(_v), _table); iter == std::end(_v)) {
            _v.push_back(_table);
        }
    }

    template <typename Container, typename Value>
    constexpr bool contains(const Container& _container, const Value& _value)
    {
        return std::find(std::begin(_container), std::end(_container), _value) != std::end(_container);
    }

    std::string sql(const Column& column)
    {
        const auto iter = column_name_mappings.find(column.name);
        
        if (iter != std::end(column_name_mappings)) {
            // TODO How do we know when to generate a new table alias?
            //
            // Consider the case where META_DATA_ATTR_NAME and META_COLL_ATTR_NAME
            // appear in the query.

            std::string table_alias;

            const auto table_iter = std::find_if(std::begin(tables), std::end(tables), [&col = *iter](auto&& _t) {
                return _t.name == col.second.table && _t.id == col.second.id;
            });

            if (table_iter == std::end(tables)) {
                table_alias = irods::experimental::genquery::generate_table_alias();
            }
            else {
                table_alias = table_iter->alias;
            }

            columns.push_back({table_alias, iter->second.name.data()});

            if (in_select_clause) {
                const auto& c = columns.back();
                select_columns.push_back(fmt::format("{}.{}", c.table_alias, c.name));
            }

            add_table(tables, {iter->second.table.data(), table_alias, iter->second.id});

            const auto& c = columns.back();
            return fmt::format("{}.{}", table_alias, c.name);
        }

        // TODO Is this the error case? Most likely.
        // You only reach this line if the GenQuery column name doesn't have a mapping.
        // Keep in mind that the select clause's return value is never used in this implementation.
        return "";
    }

    std::string sql(const SelectFunction& select_function)
    {
        const auto iter = column_name_mappings.find(select_function.column.name);
        
        if (iter != std::end(column_name_mappings)) {
            // TODO How do we know when to generate a new table alias?
            //
            // Consider the case where META_DATA_ATTR_NAME and META_COLL_ATTR_NAME
            // appear in the query.

            const auto table_alias = irods::experimental::genquery::generate_table_alias();

            columns.push_back({table_alias, iter->second.name.data()});

            if (in_select_clause) {
                const auto& c = columns.back();
                select_columns.push_back(fmt::format("{}({}.{})", select_function.name, c.table_alias, c.name));
            }

            add_table(tables, {iter->second.table.data(), table_alias});

            return select_columns.back();
        }

        // TODO Is this the error case? Most likely.
        // You only reach this line if the GenQuery column name doesn't have a mapping.
        // Keep in mind that the select clause's return value is never used in this implementation.
        return "";
    }

    std::string sql(const Selections& selections)
    {
        // TODO Replace with at_scope_exit.
        struct restore_value {
            bool* const value = &in_select_clause;
            ~restore_value() { *value = false; }
        } tmp;

        // Provides additional context around the processing of the select-clause's column list.
        //
        // This specifically helps the parser determine what should be included in the select
        // clause's column list. This is necessary because the Column data type is used in multiple
        // bison parser rules.
        in_select_clause = true;

        tables.clear();

        if (selections.empty()) {
            throw std::runtime_error{"selections are empty"};
        }

        for (auto&& selection : selections) {
            boost::apply_visitor(sql_visitor(), selection);
        }

        return "";
    }

    std::string sql(const ConditionOperator_Not& op_not)
    {
        return fmt::format(" not {}", boost::apply_visitor(sql_visitor(), op_not.expression));
    }

    std::string sql(const ConditionNotEqual& not_equal)
    {
        values.push_back(not_equal.string_literal);
        return " != ?";
    }

    std::string sql(const ConditionEqual& equal)
    {
        values.push_back(equal.string_literal);
        return " = ?";
    }

    std::string sql(const ConditionLessThan& less_than)
    {
        values.push_back(less_than.string_literal);
        return " < ?";
    }

    std::string sql(const ConditionLessThanOrEqualTo& less_than_or_equal_to)
    {
        values.push_back(less_than_or_equal_to.string_literal);
        return " <= ?";
    }

    std::string sql(const ConditionGreaterThan& greater_than)
    {
        values.push_back(greater_than.string_literal);
        return " > ?";
    }

    std::string sql(const ConditionGreaterThanOrEqualTo& greater_than_or_equal_to)
    {
        values.push_back(greater_than_or_equal_to.string_literal);
        return " >= ?";
    }

    std::string sql(const ConditionBetween& between)
    {
        values.push_back(between.low);
        values.push_back(between.high);
        return " between ? and ?";
    }

    std::string sql(const ConditionIn& in)
    {
        values.insert(std::end(values),
                      std::begin(in.list_of_string_literals),
                      std::end(in.list_of_string_literals));

        struct
        {
            using result_type = std::string_view;
            auto operator()() const noexcept -> result_type { return "?"; }
        } gen;

        using size_type = decltype(in.list_of_string_literals)::size_type;

        auto first = boost::make_function_input_iterator(gen, size_type{0});
        auto last  = boost::make_function_input_iterator(gen, in.list_of_string_literals.size());

        return fmt::format(" in ({})", fmt::join(first, last, ", "));
    }

    std::string sql(const ConditionLike& like)
    {
        values.push_back(like.string_literal);
        return " like ?";
    }

    std::string sql(const ConditionParentOf& parent_of)
    {
        values.push_back(parent_of.string_literal);
        return " parent_of ?"; // TODO Is this valid SQL?
    }

    std::string sql(const ConditionBeginningOf& beginning_of)
    {
        values.push_back(beginning_of.string_literal);
        return " beginning_of ?"; // TODO Is this valid SQL?
    }

    std::string sql(const Condition& condition)
    {
        return fmt::format("{}{}", sql(condition.column), boost::apply_visitor(sql_visitor(), condition.expression));
    }

    std::string sql(const Conditions& conditions)
    {
        std::string ret;

        for (auto&& condition : conditions) {
            ret += boost::apply_visitor(sql_visitor(), condition);
        }

        return ret;
    }

    std::string sql(const logical_and& condition)
    {
        return fmt::format(" and {}", sql(condition.condition));
    }

    std::string sql(const logical_or& condition)
    {
        return fmt::format(" or {}", sql(condition.condition));
    }

    std::string sql(const logical_grouping& condition)
    {
        return fmt::format("({})", sql(condition.conditions));
    }

    std::string sql(const Select& select)
    {
        sql(select.selections);
        const auto conds = sql(select.conditions);

        if (tables.empty()) {
            return "EMPTY RESULTSET";
        }

        // TODO Algorithm
        // 1. Gather all tables from columns.
        // 2. Assign table aliases to tables and columns.
        // 3. Resolve joins for each table.
        //
        // TODO Optimizations
        // - Cache the SQL for multiple runs of the same GenQuery string.

        // TODO Handle special case where the user only queries columns from a
        // single table (e.g. DATA_NAME, DATA_ID, DATA_REPL_NUM).

        std::for_each(std::begin(tables), std::end(tables), [](auto&& _t) {
            fmt::print("TABLE => {}, ALIAS => {}\n", _t.name, _t.alias);
        });
        fmt::print("\n");

        std::for_each(std::begin(columns), std::end(columns), [](auto&& _c) {
            fmt::print("COLUMN => {}.{}\n", _c.table_alias, _c.name);
        });
        fmt::print("\n");

        std::for_each(std::begin(select_columns), std::end(select_columns), [](auto&& _c) {
            fmt::print("COLUMN => {}\n", _c);
        });
        fmt::print("\n");

        graph_type g{table_edges.data(),
                     table_edges.data() + table_edges.size(),
                     table_names.size()};

        // Assign the table names to each vertex (i.e. each vertex represents a table).
        for (auto [iter, last] = boost::vertices(g); iter != last; ++iter) {
            g[*iter].table_name = table_names[*iter];
        }

        // Assign the table joins to each edge (i.e. each edge represents a table join).
        for (auto [iter, last] = boost::edges(g); iter != last; ++iter) {
            g[*iter].sql_join_condition = table_joins[table_edges.size() - std::distance(iter, last)];
        }

        std::set<std::vector<std::string_view>> all_paths;

        //for (auto&& t : tables) {
        for (auto&& t : table_names) {
            // Generate paths.
            std::array<vertex_type, table_names.size()> preds;
            std::fill(std::begin(preds), std::end(preds), graph_type::null_vertex());

            boost::breadth_first_search(
                g,
                //boost::vertex(table_name_index(tables[0].name), g),
                //boost::vertex(table_name_index(t.name), g),
                boost::vertex(table_name_index(t), g),
                boost::visitor(boost::make_bfs_visitor(boost::record_predecessors(
                    boost::make_iterator_property_map(preds.data(), boost::get(boost::vertex_index, g)),
                    boost::on_tree_edge{})))
            );

            const auto print_path = [](const auto& _pmap, std::size_t _dst_vertex) -> std::vector<std::string_view>
            {
                std::vector<std::string_view> path;

                for (auto cur = _dst_vertex; cur != std::size_t(-1); cur = _pmap[cur]) {
                    path.push_back(table_names[cur]);
                }

                std::reverse(std::begin(path), std::end(path));
                //fmt::print("[{}]\n", fmt::join(path, ", "));

                return path;
            };

            //std::vector<std::vector<std::string_view>> paths;
            for (auto i = 0ull; i < boost::num_vertices(g); ++i) {
                //paths.push_back(print_path(preds, i));
                all_paths.insert(print_path(preds, i));
            }

            // Find the paths from the src table to each dst table.
            //std::for_each(std::begin(tables), std::end(tables), [&g, &paths](const db_table& _db_table) {
            //});

            //fmt::print("\n");
        }

        std::for_each(std::begin(all_paths), std::end(all_paths), [](auto&& _path) {
            fmt::print("[{}]\n", fmt::join(_path, ", "));
        });
#if 0
        // TODO For each entry in columns, find the joins from the source vertex (i.e. table)
        // to the destination vertex (i.e. the entry in the columns vector).
        std::for_each(std::begin(columns), std::end(columns), [&g](auto&& _c) {
            const auto paths = compute_all_paths_from_source(g, table_name_index(tables[0].name));

            // Convert table names into indices. The index of a table matches its position in the array.
            std::set<vertex_type> table_indices;
            std::transform(std::begin(tables), std::end(tables), std::inserter(table_indices, std::end(table_indices)),
                           [](auto&& _table) { return table_name_index(_table.name); });

            const auto filtered_paths = irods::experimental::genquery::filter(paths, table_indices);
            const auto shortest_path = irods::experimental::genquery::get_shortest_path(filtered_paths);

            if (shortest_path) {
                fmt::print("\nShortest Filtered Path:\n");
                fmt::print("  [{}]\n", fmt::join(*shortest_path, ", "));
            }
        });
#endif

        return "";

#if 0
        graph_type g{table_edges.data(),
                     table_edges.data() + table_edges.size(),
                     table_names.size()};

        // Assign the table names to each vertex.
        for (auto [iter, last] = boost::vertices(g); iter != last; ++iter) {
            g[*iter].table_name = table_names[*iter];
        }

        // Assign the table joins to each edge.
        for (auto [iter, last] = boost::edges(g); iter != last; ++iter) {
            g[*iter].sql_join_condition = table_joins[table_edges.size() - std::distance(iter, last)];
        }

#define IRODS_GENQUERY_ENABLE_DEBUG
#ifdef IRODS_GENQUERY_ENABLE_DEBUG
        // Print the list of edges.
        fmt::print("Edges:\n");
        for (auto [iter, last] = boost::edges(g); iter != last; ++iter) {
            const auto src_vertex = boost::source(*iter, g);
            const auto dst_vertex = boost::target(*iter, g);
            fmt::print("  ({}, {})\n", g[src_vertex].table_name, g[dst_vertex].table_name);
        }
#endif

        const auto paths = compute_all_paths_from_source(g, table_name_index(tables[0]));
#ifdef IRODS_GENQUERY_ENABLE_DEBUG
        fmt::print("All Paths:\n");
        for (auto&& p : paths) {
            fmt::print("  [{}]\n", fmt::join(p, ", "));
        }
#endif

        std::set<vertex_type> table_indices;
        std::transform(std::begin(tables), std::end(tables), std::inserter(table_indices, std::end(table_indices)),
                       [](auto&& _table_name) { return table_name_index(_table_name); });
#ifdef IRODS_GENQUERY_ENABLE_DEBUG
        fmt::print("Table Indices:\n");
        for (auto&& i : table_indices) {
            fmt::print("  {} => {}\n", table_names[i], i);
        }
#endif

        const auto filtered_paths = irods::experimental::genquery::filter(paths, table_indices);
#ifdef IRODS_GENQUERY_ENABLE_DEBUG
        fmt::print("Filtered Paths:\n");
        for (auto&& p : filtered_paths) {
            fmt::print("  [{}]\n", fmt::join(p, ", "));
        }
#endif

        const auto shortest_path = irods::experimental::genquery::get_shortest_path(filtered_paths);

        if (shortest_path) {
#ifdef IRODS_GENQUERY_ENABLE_DEBUG
            fmt::print("Shortest Filtered Path:\n");
            fmt::print("  [{}]\n", fmt::join(*shortest_path, ", "));

            const auto joins = to_table_joins(*shortest_path, g, tables, table_names);
            fmt::print("Table Joins:\n");
            fmt::print("  [\n\t{}\n  ]\n", fmt::join(joins, ",\n\t"));
#endif

            const auto table_alias = table_alias_map.find(table_names[(*shortest_path)[0]]);
            auto generated_sql = fmt::format("select {}{} from {}",
                    select.no_distinct ? "" : "distinct ",
                    fmt::join(select_columns, ", "),
                    table_alias != std::end(table_alias_map)
                        ? table_alias->second
                        : table_names[(*shortest_path)[0]]);

            if (shortest_path->size() > 1) {
                const auto& sp = *shortest_path;

                for (decltype(sp.size()) i = 1; i < sp.size(); ++i) {
                    const auto table_alias = table_alias_map.find(table_names[sp[i]]);
                    const auto [edge, exists] = boost::edge(sp[i - 1], sp[i], g);

                    if (!exists) {
                        throw std::runtime_error{"Cannot construct SQL from GenQuery string."};
                    }

                    generated_sql = fmt::format("{} inner join {} on {}",
                                                generated_sql,
                                                table_alias != std::end(table_alias_map)
                                                    ? table_alias->second
                                                    : table_names[sp[i]],
                                                g[edge].sql_join_condition);
                }
            }

            if (!conds.empty()) {
                generated_sql += fmt::format(" where {}", conds);
            }

            if (!select.order_by.columns.empty()) {
                // All columns in the order by clause must exist in the list of columns to project.
                // TODO Replace all columns with real table names.
                generated_sql += fmt::format(" order by {} {}",
                                             fmt::join(select.order_by.columns, ", "),
                                             select.order_by.ascending_order ? "asc" : "desc");
            }

            return generated_sql;
        }

        return "";
#endif
    }
} // namespace irods::experimental::api::genquery

