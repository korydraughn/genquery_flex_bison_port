#include "genquery_ast_types.hpp"

#include "vertex_property.hpp"
#include "edge_property.hpp"
#include "table_column_key_maps.hpp"
#include "genquery_utilities.hpp"

#include <boost/graph/graph_traits.hpp>
#include <boost/graph/adjacency_list.hpp>
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

        // NEW STUFF
        "R_META_MAIN_COLL",             // 19
        "R_META_MAIN_DATA",             // 20
        "R_OBJT_METAMAP_COLL",          // 21
        "R_OBJT_METAMAP_DATA",          // 22

        "R_META_MAIN_RESC",             // 23
        "R_META_MAIN_USER",             // 24
        "R_OBJT_METAMAP_RESC",          // 25
        "R_OBJT_METAMAP_USER",          // 26

        "R_OBJT_ACCESS_COLL",           // 27
        "R_OBJT_ACCESS_DATA",           // 28
        "R_TOKN_MAIN_COLL",             // 29
        "R_TOKN_MAIN_DATA",             // 30
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

        // NEW STUFF
        {0, 21},   // R_COLL_MAIN.coll_id = R_OBJT_METAMAP_COLL.object_id
        {1, 22},   // R_DATA_MAIN.data_id = R_OBJT_METAMAP_DATA.object_id
        {19, 21},  // R_META_MAIN_COLL.meta_id = R_OBJT_METAMAP_COLL.meta_id
        {20, 22},  // R_META_MAIN_DATA.meta_id = R_OBJT_METAMAP_DATA.meta_id

        {5, 25},   // R_RESC_MAIN.resc_id = R_OBJT_METAMAP_RESC.object_id
        {15, 26},  // R_USER_MAIN.user_id = R_OBJT_METAMAP_USER.object_id
        {23, 25},  // R_META_MAIN_RESC.meta_id = R_OBJT_METAMAP_RESC.meta_id
        {24, 26},  // R_META_MAIN_USER.meta_id = R_OBJT_METAMAP_USER.meta_id

        {27, 29},  // R_OBJT_ACCESS_COLL.access_type_id = R_TOKN_MAIN_COLL.token_id
        {28, 30},  // R_OBJT_ACCESS_DATA.access_type_id = R_TOKN_MAIN_DATA.token_id

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

        // NEW STUFF
        "R_COLL_MAIN.coll_id = R_OBJT_METAMAP_COLL.object_id",
        "R_DATA_MAIN.data_id = R_OBJT_METAMAP_DATA.object_id",
        "R_META_MAIN_COLL.meta_id = R_OBJT_METAMAP_COLL.meta_id",
        "R_META_MAIN_DATA.meta_id = R_OBJT_METAMAP_DATA.meta_id",

        "R_RESC_MAIN.resc_id = R_OBJT_METAMAP_RESC.object_id",
        "R_USER_MAIN.user_id = R_OBJT_METAMAP_USER.object_id",
        "R_META_MAIN_RESC.meta_id = R_OBJT_METAMAP_RESC.meta_id",
        "R_META_MAIN_USER.meta_id = R_OBJT_METAMAP_USER.meta_id",

        "R_OBJT_ACCESS_COLL.access_type_id = R_TOKN_MAIN_COLL.token_id",
        "R_OBJT_ACCESS_DATA.access_type_id = R_TOKN_MAIN_DATA.token_id",
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
    auto no_distinct_flag = false;
    auto in_select_clause = false;

    std::vector<std::string> select_columns;
    std::vector<std::string> columns;
    std::vector<std::string> tables;
    std::vector<std::string> values;

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

    void add_table(std::vector<std::string>& _v, const std::string_view _table)
    {
        if (auto iter = std::find(std::begin(_v), std::end(_v), _table); iter == std::end(_v)) {
            _v.push_back(_table.data());
        }
    }

    std::string sql(const Column& column)
    {
        const auto iter = column_name_mappings.find(column.name);
        
        if (iter != std::end(column_name_mappings)) {
            columns.push_back(fmt::format("{}.{}", iter->second.table, iter->second.name));

            if (in_select_clause) {
                select_columns.push_back(columns.back());
            }

            add_table(tables, iter->second.table);

            return columns.back();
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
            columns.push_back(fmt::format("{}.{}", iter->second.table, iter->second.name));

            if (in_select_clause) {
                select_columns.push_back(fmt::format("{}({})", select_function.name, columns.back()));
            }

            add_table(tables, iter->second.table);

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
    }
} // namespace irods::experimental::api::genquery

