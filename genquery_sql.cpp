#include "genquery_ast_types.hpp"

#include "vertex_property.hpp"
#include "edge_property.hpp"
#include "table_column_key_maps.hpp"
#include "genquery_utilities.hpp"

#include <boost/graph/graph_traits.hpp>
#include <boost/graph/adjacency_list.hpp>

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
        "R_ZONE_MAIN"                   // 18
    }); // table_names

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
        {15, 17}   // R_USER_MAIN.user_id = R_USER_SESSION_KEY.user_id
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
        "R_USER_MAIN.user_id = R_USER_SESSION_KEY.user_id"
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
    //using log = irods::experimental::log;

    auto no_distinct_flag = false;

    std::vector<std::string> columns;
    std::set<std::string> tables;
    std::vector<std::string> from_aliases;
    std::vector<std::string> where_clauses;
    std::vector<std::string> processed_tables;

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

    std::string sql(const Column& column)
    {
        const auto iter = column_name_mappings.find(column.name);
        
        if (iter != std::end(column_name_mappings)) {
            columns.push_back(fmt::format("{}.{}", iter->second.table, iter->second.name));
            tables.insert(iter->second.table);
            return columns.back();
        }

#if 0
        const auto iter = column_table_alias_map.find(column.name);

        if (iter == std::end(column_table_alias_map)) {
            throw std::runtime_error{fmt::format("failed to find column named [{}]", column.name)};
        }

        const auto& tbl = std::get<0>(iter->second);
        const auto& col = std::get<1>(iter->second);

        add_table_if_applicable(tbl);
        columns.push_back(col);

        return fmt::format("{}.{}", tbl, col);
#endif
        return "";
    }

    static std::uint8_t emit_second_paren{};

    std::string sql(const SelectFunction& select_function)
    {
        auto ret = fmt::format("{}(", select_function.name);
        emit_second_paren = 1;
        return ret;
    }

    std::string sql(const Selections& selections)
    {
        tables.clear();

        if (selections.empty()) {
            throw std::runtime_error{"selections are empty"};
        }

        std::string ret;

        for (auto&& selection : selections) {
            auto sel = boost::apply_visitor(sql_visitor(), selection);
        }

        return ret;
    }

    std::string sql(const ConditionOperator_Not& op_not)
    {
        std::string ret{" NOT "};
        ret += boost::apply_visitor(sql_visitor(), op_not.expression);
        return ret;
    }

    std::string sql(const ConditionNotEqual& not_equal)
    {
        std::string ret{" != "};
        ret += "'";
        ret += not_equal.string_literal;
        ret += "'";
        return ret;
    }

    std::string sql(const ConditionEqual& equal)
    {
        std::string ret{" = "};
        ret += "'";
        ret += equal.string_literal;
        ret += "'";
        return ret;
    }

    std::string sql(const ConditionLessThan& less_than)
    {
        std::string ret{" < "};
        ret += "'";
        ret += less_than.string_literal;
        ret += "'";
        return ret;
    }

    std::string sql(const ConditionLessThanOrEqualTo& less_than_or_equal_to)
    {
        std::string ret{" <= "};
        ret += "'";
        ret += less_than_or_equal_to.string_literal;
        ret += "'";
        return ret;
    }

    std::string sql(const ConditionGreaterThan& greater_than)
    {
        std::string ret{" > "};
        ret += "'";
        ret += greater_than.string_literal;
        ret += "'";
        return ret;
    }

    std::string sql(const ConditionGreaterThanOrEqualTo& greater_than_or_equal_to)
    {
        std::string ret{" >= "};
        ret += greater_than_or_equal_to.string_literal;
        return ret;
    }

    std::string sql(const ConditionBetween& between)
    {
        std::string ret{" BETWEEN '"};
        ret += between.low;
        ret += "' AND '";
        ret += between.high;
        ret += "'";
        return ret;
    }

    std::string sql(const ConditionIn& in)
    {
        std::string ret{" IN "};
        ret += to_string(in.list_of_string_literals);
        return ret;
    }

    std::string sql(const ConditionLike& like)
    {
        std::string ret{" LIKE '"};
        ret += like.string_literal;
        ret += "'";
        return ret;
    }

    std::string sql(const ConditionParentOf& parent_of)
    {
        std::string ret{"parent_of"};
        ret += parent_of.string_literal;
        return ret;
    }

    std::string sql(const ConditionBeginningOf& beginning_of)
    {
        std::string ret{"beginning_of"};
        ret += beginning_of.string_literal;
        return ret;
    }

    std::string sql(const Condition& condition)
    {
        std::string ret = sql(condition.column);
        ret += boost::apply_visitor(sql_visitor(), condition.expression);
        return ret;
    }

    std::string sql(const Conditions& conditions)
    {
        std::string ret;

        for (auto&& condition : conditions) {
            const auto iter = column_name_mappings.find(condition.column.name);
            
            if (iter != std::end(column_name_mappings)) {
                columns.push_back(fmt::format("{}.{}", iter->second.table, iter->second.name));
                tables.insert(iter->second.table);
            }
        }

        return ret;
    }

    std::string sql(const Select& select)
    {
        sql(select.selections);
        sql(select.conditions);

        auto sql_tables = fmt::format("tables  = [{}]\n", fmt::join(tables, ", "));
        auto sql_columns = fmt::format("columns = [{}]\n", fmt::join(columns, ", "));

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

        // Print the list of edges.
        fmt::print("Edges:\n");
        for (auto [iter, last] = boost::edges(g); iter != last; ++iter) {
            const auto src_vertex = boost::source(*iter, g);
            const auto dst_vertex = boost::target(*iter, g);
            fmt::print("  ({}, {})\n", g[src_vertex].table_name, g[dst_vertex].table_name);
        }

        const auto paths = compute_all_paths_from_source(g, table_name_index(*std::begin(tables)));
        fmt::print("All Paths:\n");
        for (auto&& p : paths) {
            fmt::print("  [{}]\n", fmt::join(p, ", "));
        }

        std::set<vertex_type> table_indices;
        std::transform(std::begin(tables), std::end(tables), std::inserter(table_indices, std::end(table_indices)),
                       [](auto&& _table_name) { return table_name_index(_table_name); });
        fmt::print("Table Indices:\n");
        for (auto&& i : table_indices) {
            fmt::print("  {} => {}\n", table_names[i], i);
        }

        const auto filtered_paths = irods::experimental::genquery::filter(paths, table_indices);
        fmt::print("Filtered Paths:\n");
        for (auto&& p : filtered_paths) {
            fmt::print("  [{}]\n", fmt::join(p, ", "));
        }

        const auto joins = to_table_joins(filtered_paths, g);
        fmt::print("Table Joins:\n");
        for (auto&& j : joins) {
            fmt::print("  [{}]\n", fmt::join(j, ", "));
        }

#if 0
        std::array<vertex_type, table_names.size()> predecessor_map;
        std::fill(std::begin(predecessor_map), std::end(predecessor_map), graph_type::null_vertex());

        const auto paths = compute_table_join_paths(g, predecessor_map, table_name_index(*std::begin(tables)));
        print_predecessor_map(paths, *std::begin(tables));

        std::ofstream out{"./gql.graphml"};
        boost::dynamic_properties dp;
        dp.property("name", boost::get(boost::vertex_name, g));
        boost::write_graphml(out, g, dp, true);
#endif

        return sql_tables + sql_columns;
    }

#if 0
=======================================================================================
ORIGINAL GENQUERY

select distinct R_DATA_MAIN.data_name
    from  R_DATA_MAIN,
          R_OBJT_METAMAP r_data_metamap,
          R_META_MAIN    r_data_meta_main,
          R_OBJT_METAMAP r_data_metamap2,
          R_META_MAIN    r_data_meta_mn02

    where r_data_meta_main.meta_attr_name = ? AND
          r_data_meta_mn02.meta_attr_name = ? AND
          R_DATA_MAIN.data_id     = r_data_metamap.object_id AND
          r_data_metamap.meta_id  = r_data_meta_main.meta_id AND
          r_data_metamap2.meta_id = r_data_meta_mn02.meta_id AND
          R_DATA_MAIN.data_id     = r_data_metamap2.object_id

    order by R_DATA_MAIN.data_name",

=======================================================================================
#endif
} // namespace irods::experimental::api::genquery

