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

    constinit const auto table_joins = std::to_array<irods::experimental::genquery::edge_property>({
        {"{}.coll_id = {}.coll_id",         "R_COLL_MAIN.coll_id = R_DATA_MAIN.coll_id"},
        {"{}.coll_id = {}.object_id",       "R_COLL_MAIN.coll_id = R_OBJT_ACCESS.object_id"},
        {"{}.coll_id = {}.object_id",       "R_COLL_MAIN.coll_id = R_OBJT_METAMAP.object_id"},
        {"{}.coll_id = {}.object_id",       "R_COLL_MAIN.coll_id = R_TICKET_MAIN.object_id"},

        {"{}.data_id = {}.object_id",       "R_DATA_MAIN.data_id = R_OBJT_ACCESS.object_id"},
        {"{}.data_id = {}.object_id",       "R_DATA_MAIN.data_id = R_OBJT_METAMAP.object_id"},
        {"{}.resc_id = {}.resc_id",         "R_DATA_MAIN.resc_id = R_RESC_MAIN.resc_id"},
        {"{}.data_id = {}.object_id",       "R_DATA_MAIN.data_id = R_TICKET_MAIN.object_id"},

        {"{}.meta_id = {}.meta_id",         "R_META_MAIN.meta_id = R_OBJT_METAMAP.meta_id"},

        {"{}.access_type_id = {}.token_id", "R_OBJT_ACCESS.access_type_id = R_TOKN_MAIN.token_id"},

        {"{}.object_id = {}.resc_id",       "R_OBJT_METAMAP.object_id = R_RESC_MAIN.resc_id"},
        {"{}.object_id = {}.user_id",       "R_OBJT_METAMAP.object_id = R_USER_MAIN.user_id"},

        {"{}.user_id = {}.user_id",         "R_TICKET_MAIN.user_id = R_USER_MAIN.user_id"},

        {"{}.user_id = {}.user_id",         "R_USER_MAIN.user_id = R_USER_AUTH.user_id"},
        {"{}.user_id = {}.group_user_id",   "R_USER_MAIN.user_id = R_USER_GROUP.group_user_id"},
        {"{}.user_id = {}.user_id",         "R_USER_MAIN.user_id = R_USER_PASSWORD.user_id"},
        {"{}.user_id = {}.user_id",         "R_USER_MAIN.user_id = R_USER_SESSION_KEY.user_id"}
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

    auto generate_table_alias() -> std::string
    {
        static int id = 0;
        return fmt::format("t{}", id++);
    } // generate_table_alias

    constexpr auto is_metadata_column(std::string_view _column) -> bool
    {
        // clang-format off
        return _column.starts_with("META_D") ||
               _column.starts_with("META_C") ||
               _column.starts_with("META_R") ||
               _column.starts_with("META_U");
        // clang-format on
    } // is_metadata_column
} // anonymous namespace

namespace irods::experimental::api::genquery
{
    auto no_distinct_flag = false;
    auto in_select_clause = false;

    bool add_joins_for_meta_data = false;
    bool add_joins_for_meta_coll = false;
    bool add_joins_for_meta_resc = false;
    bool add_joins_for_meta_user = false;

    std::vector<std::string> columns_for_select_clause;
    std::vector<std::string> columns_for_where_clause;
    std::vector<std::string_view> sql_tables;

    std::vector<std::string> select_columns;
    std::vector<std::string> values;
    std::vector<std::string_view> resolved_columns;

    std::map<std::string, std::string> table_aliases;

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

    template <typename Container, typename Value>
    constexpr bool contains(const Container& _container, const Value& _value)
    {
        return std::find(std::begin(_container), std::end(_container), _value) != std::end(_container);
    }

    std::string sql(const Column& column)
    {
        const auto iter = column_name_mappings.find(column.name);
        
        if (iter == std::end(column_name_mappings)) {
            throw std::invalid_argument{fmt::format("unknown column: {}", column.name)};
        }

        auto is_special_column = true;
        std::string_view sp_fmt_arg;

        // clang-format off
        if      (column.name.starts_with("META_D")) { add_joins_for_meta_data = true; sp_fmt_arg = "mmd"; }
        else if (column.name.starts_with("META_C")) { add_joins_for_meta_coll = true; sp_fmt_arg = "mmc"; }
        else if (column.name.starts_with("META_R")) { add_joins_for_meta_resc = true; sp_fmt_arg = "mmr"; }
        else if (column.name.starts_with("META_U")) { add_joins_for_meta_user = true; sp_fmt_arg = "mmu"; }
        else                                        { is_special_column = false; }
        // clang-format on

        if (!contains(sql_tables, iter->second.table)) {
            // Special columns such as the general query metadata columns are handled separately
            // because they require multiple table joins. For this reason, we don't allow any of those
            // tables to be added to the table list.
            if (is_special_column) {
                if (column.name.starts_with("META_D")) {
                    if (!contains(sql_tables, "R_DATA_MAIN")) {
                        sql_tables.emplace_back("R_DATA_MAIN");
                        table_aliases["R_DATA_MAIN"] = generate_table_alias();
                    }
                }
                else if (column.name.starts_with("META_C")) {
                    if (!contains(sql_tables, "R_COLL_MAIN")) {
                        sql_tables.emplace_back("R_COLL_MAIN");
                        table_aliases["R_COLL_MAIN"] = generate_table_alias();
                    }
                }
                else if (column.name.starts_with("META_R")) {
                    if (!contains(sql_tables, "R_RESC_MAIN")) {
                        sql_tables.emplace_back("R_RESC_MAIN");
                        table_aliases["R_RESC_MAIN"] = generate_table_alias();
                    }
                }
                else if (column.name.starts_with("META_U")) {
                    if (!contains(sql_tables, "R_USER_MAIN")) {
                        sql_tables.emplace_back("R_USER_MAIN");
                        table_aliases["R_USER_MAIN"] = generate_table_alias();
                    }
                }
            }
            else {
                sql_tables.push_back(iter->second.table);
                table_aliases[std::string{iter->second.table}] = generate_table_alias();
            }
        }

        auto* columns_ptr = in_select_clause ? &columns_for_select_clause : &columns_for_where_clause;
        const std::string_view alias = is_special_column ? sp_fmt_arg : table_aliases.at(std::string{iter->second.table});
        columns_ptr->push_back(fmt::format("{}.{}", alias, iter->second.name));

        return columns_ptr->back();
    }

    std::string sql(const SelectFunction& select_function)
    {
        const auto iter = column_name_mappings.find(select_function.column.name);
        
        if (iter == std::end(column_name_mappings)) {
            throw std::invalid_argument{fmt::format("unknown column: {}", select_function.column.name)};
        }

        // TODO Allow aggregate functions in where clause.
        // For now, let's ignore that case until we have something more functional.
        //
        // TODO Aggregate functions are not allowed in the WHERE clause of an SQL statement!
        if (!in_select_clause) {
            throw std::invalid_argument{"aggregate functions not allowed in where clause"};
        }

        auto is_special_column = true;
        std::string_view sp_fmt_arg;

        // clang-format off
        if      (select_function.column.name.starts_with("META_D")) { add_joins_for_meta_data = true; sp_fmt_arg = "mmd"; }
        else if (select_function.column.name.starts_with("META_C")) { add_joins_for_meta_coll = true; sp_fmt_arg = "mmc"; }
        else if (select_function.column.name.starts_with("META_R")) { add_joins_for_meta_resc = true; sp_fmt_arg = "mmr"; }
        else if (select_function.column.name.starts_with("META_U")) { add_joins_for_meta_user = true; sp_fmt_arg = "mmu"; }
        else                                                        { is_special_column = false; }
        // clang-format on

        if (!contains(sql_tables, iter->second.table)) {
            // Special columns such as the general query metadata columns are handled separately
            // because they require multiple table joins. For this reason, we don't allow any of those
            // tables to be added to the table list.
            if (is_special_column) {
                if (select_function.column.name.starts_with("META_D")) {
                    if (!contains(sql_tables, "R_DATA_MAIN")) {
                        sql_tables.emplace_back("R_DATA_MAIN");
                        table_aliases["R_DATA_MAIN"] = generate_table_alias();
                    }
                }
                else if (select_function.column.name.starts_with("META_C")) {
                    if (!contains(sql_tables, "R_COLL_MAIN")) {
                        sql_tables.emplace_back("R_COLL_MAIN");
                        table_aliases["R_COLL_MAIN"] = generate_table_alias();
                    }
                }
                else if (select_function.column.name.starts_with("META_R")) {
                    if (!contains(sql_tables, "R_RESC_MAIN")) {
                        sql_tables.emplace_back("R_RESC_MAIN");
                        table_aliases["R_RESC_MAIN"] = generate_table_alias();
                    }
                }
                else if (select_function.column.name.starts_with("META_U")) {
                    if (!contains(sql_tables, "R_USER_MAIN")) {
                        sql_tables.emplace_back("R_USER_MAIN");
                        table_aliases["R_USER_MAIN"] = generate_table_alias();
                    }
                }
            }
            else {
                sql_tables.push_back(iter->second.table);
                table_aliases[std::string{iter->second.table}] = generate_table_alias();
            }
        }

#if 0
        auto& column = iter->second;
        columns_for_select_clause.push_back(fmt::format("{}({}.{})", select_function.name, column.table, column.name));

        if (is_special_column) {
            return fmt::format("{}({{}}.{})", select_function.name, column.name);
        }

        return fmt::format("{}({}.{})", select_function.name, table_aliases.at(std::string{column.table}), column.name);
#else
        // TODO Aggregate functions are not allowed in the WHERE clause of an SQL statement!
        //auto* columns_ptr = in_select_clause ? &columns_for_select_clause : &columns_for_where_clause;
        auto* columns_ptr = &columns_for_select_clause;
        const std::string_view alias = is_special_column ? sp_fmt_arg : table_aliases.at(std::string{iter->second.table});
        columns_ptr->push_back(fmt::format("{}({}.{})", select_function.name, alias, iter->second.name));

        return columns_ptr->back();
#endif
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

        sql_tables.clear();

        if (selections.empty()) {
            throw std::runtime_error{"no columns selected."};
        }

        for (auto&& selection : selections) {
            boost::apply_visitor(sql_visitor(), selection);
        }

        return "";
    }

    std::string sql(const ConditionOperator_Not& op_not)
    {
        return fmt::format(" not{}", boost::apply_visitor(sql_visitor(), op_not.expression));
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
        try {
            fmt::print("### PHASE 1: Gather\n\n");

            // Extract tables and columns from general query statement.
            sql(select.selections);

            // Convert the conditions of the general query statement into SQL with prepared
            // statement placeholders.
            const auto conds = sql(select.conditions);
            fmt::print("CONDITIONS = {}\n\n", conds);

            if (sql_tables.empty()) {
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

            std::for_each(std::begin(sql_tables), std::end(sql_tables), [](auto&& _t) {
                std::string_view alias = "";

                if (const auto iter = table_aliases.find(std::string{_t}); iter != std::end(table_aliases)) {
                    alias = iter->second;
                }

                fmt::print("TABLE => {} [alias={}]\n", _t, alias);
            });
            fmt::print("\n");

            std::for_each(std::begin(columns_for_select_clause), std::end(columns_for_select_clause), [](auto&& _c) {
                fmt::print("COLUMN FOR SELECT CLAUSE => {}\n", _c);
            });
            fmt::print("\n");

            std::for_each(std::begin(columns_for_where_clause), std::end(columns_for_where_clause), [](auto&& _c) {
                fmt::print("COLUMN FOR WHERE CLAUSE => {}\n", _c);
            });
            fmt::print("\n");

            fmt::print("requires metadata table joins for R_DATA_MAIN? {}\n", (add_joins_for_meta_data == true));
            fmt::print("requires metadata table joins for R_COLL_MAIN? {}\n", (add_joins_for_meta_coll == true));
            fmt::print("requires metadata table joins for R_RESC_MAIN? {}\n", (add_joins_for_meta_resc == true));
            fmt::print("requires metadata table joins for R_USER_MAIN? {}\n", (add_joins_for_meta_user == true));
            fmt::print("\n");

            //
            // Generate SQL
            //

            //std::sort(std::begin(sql_tables), std::end(sql_tables), [](auto&& _t1, auto&& _t2) {
                //return table_name_index(_t1) < table_name_index(_t2);
            //});

            fmt::print("\n### PHASE 2: SQL Generation\n\n");

            graph_type graph{table_edges.data(),
                             table_edges.data() + table_edges.size(),
                             table_names.size()};

            for (auto [iter, last] = boost::vertices(graph); iter != last; ++iter) {
                graph[*iter].table_name = table_names[*iter];
            }

            for (auto [iter, last] = boost::edges(graph); iter != last; ++iter) {
                graph[*iter] = table_joins[table_edges.size() - std::distance(iter, last)];
            }

            // Generate the SELECT clause.
            //
            // TODO Use Boost.Graph to resolve table joins for the SELECT clause.
            // This step does not concern itself with special columns (e.g. META_DATA_ATTR_NAME). Those
            // will handled in a later step.
            //
            // The tables stored in sql_tables must be directly joinable to at least one other table in
            // the sql_tables list. This step is NOT allowed to introduce intermediate tables.
            auto select_clause = fmt::format("select {columns} from {table} {alias}",
                                             fmt::arg("columns", fmt::join(columns_for_select_clause, ", ")),
                                             fmt::arg("table", sql_tables.front()),
                                             fmt::arg("alias", table_aliases.at(std::string{sql_tables.front()})));

            fmt::print("SELECT CLAUSE => {}\n", select_clause);
            fmt::print("\n");

            const auto get_table_join = [&graph](const auto& _t1, const auto& _t2) -> std::string {
                const auto t1_idx = table_name_index(_t1);
                const auto t2_idx = table_name_index(_t2);
                const auto [edge, exists] = boost::edge(t1_idx, t2_idx, graph);

                if (!exists) {
                    return "";
                }

                const auto t1_alias = table_aliases.at(std::string{_t1});
                const auto t2_alias = table_aliases.at(std::string{_t2});
                const auto sql = fmt::format("inner join {} {} on {}", _t2, t2_alias, graph[edge].table_join_fmt);

                return fmt::format(fmt::runtime(sql), t1_alias, t2_alias);
            };

            // The order of inner-joins matters when they interleave.
            //
            // IDEA?
            // 1. Find a inner joins that connect to the table in the FROM clause.
            // 2. Find next joins based on what criteria?
            //
            // THOUGHTS:
            // Should there be a join table order?
            // Some tables don't care about order while others do.
#if 1
            std::vector<std::string> inner_joins;
            std::vector<std::string> already_joined;

            // Add all join-clauses that link against the first table.
            // This is the special case.
            for (auto iter = std::begin(sql_tables) + 1; iter != std::end(sql_tables); ++iter) {
                if (auto j = get_table_join(sql_tables.front(), *iter); !j.empty()) {
                    inner_joins.push_back(j);
                    already_joined.push_back(std::string{*iter});
                }
            }

            // At this point, no other table can join the table in the FROM clause.
            // We can safely ignore the table in the FROM clause.
            for (auto i = 0ull; i < sql_tables.size() - 1; ++i) {
                const auto last = already_joined.back();

                for (auto iter = std::begin(sql_tables) + 1; iter != std::end(sql_tables); ++iter) {
                    if (contains(already_joined, *iter)) {
                        continue;
                    }

                    if (auto j = get_table_join(last, *iter); !j.empty()) {
                        inner_joins.push_back(j);
                        already_joined.push_back(std::string{*iter});
                    }
                }
            }
#elif 0
            std::vector<std::string> inner_joins;
            auto tables_remaining = sql_tables;

            // Remove the first table as it is part of the FROM clause.
            tables_remaining.erase(std::begin(tables_remaining));

            while (!tables_remaining.empty()) {
                // Prefer joins to table in FROM clause over secondary tables.
                if (auto j = get_table_join(sql_tables.front(), tables_remaining.front()); !j.empty()) {
                    inner_joins.push_back(j);
                    tables_remaining.erase(std::begin(tables_remaining));
                    continue;
                }

                for (auto iter = std::begin(tables_remaining); iter != std::end(tables_remaining); ++iter) {
                    if (auto next = iter + 1; next == std::end(tables_remaining)) {
                        continue;
                    }

                    if (auto j = get_table_join(*(iter + 1), *iter); !j.empty()) {
                        inner_joins.push_back(j);
                        tables_remaining.erase(iter);
                        break;
                    }
                }
            }
#else
            std::set<std::string> inner_joins;

            for (auto&& s : sql_tables) {
                for (auto&& t : sql_tables) {
                    if (s == t) {
                        continue;
                    }

                    // Ignore the first element of the sql_tables list. The first element is part of the
                    // FROM clause.
                    if (t == sql_tables.front()) {
                        continue;
                    }

                    if (auto j = get_table_join(s, t); !j.empty()) {
                        inner_joins.insert(j);
                    }
                }
            }
#endif
            std::for_each(std::begin(inner_joins), std::end(inner_joins), [](auto&& _j) {
                fmt::print("INNER JOIN => {}\n", _j);
            });
            fmt::print("\n");

            if (inner_joins.size() != sql_tables.size() - 1) {
                throw std::invalid_argument{"invalid general query"};
            }

            auto sql = select_clause;

            if (!inner_joins.empty()) {
                sql += fmt::format(" {}", fmt::join(inner_joins, " "));
            }

            // TODO The following SQL statements can be stored as a format string for reuse. Placeholders can
            // be given names so that we can replace markers that match the same value.
            //
            // TODO Remember: Table aliases for R_OBJT_METAMAP and R_META_MAIN can be generated without needing
            // to store them in the table_aliases container by simply concatenating the table alias of META_*_ATTR_*
            // with a fixed string. Doing this will avoid alias collisions and allow the SQL-join strings to be
            // captured in a hard-coded list.
            if (add_joins_for_meta_data) {
                // select distinct d.data_id, c.coll_name, d.data_name, mmd.meta_attr_name, mmc.meta_attr_name
                // from R_COLL_MAIN c                                                                                
                // inner join R_DATA_MAIN d on c.coll_id = d.coll_id                                                 
                // left join R_OBJT_METAMAP ommd on d.data_id = ommd.object_id                                       
                // left join R_OBJT_METAMAP ommc on c.coll_id = ommc.object_id                                       
                // left join R_META_MAIN mmd on ommd.meta_id = mmd.meta_id                                           
                // left join R_META_MAIN mmc on ommc.meta_id = mmc.meta_id                                           
                // where mmd.meta_attr_name = 'job' or                                                               
                //       mmc.meta_attr_name = 'nope';                                                                
                sql += fmt::format(" left join R_OBJT_METAMAP ommd on {}.data_id = ommd.object_id "
                                   "left join R_META_MAIN mmd on ommd.meta_id = mmd.meta_id",
                                   table_aliases.at("R_DATA_MAIN"));
            }

            if (add_joins_for_meta_coll) {
                sql += fmt::format(" left join R_OBJT_METAMAP ommc on {}.coll_id = ommc.object_id "
                                   "left join R_META_MAIN mmc on ommc.meta_id = mmc.meta_id",
                                   table_aliases.at("R_COLL_MAIN"));
            }

            if (add_joins_for_meta_resc) {
                sql += fmt::format(" left join R_OBJT_METAMAP ommr on {}.resc_id = ommr.object_id "
                                   "left join R_META_MAIN mmr on ommr.meta_id = mmr.meta_id",
                                   table_aliases.at("R_RESC_MAIN"));
            }

            if (add_joins_for_meta_user) {
                sql += fmt::format(" left join R_OBJT_METAMAP ommu on {}.user_id = ommu.object_id "
                                   "left join R_META_MAIN mmu on ommu.meta_id = mmu.meta_id",
                                   table_aliases.at("R_USER_MAIN"));
            }

            if (!conds.empty()) {
                sql += fmt::format(" where {}", conds);
            }

            fmt::print("generated SQL => [{}]\n", sql);

            return "";
        }
        catch (const std::exception& e) {
            fmt::print(stderr, "Exception: {}\n", e.what());
        }

        return "";
    }
} // namespace irods::experimental::api::genquery
