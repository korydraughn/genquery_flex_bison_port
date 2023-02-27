#include "irods/genquery_sql.hpp"

#include "irods/genquery_ast_types.hpp"
#include "irods/vertex_property.hpp"
#include "irods/edge_property.hpp"
#include "irods/table_column_key_maps.hpp"

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
#include <algorithm>

namespace
{
    namespace gq = irods::experimental::api::genquery;

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
        "R_TICKET_ALLOWED_HOSTS",       // 8
        "R_TICKET_ALLOWED_USERS",       // 9
        "R_TICKET_ALLOWED_GROUPS",      // 10
        "R_TICKET_MAIN",                // 11
        "R_TOKN_MAIN",                  // 12
        "R_USER_AUTH",                  // 13
        "R_USER_GROUP",                 // 14
        "R_USER_MAIN",                  // 15
        "R_USER_PASSWORD",              // 16
        "R_USER_SESSION_KEY",           // 17
        "R_ZONE_MAIN",                  // 18
        "R_QUOTA_MAIN",                 // 19
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

        {11, 8},   // R_TICKET_MAIN.ticket_id = R_TICKET_ALLOWED_HOSTS.ticket_id
        {11, 9},   // R_TICKET_MAIN.ticket_id = R_TICKET_ALLOWED_USERS.ticket_id
        {11, 10},  // R_TICKET_MAIN.ticket_id = R_TICKET_ALLOWED_GROUPS.ticket_id

        {15, 13},  // R_USER_MAIN.user_id = R_USER_AUTH.user_id
        {15, 14},  // R_USER_MAIN.user_id = R_USER_GROUP.group_user_id
        {15, 16},  // R_USER_MAIN.user_id = R_USER_PASSWORD.user_id
        {15, 17},  // R_USER_MAIN.user_id = R_USER_SESSION_KEY.user_id

        // TODO Handle R_USER_GROUP?
        // TODO Handle R_QUOTA_MAIN
        // TODO Handle R_QUOTA_USAGE
    }); // table_edges

    constinit const auto table_joins = std::to_array<irods::experimental::genquery::edge_property>({
        {"{}.coll_id = {}.coll_id"},
        {"{}.coll_id = {}.object_id"},
        {"{}.coll_id = {}.object_id"},
        {"{}.coll_id = {}.object_id"},

        {"{}.data_id = {}.object_id"},
        {"{}.data_id = {}.object_id"},
        {"{}.resc_id = {}.resc_id"},
        {"{}.data_id = {}.object_id"},

        {"{}.meta_id = {}.meta_id"},

        {"{}.access_type_id = {}.token_id"},

        {"{}.object_id = {}.resc_id"},
        {"{}.object_id = {}.user_id"},

        {"{}.user_id = {}.user_id"},

        {"{}.ticket_id = {}.ticket_id"},
        {"{}.ticket_id = {}.ticket_id"},
        {"{}.ticket_id = {}.ticket_id"},

        {"{}.user_id = {}.user_id"},
        {"{}.user_id = {}.group_user_id"},
        {"{}.user_id = {}.user_id"},
        {"{}.user_id = {}.user_id"},
    }); // table_joins

    // The following SQL is a recursive WITH clause which produces all resource hierarchies
    // in the database upon request.
    constexpr const char* data_resc_hier_with_clause =
        "with recursive T as ("
            "select "
                "resc_id, "
                "resc_name hier, "
                "case "
                    "when resc_parent = '' then 0 "
                    "else cast(resc_parent as bigint) " // TODO Is bigint supported in all databases?
                "end parent_id "
            "from "
                "r_resc_main "
            "where "
                "resc_id > 0 "
            "union all "
            "select "
                "T.resc_id, "
                "cast((U.resc_name || ';' || T.hier) as varchar(250)), "
                "case "
                    "when U.resc_parent = '' then 0 "
                    "else cast(U.resc_parent as bigint) "
                "end parent_id "
            "from T "
            "inner join r_resc_main U on U.resc_id = T.parent_id"
        ") ";

    constexpr auto table_name_index(const std::string_view _table_name) -> std::size_t
    {
        for (auto i = 0ull; i < table_names.size(); ++i) {
            if (table_names[i] == _table_name) {
                return i;
            }
        }

        throw std::invalid_argument{fmt::format("table [{}] not supported", _table_name)};
    } // table_name_index

    int table_alias_id = 0;

    auto generate_table_alias() -> std::string
    {
        return fmt::format("t{}", table_alias_id++);
    } // generate_table_alias

    auto generate_inner_joins(const graph_type& _graph,
                              const std::vector<std::string_view>& _tables,
                              const std::map<std::string, std::string>& _table_aliases) -> std::vector<std::string>
    {
        const auto get_table_join = [&_graph, &_table_aliases](const auto& _t1, const auto& _t2) -> std::string
        {
            const auto t1_idx = table_name_index(_t1);
            const auto t2_idx = table_name_index(_t2);
            const auto [edge, exists] = boost::edge(t1_idx, t2_idx, _graph);

            if (!exists) {
                return "";
            }

            const auto t1_alias = _table_aliases.at(std::string{_t1});
            const auto t2_alias = _table_aliases.at(std::string{_t2});
            const auto sql = fmt::format("inner join {} {} on {}", _t2, t2_alias, _graph[edge].table_join_fmt);

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
        std::vector<std::string> inner_joins;
        inner_joins.reserve(_tables.size() - 1);

        std::vector<std::string> remaining{std::begin(_tables) + 1, std::end(_tables)};
        fmt::print("remaining = [{}]\n", fmt::join(remaining, ", "));

        std::vector<std::string> processed;
        processed.reserve(_tables.size());
        processed.push_back(std::string{_tables.front()});
        fmt::print("processed = [{}]\n", fmt::join(processed, ", "));

        for (decltype(_tables.size()) i = 0; i < _tables.size() - 1; ++i) {
            const auto& last = processed.back();

            for (auto iter = std::begin(remaining); iter != std::end(remaining);) {
                if (auto j = get_table_join(last, *iter); !j.empty()) {
                    inner_joins.push_back(std::move(j));
                    processed.emplace_back(*iter);
                    iter = remaining.erase(iter);
                }
                else {
                    ++iter;
                }
            }
        }

        return inner_joins;
    } // generate_inner_joins

    auto generate_joins_for_metadata_columns(const std::map<std::string, std::string>& _table_aliases,
                                             bool _add_joins_for_meta_data,
                                             bool _add_joins_for_meta_coll,
                                             bool _add_joins_for_meta_resc,
                                             bool _add_joins_for_meta_user) -> std::string
    {

        std::string sql;

        if (_add_joins_for_meta_data) {
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
                               _table_aliases.at("R_DATA_MAIN"));
        }

        if (_add_joins_for_meta_coll) {
            sql += fmt::format(" left join R_OBJT_METAMAP ommc on {}.coll_id = ommc.object_id "
                               "left join R_META_MAIN mmc on ommc.meta_id = mmc.meta_id",
                               _table_aliases.at("R_COLL_MAIN"));
        }

        if (_add_joins_for_meta_resc) {
            sql += fmt::format(" left join R_OBJT_METAMAP ommr on {}.resc_id = ommr.object_id "
                               "left join R_META_MAIN mmr on ommr.meta_id = mmr.meta_id",
                               _table_aliases.at("R_RESC_MAIN"));
        }

        if (_add_joins_for_meta_user) {
            sql += fmt::format(" left join R_OBJT_METAMAP ommu on {}.user_id = ommu.object_id "
                               "left join R_META_MAIN mmu on ommu.meta_id = mmu.meta_id",
                               _table_aliases.at("R_USER_MAIN"));
        }

        return sql;
    } // generate_joins_for_metadata_columns

    auto generate_order_by_clause(const gq::order_by& _order_by,
                                  const std::map<std::string, std::string>& _table_aliases,
                                  const std::map<std::string_view, gq::column_info>& _column_name_mappings,
                                  const std::vector<const gq::column*>& ast_column_ptrs) -> std::string
    {
        if (_order_by.sort_expressions.empty()) {
            return "";
        }

        const auto& sort_expressions = _order_by.sort_expressions;

        std::vector<std::string> sort_expr;
        sort_expr.reserve(sort_expressions.size());

        for (const auto& se : sort_expressions) {
            const auto iter = _column_name_mappings.find(se.column);

            if (iter == std::end(_column_name_mappings)) {
                throw std::invalid_argument{fmt::format("unknown column in order-by clause: {}", se.column)};
            }

            auto is_special_column = true;
            std::string_view alias;

            // clang-format off
            if      (se.column.starts_with("META_D")) { alias = "mmd"; }
            else if (se.column.starts_with("META_C")) { alias = "mmc"; }
            else if (se.column.starts_with("META_R")) { alias = "mmr"; }
            else if (se.column.starts_with("META_U")) { alias = "mmu"; }
            else if (se.column == "DATA_RESC_HIER")   { alias = "T"; }
            else                                      { is_special_column = false; }
            // clang-format on

            if (!is_special_column) {
                alias = _table_aliases.at(std::string{iter->second.table});
            }

            const auto ast_iter = std::find_if(std::begin(ast_column_ptrs), std::end(ast_column_ptrs),
                [&se](const irods::experimental::api::genquery::column* _p)
                {
                    return _p->name == se.column;
                });

            if (std::end(ast_column_ptrs) == ast_iter) {
                throw std::invalid_argument{"cannot generate SQL from General Query."};
            }

            if ((*ast_iter)->type_name.empty()) {
                sort_expr.push_back(fmt::format("{}.{} {}",
                                                alias,
                                                iter->second.name,
                                                se.ascending_order ? "asc" : "desc"));
            }
            else {
                sort_expr.push_back(fmt::format("cast({}.{} as {}) {}",
                                                alias,
                                                iter->second.name,
                                                (*ast_iter)->type_name,
                                                se.ascending_order ? "asc" : "desc"));
            }
        }

        // All columns in the order by clause must exist in the list of columns to project.
        return fmt::format(" order by {}", fmt::join(sort_expr, ", "));
    } // generate_order_by_clause
} // anonymous namespace

namespace irods::experimental::api::genquery
{
    bool in_select_clause = false;

    bool add_joins_for_meta_data = false;
    bool add_joins_for_meta_coll = false;
    bool add_joins_for_meta_resc = false;
    bool add_joins_for_meta_user = false;

    bool add_joins_for_data_perms = false;
    bool add_joins_for_coll_perms = false;

    bool add_sql_for_data_resc_hier = false;

    bool requested_resc_hier = false;

    // Holds pointers to column objects which contain SQL CAST syntax.
    // These pointers allow the parser to forward the SQL CAST text to the final output.
    std::vector<const column*> ast_column_ptrs;

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

    auto setup_column_for_post_processing(const column& _column, const column_info& _column_info) -> std::tuple<bool, std::string_view>
    {
        auto is_special_column = true;
        std::string_view table_alias_for_special_column;

        auto add_r_data_main = false;
        auto add_r_coll_main = false;
        auto add_r_user_main = false;
        auto add_r_resc_main = false;

        // The columns that trigger these branches rely on special joins and therefore must use pre-defined
        // table aliases.
        if (_column.name.starts_with("META_D")) {
            add_r_data_main = add_joins_for_meta_data = true;
            table_alias_for_special_column = "mmd";
        }
        else if (_column.name.starts_with("META_C")) {
            add_r_coll_main = add_joins_for_meta_coll = true;
            table_alias_for_special_column = "mmc";
        }
        else if (_column.name.starts_with("META_R")) {
            add_r_resc_main = add_joins_for_meta_resc = true;
            table_alias_for_special_column = "mmr";
        }
        else if (_column.name.starts_with("META_U")) {
            add_r_user_main = add_joins_for_meta_user = true;
            table_alias_for_special_column = "mmu";
        }
        else if (_column.name.starts_with("DATA_ACCESS_")) {
            // There are three tables which are secretly joined to satisfy columns that trigger this branch.
            // The columns require special hard-coded table aliases to work properly. This is because the joins
            // cannot be worked out using only the graph.
            if (_column.name == "DATA_ACCESS_PERM_NAME") {
                add_r_data_main = add_joins_for_data_perms = true;
                table_alias_for_special_column = "pdt"; // The alias for R_TOKN_MAIN as it relates to data objects.
            }
            else if (_column.name == "DATA_ACCESS_USER_NAME") {
                add_r_data_main = add_joins_for_data_perms = true;
                table_alias_for_special_column = "pdu"; // The alias for R_USER_MAIN as it relates to data objects.
            }
            else {
                add_r_data_main = add_joins_for_data_perms = true;
                table_alias_for_special_column = "pdoa"; // The alias for R_OBJT_ACCESS as it relates to data objects.
            }
        }
        else if (_column.name.starts_with("COLL_ACCESS_")) {
            // There are three tables which are secretly joined to satisfy columns that trigger this branch.
            // The columns require special hard-coded table aliases to work properly. This is because the joins
            // cannot be worked out using only the graph.
            if (_column.name == "COLL_ACCESS_PERM_NAME") {
                add_r_coll_main = add_joins_for_coll_perms = true;
                table_alias_for_special_column = "pct"; // The alias for R_TOKN_MAIN as it relates to collections.
            }
            else if (_column.name == "COLL_ACCESS_USER_NAME") {
                add_r_coll_main = add_joins_for_coll_perms = true;
                table_alias_for_special_column = "pcu"; // The alias for R_USER_MAIN as it relates to collections.
            }
            else {
                add_r_coll_main = add_joins_for_coll_perms = true;
                table_alias_for_special_column = "pcoa"; // The alias for R_OBJT_ACCESS as it relates to collections.
            }
        }
        else if (_column.name == "DATA_RESC_HIER") {
            add_r_resc_main = add_sql_for_data_resc_hier = true;
            table_alias_for_special_column = "T";
        }
        else {
            is_special_column = false;
        }

        if (!contains(sql_tables, _column_info.table)) {
            // Special columns such as the general query metadata columns are handled separately
            // because they require multiple table joins. For this reason, we don't allow any of those
            // tables to be added to the table list.
            if (is_special_column) {
                if (add_r_data_main) {
                    if (!contains(sql_tables, "R_DATA_MAIN")) {
                        sql_tables.emplace_back("R_DATA_MAIN");
                        table_aliases["R_DATA_MAIN"] = generate_table_alias();
                    }
                }
                else if (add_r_coll_main) {
                    if (!contains(sql_tables, "R_COLL_MAIN")) {
                        sql_tables.emplace_back("R_COLL_MAIN");
                        table_aliases["R_COLL_MAIN"] = generate_table_alias();
                    }
                }
                else if (add_r_resc_main) {
                    if (!contains(sql_tables, "R_RESC_MAIN")) {
                        sql_tables.emplace_back("R_RESC_MAIN");
                        table_aliases["R_RESC_MAIN"] = generate_table_alias();
                    }
                }
                else if (add_r_user_main) {
                    if (!contains(sql_tables, "R_USER_MAIN")) {
                        sql_tables.emplace_back("R_USER_MAIN");
                        table_aliases["R_USER_MAIN"] = generate_table_alias();
                    }
                }
            }
            else {
                sql_tables.push_back(_column_info.table);
                table_aliases[std::string{_column_info.table}] = generate_table_alias();
            }
        }

        return {is_special_column, table_alias_for_special_column};
    } // setup_column_for_post_processing

    std::string sql(const column& column)
    {
        const auto iter = column_name_mappings.find(column.name);
        
        if (iter == std::end(column_name_mappings)) {
            throw std::invalid_argument{fmt::format("unknown column: {}", column.name)};
        }

        // Capture all column objects as some parts of the implementation need to access them in
        // order to generate the proper SQL.
        ast_column_ptrs.push_back(&column);

        auto [is_special_column, table_alias_for_special_column] = setup_column_for_post_processing(column, iter->second);
        auto* columns_ptr = in_select_clause ? &columns_for_select_clause : &columns_for_where_clause;
        const std::string_view alias = is_special_column ? table_alias_for_special_column : table_aliases.at(std::string{iter->second.table});

        if (column.type_name.empty()) {
            columns_ptr->push_back(fmt::format("{}.{}", alias, iter->second.name));
        }
        else {
            columns_ptr->push_back(fmt::format("cast({}.{} as {})", alias, iter->second.name, column.type_name));
        }

        return columns_ptr->back();
    }

    std::string sql(const select_function& select_function)
    {
        const auto iter = column_name_mappings.find(select_function.column.name);
        
        if (iter == std::end(column_name_mappings)) {
            throw std::invalid_argument{fmt::format("unknown column: {}", select_function.column.name)};
        }

        // Capture all column objects as some parts of the implementation need to access them in
        // order to generate the proper SQL.
        ast_column_ptrs.push_back(&select_function.column);

        if (!in_select_clause) {
            throw std::invalid_argument{"aggregate functions not allowed in where clause"};
        }

        auto [is_special_column, table_alias_for_special_column] = setup_column_for_post_processing(select_function.column, iter->second);

        // Aggregate functions are not allowed in the WHERE clause of an SQL statement!
        auto* columns_ptr = &columns_for_select_clause;
        const std::string_view alias = is_special_column ? table_alias_for_special_column : table_aliases.at(std::string{iter->second.table});

        if (select_function.column.type_name.empty()) {
            columns_ptr->push_back(fmt::format("{}({}.{})", select_function.name, alias, iter->second.name));
        }
        else {
            columns_ptr->push_back(fmt::format("{}(cast({}.{} as {}))", select_function.name, alias, iter->second.name, select_function.column.type_name));
        }

        return columns_ptr->back();
    }

    std::string sql(const selections& selections)
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

    std::string sql(const condition_operator_not& op_not)
    {
        return fmt::format(" not{}", boost::apply_visitor(sql_visitor(), op_not.expression));
    }

    std::string sql(const condition_not_equal& not_equal)
    {
        values.push_back(not_equal.string_literal);
        return " != ?";
    }

    std::string sql(const condition_equal& equal)
    {
        values.push_back(equal.string_literal);
        return " = ?";
    }

    std::string sql(const condition_less_than& less_than)
    {
        values.push_back(less_than.string_literal);
        return " < ?";
    }

    std::string sql(const condition_less_than_or_equal_to& less_than_or_equal_to)
    {
        values.push_back(less_than_or_equal_to.string_literal);
        return " <= ?";
    }

    std::string sql(const condition_greater_than& greater_than)
    {
        values.push_back(greater_than.string_literal);
        return " > ?";
    }

    std::string sql(const condition_greater_than_or_equal_to& greater_than_or_equal_to)
    {
        values.push_back(greater_than_or_equal_to.string_literal);
        return " >= ?";
    }

    std::string sql(const condition_between& between)
    {
        values.push_back(between.low);
        values.push_back(between.high);
        return " between ? and ?";
    }

    std::string sql(const condition_in& in)
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

    std::string sql(const condition_like& like)
    {
        values.push_back(like.string_literal);
        return " like ?";
    }

    std::string sql(const condition_is_null&)
    {
        return " is null";
    }

    std::string sql(const condition_is_not_null&)
    {
        return " is not null";
    }

    std::string sql(const condition& condition)
    {
        return fmt::format("{}{}", sql(condition.column), boost::apply_visitor(sql_visitor(), condition.expression));
    }

    std::string sql(const conditions& conditions)
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

    std::string sql(const logical_not& condition)
    {
        return fmt::format("not {}", sql(condition.condition));
    }

    std::string sql(const logical_grouping& condition)
    {
        return fmt::format("({})", sql(condition.conditions));
    }

    std::string sql(const select& select, const options& _opts)
    {
        try {
            fmt::print("### PHASE 1: Gather\n\n");

            table_alias_id = 0; // Reset so we never exhaust the range.

            // Extract tables and columns from general query statement.
            sql(select.selections);

            // Convert the conditions of the general query statement into SQL with prepared
            // statement placeholders.
            const auto conds = sql(select.conditions);
            fmt::print("CONDITIONS = {}\n\n", conds);

            if (sql_tables.empty()) {
                return "";
            }

            {
                std::sort(std::begin(ast_column_ptrs), std::end(ast_column_ptrs));
                auto end = std::end(ast_column_ptrs);
                ast_column_ptrs.erase(std::unique(std::begin(ast_column_ptrs), end), end);
            }

            // TODO Optimizations - Cache the SQL for multiple runs of the same GenQuery string.

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

            fmt::print("requires metadata table joins for R_DATA_MAIN? {}\n", add_joins_for_meta_data);
            fmt::print("requires metadata table joins for R_COLL_MAIN? {}\n", add_joins_for_meta_coll);
            fmt::print("requires metadata table joins for R_RESC_MAIN? {}\n", add_joins_for_meta_resc);
            fmt::print("requires metadata table joins for R_USER_MAIN? {}\n", add_joins_for_meta_user);
            fmt::print("\n");

            fmt::print("requires table joins for DATA_RESC_HIER? {}\n", add_sql_for_data_resc_hier);
            fmt::print("\n");

            //
            // Generate SQL
            //

            fmt::print("\n### PHASE 2: SQL Generation\n\n");

            graph_type graph{table_edges.data(),
                             table_edges.data() + table_edges.size(),
                             table_names.size()};

            for (auto [iter, last] = boost::vertices(graph); iter != last; ++iter) {
                graph[*iter].table_name = table_names[*iter];
            }

            for (auto [iter, last] = boost::edges(graph); iter != last; ++iter) {
                graph[*iter] = table_joins[table_edges.size() - std::distance(iter, last)]; // TODO
            }

            // Generate the SELECT clause.
            //
            // TODO Use Boost.Graph to resolve table joins for the SELECT clause.
            // This step does not concern itself with special columns (e.g. META_DATA_ATTR_NAME). Those
            // will handled in a later step.
            //
            // The tables stored in sql_tables must be directly joinable to at least one other table in
            // the sql_tables list. This step is NOT allowed to introduce intermediate tables.
            auto select_clause = fmt::format("{with_clause}select {distinct}{columns} from {table} {alias}",
                                             fmt::arg("with_clause", add_sql_for_data_resc_hier ? data_resc_hier_with_clause : ""),
                                             fmt::arg("distinct", select.distinct ? "distinct " : ""),
                                             fmt::arg("columns", fmt::join(columns_for_select_clause, ", ")),
                                             fmt::arg("table", sql_tables.front()),
                                             fmt::arg("alias", table_aliases.at(std::string{sql_tables.front()})));

            fmt::print("SELECT CLAUSE => {}\n", select_clause);
            fmt::print("\n");

            const auto inner_joins = generate_inner_joins(graph, sql_tables, table_aliases);

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

            // TODO Handle tickets.
            // Q. Should tickets be scoped to data objects and collections separately?
            // Q. What happens if a user attempts to query data objects, collections, and tickets in the same query?
            // Q. Should these questions be handled by specific queries instead?

            // Permission checking.
            // Always include the joins if the query involves columns related to data objects and/or collections.
            // This is required due to how columns in R_OBJT_ACCESS and other tables are handled.
            /*
                select d.*
                from R_DATA_MAIN d
                inner join R_COLL_MAIN c on doa.object_id = d.data_id
                inner join R_OBJT_ACCESS doa on doa.object_id = d.data_id
                inner join R_OBJT_ACCESS coa on coa.object_id = c.coll_id
                inner join R_USER_MAIN du on du.user_id = doa.user_id
                inner join R_USER_MAIN cu on cu.user_id = coa.user_id
                where doa.access_type_id >= ? and
                      coa.access_type_id >= ?
             */
            if (const auto iter = table_aliases.find("R_DATA_MAIN"); iter != std::end(table_aliases)) {
                sql += fmt::format(" inner join R_OBJT_ACCESS pdoa on {}.data_id = pdoa.object_id"
                                   " inner join R_TOKN_MAIN pdt on pdoa.access_type_id = pdt.token_id"
                                   " inner join R_USER_MAIN pdu on pdoa.user_id = pdu.user_id",
                                   iter->second);
            }

            if (const auto iter = table_aliases.find("R_COLL_MAIN"); iter != std::end(table_aliases)) {
                sql += fmt::format(" inner join R_OBJT_ACCESS pcoa on {}.coll_id = pcoa.object_id"
                                   " inner join R_TOKN_MAIN pct on pcoa.access_type_id = pct.token_id"
                                   " inner join R_USER_MAIN pcu on pcoa.user_id = pcu.user_id",
                                   iter->second);
            }

            // TODO Handle situations where columns in the R_OBJT_ACCESS table are requested.
            // That also means handling R_TOKN_NAMESPACE for permission names (rather than permission integers).
            //
            // For example:
            //
            //      select DATA_NAME, COLL_NAME, DATA_ACCESS_TYPE, COLL_ACCESS_NAME
            //
            // This should result in the following tables:
            //
            //      - R_DATA_MAIN
            //      - R_COLL_MAIN
            //      - R_OBJT_ACCESS
            //      - R_TOKN_MAIN
            //          - condition: token_namespace = 'access_type' and token_id = <integer>
            //          - column of interest: token_name
            //
            // NOTE: Keep in mind that the joins for admins vs non-admins must be handled as well. That means the
            // example query above needs to work when admins run the query too. Remember, the joins introduced
            // for non-admins will not be included when an admin runs the GenQuery. Therefore, the results can
            // change. One way around this is to change the integer value to 0 or the lowest permission integer
            // when an admin runs the query. That way, the behavior remains the same for all users.

            sql += generate_joins_for_metadata_columns(table_aliases,
                                                       add_joins_for_meta_data,
                                                       add_joins_for_meta_coll,
                                                       add_joins_for_meta_resc,
                                                       add_joins_for_meta_user);

            if (add_sql_for_data_resc_hier) {
                sql += fmt::format(" inner join T on T.resc_id = {}.resc_id", table_aliases.at("R_RESC_MAIN"));
            }

            if (!conds.empty()) {
                sql += fmt::format(" where {}", conds);

                const auto min_perm_level = _opts.admin_mode ? 1000 : 1050;

                // TODO Permission checking.
                if (_opts.admin_mode) {
                    /*
                        select d.*
                        from R_DATA_MAIN d
                        inner join R_COLL_MAIN c on doa.object_id = d.data_id
                        inner join R_OBJT_ACCESS doa on doa.object_id = d.data_id
                        inner join R_OBJT_ACCESS coa on coa.object_id = c.coll_id
                        inner join R_USER_MAIN du on du.user_id = doa.user_id
                        inner join R_USER_MAIN cu on cu.user_id = coa.user_id
                        where doa.access_type_id >= ? and du.user_name = ? and
                              coa.access_type_id >= ? and cu.user_name = ?
                     */
                    const auto d_iter = table_aliases.find("R_DATA_MAIN");
                    const auto c_iter = table_aliases.find("R_COLL_MAIN");
                    const auto end = std::end(table_aliases);

                    if (d_iter != end && c_iter != end) {
                        sql += fmt::format(" and pdoa.access_type_id >= {perm} and pcoa.access_type_id >= {perm}",
                                           fmt::arg("perm", min_perm_level));
                    }
                    else if (d_iter != end) {
                        sql += fmt::format(" and pdoa.access_type_id >= {}", min_perm_level);
                    }
                    else if (c_iter != end) {
                        sql += fmt::format(" and pcoa.access_type_id >= {}", min_perm_level);
                    }
                }
                else {
                    /*
                        select d.*
                        from R_DATA_MAIN d
                        inner join R_COLL_MAIN c on doa.object_id = d.data_id
                        inner join R_OBJT_ACCESS doa on doa.object_id = d.data_id
                        inner join R_OBJT_ACCESS coa on coa.object_id = c.coll_id
                        inner join R_USER_MAIN du on du.user_id = doa.user_id
                        inner join R_USER_MAIN cu on cu.user_id = coa.user_id
                        where doa.access_type_id >= ? and du.user_name = ? and
                              coa.access_type_id >= ? and cu.user_name = ?
                     */
                    const auto d_iter = table_aliases.find("R_DATA_MAIN");
                    const auto c_iter = table_aliases.find("R_COLL_MAIN");
                    const auto end = std::end(table_aliases);

                    if (d_iter != end && c_iter != end) {
                        sql += fmt::format(" and pdu.user_name = ? and pcu.user_name = ? and"
                                           " pdoa.access_type_id >= {perm} and pcoa.access_type_id >= {perm}",
                                           fmt::arg("perm", min_perm_level));
                        values.push_back(std::string{_opts.username});
                        values.push_back(std::string{_opts.username});
                    }
                    else if (d_iter != end) {
                        sql += fmt::format(" and pdu.user_name = ? and pdoa.access_type_id >= {}", min_perm_level);
                        values.push_back(std::string{_opts.username});
                    }
                    else if (c_iter != end) {
                        sql += fmt::format(" and pcu.user_name = ? and pcoa.access_type_id >= {}", min_perm_level);
                        values.push_back(std::string{_opts.username});
                    }
                }
            }
            else {
                const auto min_perm_level = _opts.admin_mode ? 1000 : 1050;

                // TODO Permission checking.
                if (_opts.admin_mode) {
                    /*
                        select d.*
                        from R_DATA_MAIN d
                        inner join R_COLL_MAIN c on doa.object_id = d.data_id
                        inner join R_OBJT_ACCESS doa on doa.object_id = d.data_id
                        inner join R_OBJT_ACCESS coa on coa.object_id = c.coll_id
                        inner join R_USER_MAIN du on du.user_id = doa.user_id
                        inner join R_USER_MAIN cu on cu.user_id = coa.user_id
                        where doa.access_type_id >= ? and du.user_name = ? and
                              coa.access_type_id >= ? and cu.user_name = ?
                     */
                    const auto d_iter = table_aliases.find("R_DATA_MAIN");
                    const auto c_iter = table_aliases.find("R_COLL_MAIN");
                    const auto end = std::end(table_aliases);

                    if (d_iter != end && c_iter != end) {
                        sql += fmt::format(" where pdoa.access_type_id >= {perm} and pcoa.access_type_id >= {perm}",
                                           fmt::arg("perm", min_perm_level));
                    }
                    else if (d_iter != end) {
                        sql += fmt::format(" where pdoa.access_type_id >= {}", min_perm_level);
                    }
                    else if (c_iter != end) {
                        sql += fmt::format(" where pcoa.access_type_id >= {}", min_perm_level);
                    }
                }
                else {
                    /*
                        select d.*
                        from R_DATA_MAIN d
                        inner join R_COLL_MAIN c on doa.object_id = d.data_id
                        inner join R_OBJT_ACCESS doa on doa.object_id = d.data_id
                        inner join R_OBJT_ACCESS coa on coa.object_id = c.coll_id
                        inner join R_USER_MAIN du on du.user_id = doa.user_id
                        inner join R_USER_MAIN cu on cu.user_id = coa.user_id
                        where doa.access_type_id >= ? and du.user_name = ? and
                              coa.access_type_id >= ? and cu.user_name = ?
                     */
                    const auto d_iter = table_aliases.find("R_DATA_MAIN");
                    const auto c_iter = table_aliases.find("R_COLL_MAIN");
                    const auto end = std::end(table_aliases);

                    if (d_iter != end && c_iter != end) {
                        sql += fmt::format(" where pdu.user_name = ? and pcu.user_name = ?"
                                           " and pdoa.access_type_id >= {perm} and pcoa.access_type_id >= {perm}",
                                           fmt::arg("perm", min_perm_level));
                        values.push_back(std::string{_opts.username});
                        values.push_back(std::string{_opts.username});
                    }
                    else if (d_iter != end) {
                        sql += fmt::format(" where pdu.user_name = ? and pdoa.access_type_id >= {}", min_perm_level);
                        values.push_back(std::string{_opts.username});
                    }
                    else if (c_iter != end) {
                        sql += fmt::format(" where pcu.user_name = ? and pcoa.access_type_id >= {}", min_perm_level);
                        values.push_back(std::string{_opts.username});
                    }
                }
            }

            sql += generate_order_by_clause(select.order_by, table_aliases, column_name_mappings, ast_column_ptrs);

            if (!select.range.offset.empty()) {
                sql += fmt::format(" offset {}", select.range.offset);
            }

            if (!select.range.number_of_rows.empty()) {
                sql += fmt::format(" fetch first {} rows only", select.range.number_of_rows);
            }

            std::for_each(std::begin(values), std::end(values), [](auto&& _j) {
                fmt::print("BINDABLE VALUE => {}\n", _j);
            });
            fmt::print("\n");

            fmt::print("GENERATED SQL => [{}]\n", sql);

            return sql;
        }
        catch (const std::exception& e) {
            fmt::print(stderr, "Exception: {}\n", e.what());
        }

        return "";
    }

    auto get_bind_values() noexcept -> std::vector<std::string>&
    {
        return values;
    }
} // namespace irods::experimental::api::genquery

/*
    R_OBJT_ACCESS: contains mappings between data objects/collections and users/groups.
        - object_id: data_id or coll_id
        - user_id: user or group id
        - access_type_id: permission level (integer)

    select d.*
    from R_DATA_MAIN d
    inner join R_COLL_MAIN c on doa.object_id = d.data_id
    inner join R_OBJT_ACCESS doa on doa.object_id = d.data_id
    inner join R_OBJT_ACCESS coa on coa.object_id = c.coll_id
    inner join R_USER_MAIN du on du.user_id = doa.user_id
    inner join R_USER_MAIN cu on cu.user_id = coa.user_id
    where doa.access_type_id >= ? and du.user_name = ? and
          coa.access_type_id >= ? and cu.user_name = ?


    The following query will produce a resource hierarchy starting from a leaf resource ID.
    Keep in mind that the ::<type> syntax may be specific to PostgreSQL. Remember to check the
    other database systems for compatibility.

        with recursive T as (
            select
                resc_id,
                resc_name hier,
                case
                    when resc_parent = '' then 0
                    else resc_parent::bigint
                end parent_id
            from
                r_resc_main
            where
                resc_id > 0     -- Or, resc_id = <child_resc_id>

            union all

            select
                T.resc_id,
                (U.resc_name || ';' || T.hier)::varchar(250),
                case
                    when U.resc_parent = '' then 0
                    else U.resc_parent::bigint
                end parent_id
            from T
            inner join r_resc_main U on U.resc_id = T.parent_id
        )
        select resc_id, hier from T where parent_id = 0 and resc_id = <resc_id>;

    Q. Can this be used with other queries?
    A. Yes! CTEs can be joined just like any other table.

    Q. What tables need to be joined in order to support this?
    A. R_RESC_MAIN is the only table that is needed.
 */
