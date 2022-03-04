#include "genquery_ast_types.hpp"

#include "table_column_key_maps.hpp"

#include <fmt/format.h>

#include <iostream>
#include <stdexcept>

namespace irods::experimental::api::genquery
{
    //using log = irods::experimental::log;

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

    auto no_distinct_flag = false;

    std::vector<std::string> columns;
    std::vector<std::string> tables;
    std::vector<std::string> from_aliases;
    std::vector<std::string> where_clauses;
    std::vector<std::string> processed_tables;

    std::string sql(const Column& column)
    {
        const auto iter = column_name_mappings.find(column.name);
        
        if (iter != std::end(column_name_mappings)) {
            columns.push_back(fmt::format("{}.{}", iter->second.table, iter->second.name));
            tables.push_back(iter->second.table);
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
            auto sel = boost::apply_visitor(sql_visitor{}, selection);
        }

        return ret;
    }

    std::string sql(const ConditionOperator_Or& op_or)
    {
        std::string ret;
        ret += boost::apply_visitor(sql_visitor(), op_or.left);
        ret += " OR ";
        ret += boost::apply_visitor(sql_visitor(), op_or.right);
        return ret;
    }

    std::string sql(const ConditionOperator_And& op_and)
    {
        std::string ret = boost::apply_visitor(sql_visitor(), op_and.left);
        ret += " AND";
        ret += boost::apply_visitor(sql_visitor(), op_and.right);
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
                tables.push_back(iter->second.table);
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

