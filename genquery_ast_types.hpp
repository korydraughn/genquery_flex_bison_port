#ifndef IRODS_GENQUERY_AST_TYPES_HPP
#define IRODS_GENQUERY_AST_TYPES_HPP

#include <boost/variant.hpp>

#include <string>
#include <utility>
#include <vector>

namespace irods::experimental::api::genquery
{
    struct Column {
        Column() = default;
        explicit Column(std::string name)
            : name{std::move(name)} {}
        std::string name;
    };

    struct SelectFunction {
        SelectFunction() = default;
        SelectFunction(std::string name, Column column)
            : name{std::move(name)}, column{std::move(column)} {}
        std::string name;
        Column column;
    };

    struct ConditionLike {
        ConditionLike() = default;
        explicit ConditionLike(std::string string_literal)
            : string_literal{std::move(string_literal)} {}
        std::string string_literal;
    };

    struct ConditionIn {
        ConditionIn() = default;
        explicit ConditionIn(std::vector<std::string> list_of_string_literals)
            : list_of_string_literals{std::move(list_of_string_literals)} {}
        std::vector<std::string> list_of_string_literals;
    };

    struct ConditionBetween {
        ConditionBetween() = default;
        ConditionBetween(std::string low, std::string high)
            : low{std::move(low)}, high{std::move(high)} {}
        std::string low;
        std::string high;
    };

    struct ConditionEqual {
        ConditionEqual() = default;
        explicit ConditionEqual(std::string string_literal)
            : string_literal{std::move(string_literal)} {}
        std::string string_literal;
    };

    struct ConditionNotEqual {
        ConditionNotEqual() = default;
        explicit ConditionNotEqual(std::string string_literal)
            : string_literal{std::move(string_literal)} {}
        std::string string_literal;
    };

    struct ConditionLessThan {
        ConditionLessThan() = default;
        explicit ConditionLessThan(std::string string_literal)
            : string_literal{std::move(string_literal)} {}
        std::string string_literal;
    };

    struct ConditionLessThanOrEqualTo {
        ConditionLessThanOrEqualTo() = default;
        explicit ConditionLessThanOrEqualTo(std::string string_literal)
            : string_literal{std::move(string_literal)} {}
        std::string string_literal;
    };

    struct ConditionGreaterThan {
        ConditionGreaterThan() = default;
        explicit ConditionGreaterThan(std::string string_literal)
            : string_literal{std::move(string_literal)} {}
        std::string string_literal;
    };

    struct ConditionGreaterThanOrEqualTo {
        ConditionGreaterThanOrEqualTo() = default;
        explicit ConditionGreaterThanOrEqualTo(std::string string_literal)
            : string_literal{std::move(string_literal)} {}
        std::string string_literal;
    };

    struct ConditionIsNull
    {
    };

    struct ConditionIsNotNull
    {
    };

    struct ConditionOperator_Not;

    using ConditionExpression = boost::variant<ConditionLike,
                                               ConditionIn,
                                               ConditionBetween,
                                               ConditionEqual,
                                               ConditionNotEqual,
                                               ConditionLessThan,
                                               ConditionLessThanOrEqualTo,
                                               ConditionGreaterThan,
                                               ConditionGreaterThanOrEqualTo,
                                               ConditionIsNull,
                                               ConditionIsNotNull,
                                               boost::recursive_wrapper<ConditionOperator_Not>>;

    struct ConditionOperator_Not {
        ConditionOperator_Not() = default;
        ConditionOperator_Not(ConditionExpression expression)
            : expression{std::move(expression)} {}
        ConditionExpression expression;
    };

    struct Condition {
        Condition() = default;
        Condition(Column column, ConditionExpression expression)
            : column{std::move(column)}, expression{std::move(expression)} {}
        Column column;
        ConditionExpression expression;
    };

    struct logical_and;
    struct logical_or;
    struct logical_grouping;

    using condition_type = boost::variant<logical_and,
                                          logical_or,
                                          logical_grouping,
                                          Condition>;

    // clang-format off
    using Selection     = boost::variant<SelectFunction, Column>;
    using Selections    = std::vector<Selection>;
    using Conditions    = std::vector<condition_type>;
    //using order_by_type = std::vector<std::string>;
    // clang-format on

    struct logical_and
    {
        Conditions condition;
    };

    struct logical_or
    {
        Conditions condition;
    };

    struct logical_grouping
    {
        Conditions conditions;
    };

    struct sort_expression
    {
        std::string column;
        bool ascending_order = true;
    };

    struct order_by
    {
        std::vector<sort_expression> sort_expressions;
    }; // struct order_by

    struct range
    {
        std::string offset;
        std::string number_of_rows;
    }; // struct range

    struct Select
    {
        Select() = default;

        Select(Selections selections, Conditions conditions)
            : selections(std::move(selections))
            , conditions(std::move(conditions))
        {
        }

        Selections selections;
        Conditions conditions;
        order_by order_by;
        range range;
        bool distinct = true;
    };
} // namespace irods::experimental::api::genquery

#endif // IRODS_GENQUERY_AST_TYPES_HPP

