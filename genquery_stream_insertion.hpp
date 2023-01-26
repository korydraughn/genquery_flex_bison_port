#ifndef IRODS_GENQUERY_STREAM_INSERTION_HPP
#define IRODS_GENQUERY_STREAM_INSERTION_HPP

#include "genquery_ast_types.hpp"

namespace irods::experimental::api::genquery
{
    template <typename T>
    T& operator<<(T& os, const Column& column)
    {
        return os << column.name;
    }

    template <typename T>
    T& operator<<(T& os, const SelectFunction& select_function)
    {
        return os << select_function.name << "(" << select_function.column << ")";
    }

    template <typename T>
    T& operator<<(T& os, const Selections& selections)
    {
        os << "select ";

        for (auto it = selections.cbegin(); it != selections.cend(); ++it) {
            os << *it;

            if (it + 1 != selections.cend()) {
                os << ", ";
            }
        }

        return os;
    }

    template <typename T>
    T& operator<<(T& os, const ConditionOperator_Not& op_not)
    {
        return os << "not " << op_not.expression;
    }

    template <typename T>
    T& operator<<(T& os, const ConditionNotEqual& not_equal)
    {
        return os << "!= '" << not_equal.string_literal << "'";
    }

    template <typename T>
    T& operator<<(T& os, const ConditionEqual& equal)
    {
        return os << "= '" << equal.string_literal << "'";
    }

    template <typename T>
    T& operator<<(T& os, const ConditionLessThan& less_than)
    {
        return os << "< '" << less_than.string_literal << "'";
    }

    template <typename T>
    T& operator<<(T& os, const ConditionLessThanOrEqualTo& less_than_or_equal_to)
    {
        return os << "<= '" << less_than_or_equal_to.string_literal << "'";
    }

    template <typename T>
    T& operator<<(T& os, const ConditionGreaterThan& greater_than)
    {
        return os << "> '" << greater_than.string_literal << "'";
    }

    template <typename T>
    T& operator<<(T& os, const ConditionGreaterThanOrEqualTo& greater_than_or_equal_to)
    {
        return os << ">= '" << greater_than_or_equal_to.string_literal << "'";
    }

    template <typename T>
    T& operator<<(T& os, const ConditionBetween& between)
    {
        return os << "between "
                  << "'" << between.low << "'"
                  << " "
                  << "'" << between.high << "'";
    }

    template <typename T>
    T& operator<<(T& os, const ConditionIn& in)
    {
        os << "in (";

        for (auto it = in.list_of_string_literals.cbegin(); it != in.list_of_string_literals.cend(); ++it) {
            os << "'" << *it << "'";
            if (it+1 != in.list_of_string_literals.cend()) {
                os << ", ";
            }
        }

        return os << ")";
    }

    template <typename T>
    T& operator<<(T& os, const ConditionLike& like)
    {
        return os << "like '" << like.string_literal << "'";
    }

    template <typename T>
    T& operator<<(T& os, const Condition& condition)
    {
        return os << condition.column << " " << condition.expression;
    }

    template <typename T>
    T& operator<<(T& os, const Conditions& conditions)
    {
        for (auto it = conditions.cbegin(); it != conditions.cend(); ++it) {
            os << *it;
            if (it + 1 != conditions.cend()) {
                os << " and ";
            }
        }

        return os;
    }

    template <typename T>
    T& operator<<(T& os, const Select& select)
    {
        os << select.selections;

        if (!select.conditions.empty()) {
            os << " where ";
            os << select.conditions;
        }

        return os;
    }
} // namespace irods::experimental::api::genquery

#endif // IRODS_GENQUERY_STREAM_INSERTION_HPP
