#ifndef IRODS_GENQUERY_SQL_HPP
#define IRODS_GENQUERY_SQL_HPP

#include "irods/genquery_ast_types.hpp"

#include <string>

namespace irods::experimental::api::genquery
{
    auto sql(const Select& _select_statement) -> std::string;

    auto get_bind_values() noexcept -> std::vector<std::string>&;
} // namespace irods::experimental::api::genquery

#endif // IRODS_GENQUERY_SQL_HPP
