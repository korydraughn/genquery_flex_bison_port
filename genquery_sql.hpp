#ifndef IRODS_GENQUERY_SQL_HPP
#define IRODS_GENQUERY_SQL_HPP

#include "genquery_ast_types.hpp"

#include <string>

namespace irods::experimental::api::genquery
{
    std::string sql(const Select&);
} // namespace irods::experimental::api::genquery

#endif // IRODS_GENQUERY_SQL_HPP
