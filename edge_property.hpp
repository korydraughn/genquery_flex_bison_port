#ifndef IRODS_GENQUERY_EDGE_PROPERTY_HPP
#define IRODS_GENQUERY_EDGE_PROPERTY_HPP

#include <string_view>

namespace irods::experimental::genquery
{
    struct edge_property
    {
        std::string_view table_join_fmt;
        std::string_view table_join_default; // TODO Remove as it is too early for this.
    };
} // namespace irods::experimental::genquery

#endif // IRODS_GENQUERY_EDGE_PROPERTY_HPP
