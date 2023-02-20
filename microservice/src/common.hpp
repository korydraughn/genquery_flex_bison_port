#ifndef IRODS_MICROSERVICE_GENQUERY2_COMMON_HPP
#define IRODS_MICROSERVICE_GENQUERY2_COMMON_HPP

#include <nlohmann/json.hpp>

#include <cstdint>

struct genquery2_context
{
    void* storage;
    nlohmann::json* rows;
    std::int32_t row_index; 
}; // struct genquery2_context

#define GENQUERY2_CONTEXT_PI ""

#endif // IRODS_MICROSERVICE_GENQUERY2_COMMON_HPP
