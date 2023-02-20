/// \file

#include "common.hpp"

#include "irods/irods_error.hpp"
#include "irods/irods_logger.hpp"
#include "irods/irods_ms_plugin.hpp"
#include "irods/irods_re_structs.hpp"
#include "irods/msParam.h"
#include "irods/rodsErrorTable.h"

#include "irods/irods_server_api_call.hpp"
#include "irods/plugins/api/genquery2_common.h"

#include <nlohmann/json.hpp>

#include <cstdlib>
#include <cstring>
#include <functional>
#include <string>

namespace
{
    auto msi_impl(MsParam* _context, MsParam* _query, ruleExecInfo_t* _rei) -> int
    {
        using log_msi = irods::experimental::log::microservice;

        log_msi::info("MSI_GENQUERY2_EXECUTE");

        // Check input parameters.
        if (!_context || !_query || !_rei) { // NOLINT(readability-implicit-bool-conversion)
            log_msi::error("At least one input argument is null.");
            return SYS_INTERNAL_NULL_INPUT_ERR;
        }

        const auto* gql = parseMspForStr(_query);

        if (!gql) { // NOLINT(readability-implicit-bool-conversion)
            log_msi::error("Could not parse microservice parameter into a string.");
            return SYS_INVALID_INPUT_PARAM;
        }

        char* results{};

        if (const auto ec = irods::server_api_call(IRODS_APN_GENQUERY2, _rei->rsComm, gql, &results); ec != 0) {
            log_msi::error("Error executing GenQuery 2 query [error_code=[{}]].", ec);
            return ec;
        }

        log_msi::info("GenQuery 2 results => [{}]", results);
#if 0
        log_msi::info("Allocating GenQuery 2 context ...");
        auto* ctx = static_cast<genquery2_context*>(std::malloc(sizeof(genquery2_context)));

        log_msi::info("Allocating storage for JSON object ...");
        ctx->storage = std::malloc(sizeof(nlohmann::json));

        log_msi::info("Constructing JSON object in storage ...");
        ctx->rows = new (ctx->storage) auto{nlohmann::json::parse(results)};

        log_msi::info("Setting row index to -1 ...");
        ctx->row_index = -1;

        log_msi::info("Freeing JSON string returned by API ...");
        std::free(results);

        //auto* ptrs = static_cast<char*>(std::malloc(sizeof(void*) * 2));
        //new (ptrs) void*{ctx};
        //new (ptrs + sizeof(void*)) char*;

        //_context->type = nullptr;
        //_context->inOutStruct = ctx;
#else
        _context->type = strdup(STR_MS_T);
        _context->inOutStruct = std::malloc(sizeof(char) * 1000);
        std::strcpy((char*) _context->inOutStruct, "GenQuery2");
#endif

        return 0;
    } // msi_impl

    template <typename... Args, typename Function>
    auto make_msi(const std::string& name, Function func) -> irods::ms_table_entry*
    {
        auto* msi = new irods::ms_table_entry{sizeof...(Args)}; // NOLINT(cppcoreguidelines-owning-memory) 
        msi->add_operation(name, std::function<int(Args..., ruleExecInfo_t*)>(func));
        return msi;
    } // make_msi
} // namespace (anonymous)

extern "C"
auto plugin_factory() -> irods::ms_table_entry*
{
    return make_msi<MsParam*, MsParam*>("msi_genquery2_execute", msi_impl);
} // plugin_factory

#ifdef IRODS_FOR_DOXYGEN
/// Query the iRODS catalog.
///
/// \param[in]     _context  The GenQuery 2 string to execute.
/// \param[out]    _query The variable that will hold the handle of the executed query.
/// \param[in,out] _rei    A ::RuleExecInfo object that is automatically handled by the
///                        rule engine plugin framework. Users must ignore this parameter.
///
/// \return An integer.
/// \retval 0        On success.
/// \retval Non-zero On failure.
///
/// \since X.Y.Z
int msi_genquery2_execute(MsParam* _context, MsParam* _query, ruleExecInfo_t* _rei);
#endif // IRODS_FOR_DOXYGEN
