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
    auto msi_impl(MsParam* _context, ruleExecInfo_t* _rei) -> int
    {
        using log_msi = irods::experimental::log::microservice;

        log_msi::info("MSI_GENQUERY2_DESTROY");

        // Check input parameters.
        if (!_context || !_rei) { // NOLINT(readability-implicit-bool-conversion)
            log_msi::error("At least one input argument is null.");
            return SYS_INTERNAL_NULL_INPUT_ERR;
        }
#if 0
        log_msi::info("Casting void pointer to genquery2_context pointer ...");
        auto* ctx = static_cast<genquery2_context*>(_context->inOutStruct);

        log_msi::info("Freeing heap-allocated memory ...");
        ctx->rows->nlohmann::json::~json();
        std::free(ctx->storage);
        std::free(ctx);

        log_msi::info("Setting context to nullptr ...");
        _context->inOutStruct = nullptr;
#else
        if (std::strcmp(_context->type, STR_MS_T) == 0) {
            log_msi::info("_context->inOutStruct memory address = [{}]", _context->inOutStruct);
            log_msi::info("_context->inOutStruct = [{}]", (char*) _context->inOutStruct);
        }
#endif
        log_msi::info("Context was destroyed successfully.");
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
    return make_msi<MsParam*>("msi_genquery2_destory", msi_impl);
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
int msi_genquery2_destory(MsParam* _context, MsParam* _query, ruleExecInfo_t* _rei);
#endif // IRODS_FOR_DOXYGEN
