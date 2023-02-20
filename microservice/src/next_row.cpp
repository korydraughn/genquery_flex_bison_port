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

#include <cstdlib>
#include <cstring>
#include <functional>
#include <string>

namespace
{
    auto msi_impl(MsParam* _context, ruleExecInfo_t* _rei) -> int
    {
        using log_msi = irods::experimental::log::microservice;

        log_msi::info("MSI_GENQUERY2_NEXT_ROw");

        // Check input parameters.
        if (!_context || !_rei) { // NOLINT(readability-implicit-bool-conversion)
            log_msi::error("At least one input argument is null.");
            return SYS_INTERNAL_NULL_INPUT_ERR;
        }

        log_msi::info("Casting void pointer to genquery2_context pointer ...");
        log_msi::info("_context->inOutStruct memory address = [{}]", _context->inOutStruct);
#if 0
        auto* ctx = static_cast<genquery2_context*>(_context->inOutStruct);
        
        log_msi::info("Checking if row index is out of bounds ...");
        if (ctx->row_index < static_cast<std::int32_t>(ctx->rows->size()) - 1) {
            log_msi::info("Row index is in bounds. Incrementing row index ...");
            ++ctx->row_index;
            return 0;
        }

        log_msi::info("Row index is out of bounds.");

        return 1;
#else
        if (std::strcmp(_context->type, STR_MS_T) == 0) {
            log_msi::info("_context->inOutStruct = [{}]", (char*) _context->inOutStruct);
            log_msi::info("Appending string to _context->inOutStruct ...");
            std::strcat((char*) _context->inOutStruct, " is good.");
            //log_msi::info("(updated) _context->inOutStruct = [{}]", (char*) _context->inOutStruct);
        }
        return 0;
#endif
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
    return make_msi<MsParam*>("msi_genquery2_next_row", msi_impl);
} // plugin_factory

#ifdef IRODS_FOR_DOXYGEN
/// Iterate to the next row of the GenQuery 2 resultset.
///
/// \param[in]     _handle The GenQuery 2 handle to recently executed query.
/// \param[in,out] _rei    A ::RuleExecInfo object that is automatically handled by the
///                        rule engine plugin framework. Users must ignore this parameter.
///
/// \return An integer.
/// \retval 0        On success.
/// \retval Non-zero On failure.
///
/// \since X.Y.Z
int msi_genquery2_next_row(MsParam* _handle, ruleExecInfo_t* _rei);
#endif // IRODS_FOR_DOXYGEN
