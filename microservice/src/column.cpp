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
    auto msi_impl(MsParam* _context, MsParam* _column_index, MsParam* _value, ruleExecInfo_t* _rei) -> int
    {
        using log_msi = irods::experimental::log::microservice;

        log_msi::info("MSI_GENQUERY2_COLUMN");

        // Check input parameters.
        if (!_context || !_column_index || ! _value || !_rei) { // NOLINT(readability-implicit-bool-conversion)
            log_msi::error("At least one input argument is null.");
            return SYS_INTERNAL_NULL_INPUT_ERR;
        }
#if 0
        log_msi::info("Casting void pointer to genquery2_context pointer ...");
        const auto* ctx = static_cast<genquery2_context*>(_context->inOutStruct);

        log_msi::info("Parsing msi param into integer (column index) ...");
        const auto cindex = parseMspForPosInt(_column_index);

        log_msi::info("Checking if column index is out of bounds ...");
        if (cindex < 0) {
            log_msi::error("Invalid column index: must be greater than or equal to zero.");
            return SYS_INVALID_INPUT_PARAM;
        }

        if (cindex >= static_cast<decltype(cindex)>(ctx->rows->size())) {
            log_msi::error("Invalid column index: exceeds row count.");
            return SYS_INVALID_INPUT_PARAM;
        }

        log_msi::info("Column index is in bounds. Copying column value into output msi param (value) ...");
        _value->type = strdup(STR_MS_T);
        _value->inOutStruct = strdup(ctx->rows->at(ctx->row_index).at(cindex).get_ref<const std::string&>().c_str());
#else
        _value->type = strdup(STR_MS_T);
        _value->inOutStruct = strdup("<test_value>");
        if (std::strcmp(_context->type, STR_MS_T) == 0) {
            log_msi::info("_context->inOutStruct memory address = [{}]", _context->inOutStruct);
            log_msi::info("_context->inOutStruct = [{}]", (char*) _context->inOutStruct);
            //log_msi::info("Appending string to _context->inOutStruct ...");
            //std::strcat((char*) _context->inOutStruct, " Must better than GenQuery1.");
            //log_msi::info("(updated) _context->inOutStruct = [{}]", (char*) _context->inOutStruct);
        }
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
    return make_msi<MsParam*, MsParam*, MsParam*>("msi_genquery2_column", msi_impl);
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
int msi_genquery2_column(MsParam* _handle, ruleExecInfo_t* _rei);
#endif // IRODS_FOR_DOXYGEN
