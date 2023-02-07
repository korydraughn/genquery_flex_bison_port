#include "irods/plugins/api/private/genquery2_common.hpp"
#include "irods/plugins/api/genquery2_common.h" // For API plugin number.

#include <irods/apiHandler.hpp>
#include <irods/catalog_utilities.hpp> // Requires linking against libnanodbc.so
#include <irods/irods_logger.hpp>
//#include <irods/irods_re_serialization.hpp>
#include <irods/procApiRequest.h>
#include <irods/rodsErrorTable.h>

#include "irods/genquery_sql.hpp"
#include "irods/genquery_wrapper.hpp"

#include <fmt/format.h>

#include <cstring> // For strdup.

namespace
{
	using log_api = irods::experimental::log::api;

	//
	// Function Prototypes
	//

	auto call_genquery2(irods::api_entry*, RsComm*, const char*, char**) -> int;

	auto rs_genquery2(RsComm*, const char*, char**) -> int;

	//
	// Function Implementations
	//

	auto call_genquery2(irods::api_entry* _api, RsComm* _comm, const char* _msg, char** _resp) -> int
	{
		return _api->call_handler<const char*, char**>(_comm, _msg, _resp);
	} // call_genquery2

	auto rs_genquery2(RsComm* _comm, const char* _msg, char** _resp) -> int
	{
		if (!_msg || !_resp) {
			log_api::error("Inalid input: received nullptr for message pointer and/or response pointer.");
			return SYS_INVALID_INPUT_PARAM;
		}

		log_api::info("GenQuery 2 API endpoint received: [{}]", _msg);

		// Redirect to the catalog service provider so that we can query the database.
		try {
			namespace ic = irods::experimental::catalog;

			if (!ic::connected_to_catalog_provider(*_comm)) {
				log_api::trace("Redirecting request to catalog service provider.");
				[[maybe_unused]] auto* host_info = ic::redirect_to_catalog_provider(*_comm);

                                return procApiRequest(
                                        host_info->conn,
                                        IRODS_APN_GENQUERY2,
                                        const_cast<char*>(_msg), // NOLINT(cppcoreguidelines-pro-type-const-cast)
                                        nullptr,
                                        reinterpret_cast<void**>(_resp), // NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
                                        nullptr);
			}

			ic::throw_if_catalog_provider_service_role_is_invalid();
		}
		catch (const irods::exception& e) {
			log_api::error(e.what());
			return e.code();
		}

		const auto ast = gq::wrapper::parse(_msg);
                const auto sql = gq::sql(ast);
                log_api::info("Returning to client: [{}]", sql);
		*_resp = strdup(sql.c_str());

		return 0;
	} // rs_genquery2
} //namespace

const operation_type op = rs_genquery2;
auto fn_ptr = reinterpret_cast<funcPtr>(call_genquery2);
