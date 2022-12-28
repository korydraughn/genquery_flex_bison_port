#ifndef IRODS_TABLE_COLUMN_KEY_MAPS_HPP
#define IRODS_TABLE_COLUMN_KEY_MAPS_HPP

#include <map>
#include <string_view>

namespace irods::experimental::api::genquery
{
    struct column_info
    {
        std::string_view table;
        std::string_view name;
        int id = 0;

        auto operator==(const column_info& _rhs) const noexcept -> bool
        {
            return table == _rhs.table && name == _rhs.name && id == _rhs.id;
        }
    }; // struct column_info

    const std::map<std::string_view, column_info> column_name_mappings{
        {"ZONE_ID",          {"R_ZONE_MAIN", "zone_id"}},
        {"ZONE_NAME",        {"R_ZONE_MAIN", "zone_name"}},
        {"ZONE_TYPE",        {"R_ZONE_MAIN", "zone_type_name"}},
        {"ZONE_CONNECTION",  {"R_ZONE_MAIN", "zone_conn_string"}},
        {"ZONE_COMMENT",     {"R_ZONE_MAIN", "r_comment"}},
        {"ZONE_CREATE_TIME", {"R_ZONE_MAIN", "create_ts"}},
        {"ZONE_MODIFY_TIME", {"R_ZONE_MAIN", "modify_ts"}},

        {"USER_ID",          {"R_USER_MAIN", "user_id"}},
        {"USER_NAME",        {"R_USER_MAIN", "user_name"}},
        {"USER_TYPE",        {"R_USER_MAIN", "user_type_name"}},
        {"USER_ZONE",        {"R_USER_MAIN", "zone_name"}},
        {"USER_INFO",        {"R_USER_MAIN", "user_info"}},
        {"USER_COMMENT",     {"R_USER_MAIN", "r_comment"}},
        {"USER_CREATE_TIME", {"R_USER_MAIN", "create_ts"}},
        {"USER_MODIFY_TIME", {"R_USER_MAIN", "modify_ts"}},
        {"USER_AUTH_ID",     {"R_USER_AUTH", "user_id"}},
        {"USER_DN",          {"R_USER_AUTH", "user_auth_name"}},
        {"USER_DN_INVALID",  {"R_USER_MAIN", "r_comment"}}, // For compatibility.

        {"RESC_ID",              {"R_RESC_MAIN", "resc_id"}},
        {"RESC_NAME",            {"R_RESC_MAIN", "resc_name"}},
        {"RESC_ZONE_NAME",       {"R_RESC_MAIN", "zone_name"}},
        {"RESC_TYPE_NAME",       {"R_RESC_MAIN", "resc_type_name"}},
        {"RESC_CLASS_NAME",      {"R_RESC_MAIN", "resc_class_name"}},
        {"RESC_HOSTNAME",        {"R_RESC_MAIN", "resc_net"}}, // Alias for LOC
        {"RESC_VAULT_PATH",      {"R_RESC_MAIN", "resc_def_path "}},
        {"RESC_FREE_SPACE",      {"R_RESC_MAIN", "free_space"}},
        {"RESC_FREE_SPACE_TIME", {"R_RESC_MAIN", "free_space_ts"}},
        {"RESC_INFO",            {"R_RESC_MAIN", "resc_info"}},
        {"RESC_COMMENT",         {"R_RESC_MAIN", "r_comment"}},
        {"RESC_STATUS",          {"R_RESC_MAIN", "resc_status"}},
        {"RESC_CREATE_TIME",     {"R_RESC_MAIN", "create_ts"}},
        {"RESC_MODIFY_TIME",     {"R_RESC_MAIN", "modify_ts "}},
        {"RESC_CHILDREN",        {"R_RESC_MAIN", "resc_children "}},
        {"RESC_CONTEXT",         {"R_RESC_MAIN", "resc_context "}},
        {"RESC_PARENT",          {"R_RESC_MAIN", "resc_parent "}},
        {"RESC_PARENT_CONTEXT",  {"R_RESC_MAIN", "resc_parent_context"}},

        {"DATA_ID",             {"R_DATA_MAIN", "data_id"}},
        {"DATA_COLL_ID",        {"R_DATA_MAIN", "coll_id"}},
        {"DATA_NAME",           {"R_DATA_MAIN", "data_name"}},
        {"DATA_REPL_NUM",       {"R_DATA_MAIN", "data_repl_num"}},
        {"DATA_VERSION",        {"R_DATA_MAIN", "data_version"}},
        {"DATA_TYPE_NAME",      {"R_DATA_MAIN", "data_type_name"}},
        {"DATA_SIZE",           {"R_DATA_MAIN", "data_size"}},
        {"DATA_PATH",           {"R_DATA_MAIN", "data_path"}},
        {"DATA_OWNER_NAME",     {"R_DATA_MAIN", "data_owner_name"}},
        {"DATA_OWNER_ZONE",     {"R_DATA_MAIN", "data_owner_zone"}},
        {"DATA_REPL_STATUS",    {"R_DATA_MAIN", "data_is_dirty"}},
        {"DATA_STATUS",         {"R_DATA_MAIN", "data_status"}},
        {"DATA_CHECKSUM",       {"R_DATA_MAIN", "data_checksum"}},
        {"DATA_EXPIRY",         {"R_DATA_MAIN", "data_expiry_ts"}},
        {"DATA_MAP_ID",         {"R_DATA_MAIN", "data_map_id"}},
        {"DATA_COMMENTS",       {"R_DATA_MAIN", "r_comment"}},
        {"DATA_CREATE_TIME",    {"R_DATA_MAIN", "create_ts"}},
        {"DATA_MODIFY_TIME",    {"R_DATA_MAIN", "modify_ts"}},
        {"DATA_MODE",           {"R_DATA_MAIN", "data_mode"}},
        {"DATA_RESC_ID",        {"R_DATA_MAIN", "resc_id"}},
        {"DATA_USER_NAME",      {"R_USER_MAIN", "user_name"}},
        {"DATA_USER_ZONE",      {"R_USER_MAIN", "zone_name"}},

        {"COLL_ID",          {"R_COLL_MAIN", "coll_id"}},
        {"COLL_NAME",        {"R_COLL_MAIN", "coll_name"}},
        {"COLL_PARENT_NAME", {"R_COLL_MAIN", "parent_coll_name"}},
        {"COLL_OWNER_NAME",  {"R_COLL_MAIN", "coll_owner_name"}},
        {"COLL_OWNER_ZONE",  {"R_COLL_MAIN", "coll_owner_zone"}},
        {"COLL_MAP_ID",      {"R_COLL_MAIN", "coll_map_id"}},
        {"COLL_INHERITANCE", {"R_COLL_MAIN", "coll_inheritance"}},
        {"COLL_COMMENTS",    {"R_COLL_MAIN", "r_comment"}},
        {"COLL_CREATE_TIME", {"R_COLL_MAIN", "create_ts"}},
        {"COLL_MODIFY_TIME", {"R_COLL_MAIN", "modify_ts"}},
        {"COLL_TYPE",        {"R_COLL_MAIN", "coll_type"}},
        {"COLL_INFO1",       {"R_COLL_MAIN", "coll_info1"}},
        {"COLL_INFO2",       {"R_COLL_MAIN", "coll_info2"}},

        {"META_DATA_ATTR_NAME",   {"R_META_MAIN", "meta_attr_name"}},
        {"META_DATA_ATTR_VALUE",  {"R_META_MAIN", "meta_attr_value"}},
        {"META_DATA_ATTR_UNITS",  {"R_META_MAIN", "meta_attr_unit"}},
        {"META_DATA_ATTR_ID",     {"R_META_MAIN", "meta_id"}},
        {"META_DATA_CREATE_TIME", {"R_META_MAIN", "create_ts"}},
        {"META_DATA_MODIFY_TIME", {"R_META_MAIN", "modify_ts"}},

        {"META_COLL_ATTR_NAME",   {"R_META_MAIN", "meta_attr_name", 1}},
        {"META_COLL_ATTR_VALUE",  {"R_META_MAIN", "meta_attr_value", 1}},
        {"META_COLL_ATTR_UNITS",  {"R_META_MAIN", "meta_attr_unit", 1}},
        {"META_COLL_ATTR_ID",     {"R_META_MAIN", "meta_id", 1}},
        {"META_COLL_CREATE_TIME", {"R_META_MAIN", "create_ts", 1}},
        {"META_COLL_MODIFY_TIME", {"R_META_MAIN", "modify_ts", 1}},

        {"META_RESC_ATTR_NAME",   {"R_META_MAIN", "meta_attr_name", 2}},
        {"META_RESC_ATTR_VALUE",  {"R_META_MAIN", "meta_attr_value", 2}},
        {"META_RESC_ATTR_UNITS",  {"R_META_MAIN", "meta_attr_unit", 2}},
        {"META_RESC_ATTR_ID",     {"R_META_MAIN", "meta_id", 2}},
        {"META_RESC_CREATE_TIME", {"R_META_MAIN", "create_ts", 2}},
        {"META_RESC_MODIFY_TIME", {"R_META_MAIN", "modify_ts", 2}},

        {"META_USER_ATTR_NAME",   {"R_META_MAIN", "meta_attr_name", 3}},
        {"META_USER_ATTR_VALUE",  {"R_META_MAIN", "meta_attr_value", 3}},
        {"META_USER_ATTR_UNITS",  {"R_META_MAIN", "meta_attr_unit", 3}},
        {"META_USER_ATTR_ID",     {"R_META_MAIN", "meta_id", 3}},
        {"META_USER_CREATE_TIME", {"R_META_MAIN", "create_ts", 3}},
        {"META_USER_MODIFY_TIME", {"R_META_MAIN", "modify_ts", 3}},

        {"USER_GROUP_ID",   {"R_USER_GROUP", "group_user_id"}},
        {"USER_GROUP_NAME", {"R_USER_MAIN", "user_name"}},

        {"RULE_EXEC_ID",                 {"R_RULE_EXEC", "rule_exec_id"}},
        {"RULE_EXEC_NAME",               {"R_RULE_EXEC", "rule_name"}},
        {"RULE_EXEC_REI_FILE_PATH",      {"R_RULE_EXEC", "rei_file_path"}},
        {"RULE_EXEC_USER_NAME",          {"R_RULE_EXEC", "user_name"}},
        {"RULE_EXEC_ADDRESS",            {"R_RULE_EXEC", "exe_address"}},
        {"RULE_EXEC_TIME",               {"R_RULE_EXEC", "exe_time"}},
        {"RULE_EXEC_FREQUENCY",          {"R_RULE_EXEC", "exe_frequency"}},
        {"RULE_EXEC_PRIORITY",           {"R_RULE_EXEC", "priority"}},
        {"RULE_EXEC_ESTIMATED_EXE_TIME", {"R_RULE_EXEC", "estimated_exe_time"}},
        {"RULE_EXEC_NOTIFICATION_ADDR",  {"R_RULE_EXEC", "notification_addr"}},
        {"RULE_EXEC_LAST_EXE_TIME",      {"R_RULE_EXEC", "last_exe_time"}},
        {"RULE_EXEC_STATUS",             {"R_RULE_EXEC", "exe_status"}},

        {"TOKEN_NAMESPACE", {"R_TOKN_MAIN", "token_namespace"}},
        {"TOKEN_ID",        {"R_TOKN_MAIN", "token_id"}},
        {"TOKEN_NAME",      {"R_TOKN_MAIN", "token_name"}},
        {"TOKEN_VALUE",     {"R_TOKN_MAIN", "token_value"}},
        {"TOKEN_VALUE2",    {"R_TOKN_MAIN", "token_value2"}},
        {"TOKEN_VALUE3",    {"R_TOKN_MAIN", "token_value3"}},
        {"TOKEN_COMMENT",   {"R_TOKN_MAIN", "r_comment"}},

        // TODO These likely need table alias?
        // What is the difference between COLL_USER_ZONE and COLL_ZONE_NAME?
        // Perhaps, COLL_USER_ZONE is the zone the user is part of while COLL_ZONE_NAME
        // is the zone name the collection is associated with.
        {"COLL_USER_NAME", {"R_USER_MAIN", "user_name"}},
        {"COLL_USER_ZONE", {"R_USER_MAIN", "zone_name"}},

        {"QUOTA_USER_ID",           {"R_QUOTA_MAIN", "user_id"}},
        {"QUOTA_RESC_ID",           {"R_QUOTA_MAIN", "resc_id"}},
        {"QUOTA_LIMIT",             {"R_QUOTA_MAIN", "quota_limit"}},
        {"QUOTA_OVER",              {"R_QUOTA_MAIN", "quota_over"}},
        {"QUOTA_MODIFY_TIME",       {"R_QUOTA_MAIN", "modify_ts"}},
        {"QUOTA_USAGE_USER_ID",     {"R_QUOTA_USAGE", "user_id"}},
        {"QUOTA_USAGE_RESC_ID",     {"R_QUOTA_USAGE", "resc_id"}},
        {"QUOTA_USAGE",             {"R_QUOTA_USAGE", "quota_usage"}},
        {"QUOTA_USAGE_MODIFY_TIME", {"R_QUOTA_USAGE", "modify_ts"}},
        {"QUOTA_USER_NAME",         {"R_USER_MAIN", "user_name"}},
        {"QUOTA_USER_TYPE",         {"R_USER_MAIN", "user_type_name"}},
        {"QUOTA_USER_ZONE",         {"R_USER_MAIN", "zone_name"}},
        {"QUOTA_RESC_NAME",         {"R_RESC_MAIN", "resc_name"}},

        {"DATA_ACCESS_TYPE",     {"R_OBJT_ACCESS", "access_type_id"}},
        {"DATA_ACCESS_USER_ID",  {"R_OBJT_ACCESS", "user_id"}},
        {"DATA_ACCESS_DATA_ID",  {"R_OBJT_ACCESS", "object_id"}},
        {"DATA_ACCESS_NAME",     {"R_TOKN_MAIN", "token_name"}},
        {"DATA_TOKEN_NAMESPACE", {"R_TOKN_MAIN", "token_namespace"}},

        {"COLL_ACCESS_TYPE",     {"R_OBJT_ACCESS", "access_type_id"}},
        {"COLL_ACCESS_USER_ID",  {"R_OBJT_ACCESS", "user_id"}},
        {"COLL_ACCESS_COLL_ID",  {"R_OBJT_ACCESS", "object_id"}},
        {"COLL_ACCESS_NAME",     {"R_TOKN_MAIN", "token_name"}},
        {"COLL_TOKEN_NAMESPACE", {"R_TOKN_MAIN", "token_namespace"}},

        {"TICKET_ID",                      {"R_TICKET_MAIN", "ticket_id"}},
        {"TICKET_STRING",                  {"R_TICKET_MAIN", "ticket_string"}},
        {"TICKET_TYPE",                    {"R_TICKET_MAIN", "ticket_type"}},
        {"TICKET_USER_ID",                 {"R_TICKET_MAIN", "user_id"}},
        {"TICKET_OBJECT_ID",               {"R_TICKET_MAIN", "object_id"}},
        {"TICKET_OBJECT_TYPE",             {"R_TICKET_MAIN", "object_type"}},
        {"TICKET_USES_LIMIT",              {"R_TICKET_MAIN", "uses_limit"}},
        {"TICKET_USES_COUNT",              {"R_TICKET_MAIN", "uses_count"}},
        {"TICKET_WRITE_FILE_LIMIT",        {"R_TICKET_MAIN", "write_file_limit"}},
        {"TICKET_WRITE_FILE_COUNT",        {"R_TICKET_MAIN", "write_file_count"}},
        {"TICKET_WRITE_BYTE_LIMIT",        {"R_TICKET_MAIN", "write_byte_limit"}},
        {"TICKET_WRITE_BYTE_COUNT",        {"R_TICKET_MAIN", "write_byte_count"}},
        {"TICKET_EXPIRY_TS",               {"R_TICKET_MAIN", "ticket_expiry_ts"}},
        {"TICKET_CREATE_TIME",             {"R_TICKET_MAIN", "create_time"}},
        {"TICKET_MODIFY_TIME",             {"R_TICKET_MAIN", "modify_time"}},
        {"TICKET_ALLOWED_HOST",            {"R_TICKET_ALLOWED_HOSTS", "host"}},
        {"TICKET_ALLOWED_HOST_TICKET_ID",  {"R_TICKET_ALLOWED_HOSTS", "ticket_id"}},
        {"TICKET_ALLOWED_USER_NAME",       {"R_TICKET_ALLOWED_USERS", "user_name"}},
        {"TICKET_ALLOWED_USER_TICKET_ID",  {"R_TICKET_ALLOWED_USERS", "ticket_id"}},
        {"TICKET_ALLOWED_GROUP_NAME",      {"R_TICKET_ALLOWED_GROUPS", "group_name"}},
        {"TICKET_ALLOWED_GROUP_TICKET_ID", {"R_TICKET_ALLOWED_GROUPS", "ticket_id"}},
        {"TICKET_DATA_NAME",               {"R_DATA_MAIN", "data_name"}},
        {"TICKET_COLL_NAME",               {"R_COLL_MAIN", "coll_name"}},
        {"TICKET_OWNER_NAME",              {"R_USER_MAIN", "user_name"}},
        {"TICKET_OWNER_ZONE",              {"R_USER_MAIN", "zone_name"}},
        {"TICKET_DATA_COLL_NAME",          {"R_COLL_MAIN", "coll_name"}} // Includes join between R_DATA_MAIN and R_COLL_MAIN.
    }; // column_name_mappings
} // namespace irods::experemental::api::genquery

#endif // IRODS_TABLE_COLUMN_KEY_MAPS_HPP
