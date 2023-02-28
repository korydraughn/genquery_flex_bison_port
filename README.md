# iRODS GenQuery2 Parser

An experimental re-implementation of the iRODS GenQuery parser.

Once stable, the code will be merged into the iRODS server making it available with future releases of iRODS.

## Motivation

To provide a GenQuery parser that is easy to understand, maintain, and enhance all while providing a syntax that mirrors standard SQL as much as possible.

That is only possible if users of this project provide feedback.

## Features

It supports the following:
- Logical AND, OR, and NOT
- Grouping via parentheses
- SQL CAST operator
- Metadata queries involving different iRODS entities (i.e. data objects, collections, users, and resources)
- SQL ORDER BY, FETCH FIRST N ROWS ONLY, D
- All binary operators (i.e. =, !=, <, <=, >, >=, LIKE, BETWEEN)
- Enforces the iRODS permission model

## Limitations (for now)

- Groups are not fully supported
- Cannot resolve tickets to data objects and collections using a single query
- Microservices are not callable from the Python Rule Engine Plugin
- Integer values must be treated as strings, except when used for OFFSET, LIMIT, FETCH FIRST _N_ ROWS ONLY

## Building

This project provides an API Plugin, a Rule Engine Plugin, and an iCommand upon successfully compiling it.

This project only compiles against iRODS 4.3.0 and depends on the following:
- irods-dev(el) package
- irods-externals-nlohmann-json package
- irods-externals-boost package
- flex
- bison

The steps for building the package are:
```bash
mkdir build
cd build
cmake --build /path/to/git/repo
```

## Usage

### API Plugin

- API Number: 1000001
- Input     : A GenQuery string
- Output    : A JSON string (i.e. an array of array of strings)

### Microservices

In order to use the microservices, you'll need to enable the Rule Engine Plugin.

In your server_config.json file, insert the following inside of the rule_engines stanza:
```json
{
    "plugin_instance": "irods_rule_engine-genquery2",
    "plugin_name": "irods_rule_engine-genquery2-instance",
    "plugin_specific_configuration": {}
}
```

With that in place, you now have access to several microservices. Below is an example demonstrating their use via the iRODS Rule Language.
```bash
# Execute a query. The results are stored in the Rule Engine Plugin.
genquery2_execute(*ctx, "select COLL_NAME, DATA_NAME order by DATA_NAME desc limit 1");

# Iterate over the resutls.
while (errorcode(genquery2_next_row(*ctx)) == 0) {
    genquery2_column(*ctx, '0', *coll_name); # Copy the COLL_NAME into *coll_name.
    genquery2_column(*ctx, '1', *data_name); # Copy the DATA_NAME into *data_name.
    writeLine("stdout", "logical path => [*coll_name/*data_name]");
}

# Free any resources used. This is handled for you when the agent is shut down as well.
genquery2_destroy(*ctx);
```

### iCommand



## Available Columns


