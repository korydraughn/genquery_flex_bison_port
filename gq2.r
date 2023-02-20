test_genquery2 {
    genquery2_execute(*ctx, "select COLL_NAME, DATA_NAME order by DATA_NAME desc limit 1");

    while (errorcode(genquery2_next_row(*ctx)) == 0) {
        genquery2_column(*ctx, '0', *coll_name);
        genquery2_column(*ctx, '1', *data_name);
        writeLine("serverLog", "logical path => [*coll_name/*data_name]");
        writeLine("stdout", "logical path => [*coll_name/*data_name]");
    }

    genquery2_destroy(*ctx);
}

INPUT *ctx='',*coll_name='',*data_name=''
OUTPUT ruleExecOut
