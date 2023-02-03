#include <iostream>
#include <string>

#include "genquery_sql.hpp"
#include "genquery_wrapper.hpp"

int main(int _argc, char* _argv[])
{
    try {
        namespace gq = irods::experimental::api::genquery;
        const auto sql = gq::sql(gq::wrapper::parse(_argv[1]));
        std::cout << sql << '\n';
    }
    catch (const std::exception& e) {
        std::cerr << "ERROR: " << e.what() << '\n';
    }
}

