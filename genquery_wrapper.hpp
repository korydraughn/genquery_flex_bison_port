#ifndef IRODS_GENQUERY_WRAPPER_HPP
#define IRODS_GENQUERY_WRAPPER_HPP

#include "genquery_ast_types.hpp"
#include "parser.hpp" //"genquery_parser_bison_generated.hpp"
#include "genquery_scanner.hpp"

#include <cstdint>
#include <memory>
#include <string>

namespace irods::experimental::api::genquery
{
    class wrapper
    {
    public:
        explicit wrapper(std::istream*);

        static Select parse(std::istream&);
        static Select parse(const char*);
        static Select parse(const std::string&);

        friend class Parser;
        friend class scanner;

    private:
        void increaseLocation(std::uint64_t);
        std::uint64_t location() const;

        scanner _scanner;
        Parser _parser;
        Select _select;
        std::uint64_t _location;
    };
} // namespace irods::experimental::api::genquery

#endif // IRODS_GENQUERY_WRAPPER_HPP
