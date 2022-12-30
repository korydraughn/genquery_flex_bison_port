#ifndef IRODS_GENQUERY_SCANNER_HPP
#define IRODS_GENQUERY_SCANNER_HPP

#ifndef yyFlexLexerOnce
#  undef yyFlexLexer
#  define yyFlexLexer Genquery_FlexLexer // the trick with prefix; no namespace here :(
#  include <FlexLexer.h>
#endif

#undef YY_DECL
#define YY_DECL irods::experimental::api::genquery::Parser::symbol_type irods::experimental::api::genquery::scanner::get_next_token()

#include "parser.hpp" //genquery_parser_bison_generated.hpp" // defines irods::experimental::api::genquery::Parser::symbol_type

namespace irods::experimental::api::genquery
{
    class wrapper;

    class scanner : public yyFlexLexer
    {
    public:
        scanner(wrapper& wrapper) : _wrapper(wrapper) {}
        virtual ~scanner() {}
        virtual Parser::symbol_type get_next_token();

    private:
        wrapper& _wrapper;
    };
} // namespace irods::experimental::api::genquery

#endif // IRODS_GENQUERY_SCANNER_HPP
