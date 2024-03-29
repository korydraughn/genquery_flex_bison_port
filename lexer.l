%{
    #include "genquery_scanner.hpp"
    #include "genquery_wrapper.hpp"
    #include "parser.hpp" //"genquery_parser_bison_generated.hpp"
    #include "location.hh"

    #include <iostream>

    #define yyterminate() gq::Parser::make_END_OF_INPUT(gq::location());

    #define YY_USER_ACTION _wrapper.increaseLocation(yyleng);
%}

%option c++
%option nodefault
%option noyywrap
%option yyclass="scanner"
%option prefix="Genquery_"

%%

[ \t\n]                ;
'(''|[^'])*'           { yytext[yyleng - 1] = '\0'; ++yytext; return gq::Parser::make_STRING_LITERAL(yytext, gq::location()); }
(?i:select)            return gq::Parser::make_SELECT(gq::location());
(?i:where)             return gq::Parser::make_WHERE(gq::location());
(?i:like)              return gq::Parser::make_LIKE(gq::location());
(?i:in)                return gq::Parser::make_IN(gq::location());
(?i:between)           return gq::Parser::make_BETWEEN(gq::location());
(?i:no-distinct)       return gq::Parser::make_NO_DISTINCT(gq::location());
"="                    return gq::Parser::make_EQUAL(gq::location());
"!="                   return gq::Parser::make_NOT_EQUAL(gq::location());
"<>"                   return gq::Parser::make_NOT_EQUAL(gq::location());
"<"                    return gq::Parser::make_LESS_THAN(gq::location());
"<="                   return gq::Parser::make_LESS_THAN_OR_EQUAL_TO(gq::location());
">"                    return gq::Parser::make_GREATER_THAN(gq::location());
">="                   return gq::Parser::make_GREATER_THAN_OR_EQUAL_TO(gq::location());
(?i:begin_of)          return gq::Parser::make_BEGINNING_OF(gq::location());
(?i:parent_of)         return gq::Parser::make_PARENT_OF(gq::location());
"||"                   return gq::Parser::make_CONDITION_OR(gq::location());
"||="                  return gq::Parser::make_CONDITION_OR_EQUAL(gq::location());
"&&"                   return gq::Parser::make_CONDITION_AND(gq::location());
(?i:not)               return gq::Parser::make_CONDITION_NOT(gq::location());
(?i:and)               return gq::Parser::make_AND(gq::location());
(?i:or)                return gq::Parser::make_CONDITION_OR(gq::location());
,                      return gq::Parser::make_COMMA(gq::location());
"("                    return gq::Parser::make_OPEN_PAREN(gq::location());
")"                    return gq::Parser::make_CLOSE_PAREN(gq::location());
[a-zA-Z][a-zA-Z0-9_]*  return gq::Parser::make_IDENTIFIER(yytext, gq::location());
.                      std::cerr << "scanner: unknown character [" << yytext << "]\n"; // TODO: improve error handling
<<EOF>>                return yyterminate();

%%
