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
(?i:no)                return gq::Parser::make_NO(gq::location());
(?i:distinct)          return gq::Parser::make_DISTINCT(gq::location());
(?i:order)             return gq::Parser::make_ORDER(gq::location());
(?i:by)                return gq::Parser::make_BY(gq::location());
(?i:asc)               return gq::Parser::make_ASC(gq::location());
(?i:desc)              return gq::Parser::make_DESC(gq::location());
(?i:offset)            return gq::Parser::make_OFFSET(gq::location());
(?i:fetch)             return gq::Parser::make_FETCH(gq::location());
(?i:first)             return gq::Parser::make_FIRST(gq::location());
(?i:rows)              return gq::Parser::make_ROWS(gq::location());
(?i:only)              return gq::Parser::make_ONLY(gq::location());
(?i:case)              return gq::Parser::make_CASE(gq::location());
(?i:when)              return gq::Parser::make_WHEN(gq::location());
(?i:else)              return gq::Parser::make_ELSE(gq::location());
(?i:end)               return gq::Parser::make_END(gq::location());
(?i:group)             return gq::Parser::make_GROUP(gq::location());
(?i:having)            return gq::Parser::make_HAVING(gq::location());
(?i:exists)            return gq::Parser::make_EXISTS(gq::location());
(?i:is)                return gq::Parser::make_IS(gq::location());
(?i:null)              return gq::Parser::make_NULL(gq::location());
(?i:not)               return gq::Parser::make_NOT(gq::location());
(?i:and)               return gq::Parser::make_AND(gq::location());
(?i:or)                return gq::Parser::make_OR(gq::location());
"="                    return gq::Parser::make_EQUAL(gq::location());
"!="                   return gq::Parser::make_NOT_EQUAL(gq::location());
"<>"                   return gq::Parser::make_NOT_EQUAL(gq::location());
"<"                    return gq::Parser::make_LESS_THAN(gq::location());
"<="                   return gq::Parser::make_LESS_THAN_OR_EQUAL_TO(gq::location());
">"                    return gq::Parser::make_GREATER_THAN(gq::location());
">="                   return gq::Parser::make_GREATER_THAN_OR_EQUAL_TO(gq::location());
,                      return gq::Parser::make_COMMA(gq::location());
"("                    return gq::Parser::make_PAREN_OPEN(gq::location());
")"                    return gq::Parser::make_PAREN_CLOSE(gq::location());
[a-zA-Z][a-zA-Z0-9_]*  return gq::Parser::make_IDENTIFIER(yytext, gq::location());
[0-9]+                 return gq::Parser::make_POSITIVE_INTEGER(yytext, gq::location());
-?[0-9]+               return gq::Parser::make_INTEGER(yytext, gq::location());
.                      std::cerr << "scanner: unknown character [" << yytext << "]\n"; // TODO: improve error handling
<<EOF>>                return yyterminate();

%%
