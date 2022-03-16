%skeleton "lalr1.cc" /* Enables C++ */
%require "3.0" /* position.hh and stack.hh are deprecated in 3.2 and later. */
%language "C++" /* Redundant? */
%defines /* Doesn't work in version 3.0 - %header */

/* The name of the parser class. Defaults to "parser" in C++. */
/* Doesn't work in version 3.0 - %define api.parser.class { Parser } */
%define parser_class_name { Parser }

/*
Request that symbols be handled as a whole (type, value, and possibly location)
in the scanner. In C++, only works when variant-based semantic values are enabled.
This option causes make_* functions to be generated for each token kind.
*/
%define api.token.constructor 

/* The type used for semantic values. */
%define api.value.type variant

/* Enables runtime assertions to catch invalid uses. In C++, detects improper use of variants. */
%define parse.assert

/* Defines the namespace for the parser class. */
%define api.namespace { irods::experimental::api::genquery }

%code requires
{
    #include "genquery_ast_types.hpp"

    #include <iostream> // TODO Is this needed?
    #include <string>
    #include <vector>

    namespace irods::experimental::api::genquery
    {
        class scanner;
        class wrapper;
    } // namespace irods::experimental::api::genquery

    namespace gq = irods::experimental::api::genquery;
}

%code top
{
    #include "genquery_scanner.hpp"
    #include "parser.hpp" //"genquery_parser_bison_generated.hpp"
    #include "genquery_wrapper.hpp"
    #include "location.hh"

    static gq::Parser::symbol_type yylex(gq::scanner& scanner, gq::wrapper& wrapper)
    {
        return scanner.get_next_token();
    }
}

%lex-param { gq::scanner& scanner } { gq::wrapper& wrapper }
%parse-param { gq::scanner& scanner } { gq::wrapper& wrapper }
%locations
%define parse.trace
%define parse.error verbose /* Can produce incorrect info if LAC is not enabled. */

%define api.token.prefix {GENQUERY_TOKEN_}

%token <std::string> IDENTIFIER STRING_LITERAL
%token SELECT NO_DISTINCT WHERE AND OR COMMA PAREN_OPEN PAREN_CLOSE
%token BETWEEN EQUAL NOT_EQUAL BEGINNING_OF LIKE IN PARENT_OF
%token LESS_THAN GREATER_THAN LESS_THAN_OR_EQUAL_TO GREATER_THAN_OR_EQUAL_TO
%token CONDITION_NOT ORDER BY
%token END_OF_INPUT 0

/*
%left is the same as %token, but defines associativity.
%token does not define associativity or precedence.
%precedence does not define associativity.

Operator precedence is determined by the line ordering of the declarations. 
The further down the page the line is, the higher the precedence. For example,
CONDITION_NOT has higher precedence than CONDITION_AND.

While these directives support specifying a semantic type, Bison recommends not
doing that and using these directives to specify precedence and associativity
rules only.
*/
%left OR
%left AND
%precedence CONDITION_NOT

%type<gq::Selections> selections;
%type<gq::Conditions> conditions;
%type<gq::Selection> selection;
%type<gq::Column> column;
%type<gq::SelectFunction> select_function;
%type<gq::Condition> condition;
%type<gq::ConditionExpression> condition_expression;
%type<std::vector<std::string>> list_of_string_literals;

%start select /* Defines where grammar starts */

%%

select:
    SELECT selections  { std::swap(wrapper._select.selections, $2); }
  | SELECT selections WHERE conditions  { std::swap(wrapper._select.selections, $2); std::swap(wrapper._select.conditions, $4); }
  | SELECT NO_DISTINCT selections  { wrapper._select.no_distinct = true; std::swap(wrapper._select.selections, $3); }
  | SELECT NO_DISTINCT selections WHERE conditions  { wrapper._select.no_distinct = true; std::swap(wrapper._select.selections, $3); std::swap(wrapper._select.conditions, $5); }

selections:
    selection  { $$ = gq::Selections{std::move($1)}; }
  | selections COMMA selection  { $1.push_back(std::move($3)); std::swap($$, $1); }

selection:
    column  { $$ = std::move($1); }
  | select_function  { $$ = std::move($1); }

column:
    IDENTIFIER  { $$ = gq::Column{std::move($1)}; }

select_function:
    IDENTIFIER PAREN_OPEN IDENTIFIER PAREN_CLOSE  { $$ = gq::SelectFunction{std::move($1), gq::Column{std::move($3)}}; }

conditions:
    condition  { $$ = gq::Conditions{std::move($1)}; }
  | conditions AND condition  { $1.push_back(gq::logical_and{std::move($3)}); std::swap($$, $1); }
  | conditions OR condition  { $1.push_back(gq::logical_or{std::move($3)}); std::swap($$, $1); }
  | PAREN_OPEN conditions PAREN_CLOSE  { $$ = gq::Conditions{gq::logical_grouping{std::move($2)}}; }

condition:
    column condition_expression  { $$ = gq::Condition(std::move($1), std::move($2)); }

condition_expression:
    LIKE STRING_LITERAL  { $$ = gq::ConditionLike(std::move($2)); }
  | IN PAREN_OPEN list_of_string_literals PAREN_CLOSE  { $$ = gq::ConditionIn(std::move($3)); }
  | BETWEEN STRING_LITERAL STRING_LITERAL { $$ = gq::ConditionBetween(std::move($2), std::move($3)); }
  | EQUAL STRING_LITERAL  { $$ = gq::ConditionEqual(std::move($2)); }
  | NOT_EQUAL STRING_LITERAL  { $$ = gq::ConditionNotEqual(std::move($2)); }
  | LESS_THAN STRING_LITERAL  { $$ = gq::ConditionLessThan(std::move($2)); }
  | LESS_THAN_OR_EQUAL_TO STRING_LITERAL  { $$ = gq::ConditionLessThanOrEqualTo(std::move($2)); }
  | GREATER_THAN STRING_LITERAL  { $$ = gq::ConditionGreaterThan(std::move($2)); }
  | GREATER_THAN_OR_EQUAL_TO STRING_LITERAL  { $$ = gq::ConditionGreaterThanOrEqualTo(std::move($2)); }
  | PARENT_OF PAREN_OPEN STRING_LITERAL PAREN_CLOSE  { $$ = gq::ConditionParentOf(std::move($3)); }
  | BEGINNING_OF PAREN_OPEN STRING_LITERAL PAREN_CLOSE  { $$ = gq::ConditionBeginningOf(std::move($3)); }
  | CONDITION_NOT condition_expression  { $$ = gq::ConditionOperator_Not(std::move($2)); }

list_of_string_literals:
    STRING_LITERAL  { $$ = std::vector<std::string>{std::move($1)}; }
  | list_of_string_literals COMMA STRING_LITERAL  { $1.push_back(std::move($3)); std::swap($$, $1); }

%%

void gq::Parser::error(const location& location, const std::string& message) {
    // TODO: improve error handling
    std::cerr << "Error: " << message << std::endl << "Error location: " << wrapper.location() << std::endl;
}
