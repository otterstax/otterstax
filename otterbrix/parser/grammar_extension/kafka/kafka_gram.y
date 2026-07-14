/*-------------------------------------------------------------------------
 *
 * kafka_gram.y
 *      Bison grammar for the otterstax kafka parser extension.
 *
 * A self-contained, reentrant parser with its own `kafka_yy` prefix, isolated
 * from the core `base_yy` grammar. It recognizes the ksql-flavoured statements
 * that the core SQL grammar rejects and builds kafka_grammar's own AST, wrapping
 * the root in an ExtensionNode envelope routed back to kafka_ext::transform.
 *
 * Grammar (informal):
 *   CREATE SOURCE <name> (<col> <type>, ...) WITH (<k>=<v>, ...)
 *   CREATE STREAM <name> [WITH (...)] AS <raw select>
 *   DROP   SOURCE|STREAM [IF EXISTS] <name>
 *
 *-------------------------------------------------------------------------
 */

%code requires {
    #include <memory_resource>
    #include "kafka_ast.hpp"
    #include <components/sql/parser/nodes/parsenodes.h>
}

%code {
    /* Defined by the flex scanner (kafka_scan.l). */
    int kafka_yylex(YYSTYPE* yylval_param, void* yyscanner);
    void kafka_yyerror(void* scanner, std::pmr::memory_resource* resource, Node** out, const char* message);
}

%pure-parser
%name-prefix="kafka_yy"
%parse-param {void* scanner} {std::pmr::memory_resource* resource} {Node** out}
%lex-param   {void* scanner}

%union {
    const char* str;
    bool boolean;
    kafka_grammar::kafka_stmt* stmt;
    kafka_grammar::column_def* column;
    kafka_grammar::with_option* option;
}

%token KW_CREATE KW_DROP KW_SOURCE KW_STREAM KW_WITH KW_AS KW_IF KW_EXISTS
%token <str> IDENT SCONST ICONST RAW_SELECT

%type <stmt>    statement create_source create_stream drop_stmt
%type <column>  column_list column_def
%type <str>     type_name option_value
%type <option>  with_clause opt_with option_list option
%type <boolean> opt_if_exists

%%

input:
    statement opt_semicolon {
        /* Wrap kafka's own AST in an ExtensionNode envelope */
        *out = make_extension_node(resource, "kafka", $1);
    }
    ;

opt_semicolon:
      /* empty */
    | ';'
    ;

statement:
      create_source   { $$ = $1; }
    | create_stream   { $$ = $1; }
    | drop_stmt       { $$ = $1; }
    ;

create_source:
    KW_CREATE KW_SOURCE IDENT '(' column_list ')' with_clause {
        $$ = kafka_grammar::make_stmt(resource,
                                      kafka_grammar::stmt_kind::create_source,
                                      kafka_grammar::object_kind::source,
                                      $3);
        $$->columns = $5;
        $$->options = $7;
    }
    ;

create_stream:
    KW_CREATE KW_STREAM IDENT opt_with KW_AS RAW_SELECT {
        $$ = kafka_grammar::make_stmt(resource,
                                      kafka_grammar::stmt_kind::create_stream,
                                      kafka_grammar::object_kind::stream,
                                      $3);
        $$->options = $4;
        $$->as_select = $6;
    }
    ;

drop_stmt:
      KW_DROP KW_SOURCE opt_if_exists IDENT {
        $$ = kafka_grammar::make_stmt(resource,
                                      kafka_grammar::stmt_kind::drop_object,
                                      kafka_grammar::object_kind::source,
                                      $4);
        $$->if_exists = $3;
      }
    | KW_DROP KW_STREAM opt_if_exists IDENT {
        $$ = kafka_grammar::make_stmt(resource,
                                      kafka_grammar::stmt_kind::drop_object,
                                      kafka_grammar::object_kind::stream,
                                      $4);
        $$->if_exists = $3;
      }
    ;

opt_if_exists:
      /* empty */      { $$ = false; }
    | KW_IF KW_EXISTS  { $$ = true; }
    ;

column_list:
      column_def                  { $$ = $1; }
    | column_def ',' column_list  { $1->next = $3; $$ = $1; }
    ;

column_def:
    IDENT type_name {
        $$ = kafka_grammar::make_column(resource, $1, $2);
    }
    ;

type_name:
      IDENT                  { $$ = $1; }
    | IDENT '(' ICONST ')'   { $$ = $1; } /* accept and ignore a length, e.g. VARCHAR(255) */
    ;

with_clause:
    KW_WITH '(' option_list ')'   { $$ = $3; }
    ;

opt_with:
      /* empty */   { $$ = nullptr; }
    | with_clause   { $$ = $1; }
    ;

option_list:
      option                   { $$ = $1; }
    | option ',' option_list   { $1->next = $3; $$ = $1; }
    ;

option:
    IDENT '=' option_value {
        $$ = kafka_grammar::make_option(resource, $1, $3);
    }
    ;

option_value:
      SCONST   { $$ = $1; }
    | IDENT    { $$ = $1; }
    | ICONST   { $$ = $1; }
    ;

%%

void kafka_yyerror(void*, std::pmr::memory_resource*, Node**, const char*) {
    /* Errors are reported via a non-zero kafka_yyparse() return value. */
}
