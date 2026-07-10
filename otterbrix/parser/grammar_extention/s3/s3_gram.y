/*-------------------------------------------------------------------------
 *
 * s3_gram.y
 *      Grammar for the s3 parser extension.
 *
 * A self-contained, reentrant bison parser with its own `s3yy` prefix, fully
 * isolated from the main `base_yy` grammar. It builds its OWN AST
 * (s3_ext::s3_stmt) and wraps the root in an ExtensionNode envelope.
 *
 * Recognised statements (the embedded SELECT of COPY is stripped by the parse
 * driver before this grammar runs, and re-attached afterwards):
 *
 *   CREATE EXTERNAL TABLE <db>.<table> WITH ( key = 'value', ... )
 *   COPY TO '<location>' [ WITH ( key = 'value', ... ) ]
 *
 *-------------------------------------------------------------------------
 */

%code requires {
    #include <memory_resource>
    #include "s3_ast.hpp"
    #include <components/sql/parser/nodes/parsenodes.h>
}

%code {
    /* Defined by the flex scanner (s3_scan.l). */
    int s3yylex(YYSTYPE* yylval_param, void* yyscanner);
    void s3yyerror(void* scanner, std::pmr::memory_resource* resource, Node** out, const char* message);
}

%pure-parser
%name-prefix="s3yy"
%parse-param {void* scanner} {std::pmr::memory_resource* resource} {Node** out}
%lex-param   {void* scanner}

%union {
    const char*         str;
    s3_ext::s3_option*  opt;
    s3_ext::s3_stmt*    stmt;
}

%token <str> IDENT SCONST
%token KW_CREATE KW_EXTERNAL KW_TABLE KW_COPY KW_TO KW_WITH

%type <stmt> stmt create_stmt copy_stmt
%type <opt>  opt_list option

%%

input:
    stmt {
        /* The sub-rule actions already wrote the ExtensionNode into *out. */
    }
    ;

stmt:
    create_stmt   { $$ = $1; }
    | copy_stmt   { $$ = $1; }
    ;

/* Tolerate the SQL statement terminator. Tools that drive otterstax through
 * raw SQL files (psql -f, the demo runbook) end statements with ';'; the
 * integration libraries (mysql.connector, psycopg2) strip it before send so
 * they don't notice its absence, but accepting it here keeps both transports
 * happy. */
opt_semicolon:
    /* empty */
    | ';'
    ;

create_stmt:
    KW_CREATE KW_EXTERNAL KW_TABLE IDENT '.' IDENT KW_WITH '(' opt_list ')' opt_semicolon {
        $$ = s3_ext::make_create(resource, $4, $6, $9);
        *out = make_extension_node(resource, "s3", $$);
    }
    ;

copy_stmt:
    KW_COPY KW_TO SCONST KW_WITH '(' opt_list ')' opt_semicolon {
        /* WITH (...) is mandatory: it carries the format/credentials and keeps
         * this statement distinct from the core parser's bare COPY ... TO. */
        $$ = s3_ext::make_copy(resource, $3, $6);
        *out = make_extension_node(resource, "s3", $$);
    }
    ;

opt_list:
    option                  { $$ = $1; }
    | opt_list ',' option   { $$ = s3_ext::append_option($1, $3); }
    ;

option:
    IDENT '=' SCONST        { $$ = s3_ext::make_option(resource, $1, $3); }
    ;

%%

void s3yyerror(void*, std::pmr::memory_resource*, Node**, const char*) {
    /* No diagnostic here — failure is reported via a non-zero s3yyparse() return. */
}
