%{
/*-------------------------------------------------------------------------
 *
 * s3_scan.l
 *      Flex lexer for the s3 parser extension.
 *
 * A self-contained reentrant scanner with its own `s3yy` prefix, isolated from
 * the main `core_yy` scanner. It tokenizes the statement keywords, identifiers,
 * single-quoted string constants and the `. = ( ) ,` punctuation. flex
 * generates the scanner API header itself (s3_scan.h, via the CMake
 * DEFINES_FILE option); the parse driver lives in s3_extension.cpp.
 *
 * The memory resource used to arena-copy lexemes is handed in through
 * yyextra (s3yyset_extra) by the parse driver.
 *
 *-------------------------------------------------------------------------
 */
#include <cstring>
#include "s3_ast.hpp"
#include "s3_gram.hpp"
%}

%option reentrant
%option bison-bridge
%option noyywrap
%option nounput
%option noinput
%option prefix="s3yy"
%option extra-type="std::pmr::memory_resource*"

%%

[ \t\r\n]+              { /* skip whitespace */ }

[Cc][Rr][Ee][Aa][Tt][Ee]                 { return KW_CREATE; }
[Ee][Xx][Tt][Ee][Rr][Nn][Aa][Ll]         { return KW_EXTERNAL; }
[Tt][Aa][Bb][Ll][Ee]                     { return KW_TABLE; }
[Cc][Oo][Pp][Yy]                         { return KW_COPY; }
[Tt][Oo]                                 { return KW_TO; }
[Ww][Ii][Tt][Hh]                         { return KW_WITH; }

[A-Za-z_][A-Za-z0-9_]*  { yylval->str = s3_ext::arena_strdup(yyextra, yytext, yyleng); return IDENT; }
'([^']|'')*'            { yylval->str = s3_ext::arena_sconst(yyextra, yytext, yyleng); return SCONST; }

"."                     { return '.'; }
"="                     { return '='; }
"("                     { return '('; }
")"                     { return ')'; }
","                     { return ','; }

.                       { return yytext[0]; } /* unknown char, let the parser error */

%%
