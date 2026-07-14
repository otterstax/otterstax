%{
/*-------------------------------------------------------------------------
 *
 * kafka_scan.l
 *      Flex lexer for the otterstax kafka parser extension.
 *
 * A self-contained reentrant scanner with its own `kafka_yy` prefix, isolated
 * from the core `core_yy` scanner. Tokenizes the kafka DDL keywords (case
 * insensitive), identifiers, single-quoted strings, integers and punctuation.
 * After the `AS` of a CREATE STREAM it switches to the exclusive RAW state and
 * returns the rest of the statement verbatim (RAW_SELECT), so the embedded query
 * can be re-parsed by the core otterstax parser.
 *
 * The arena memory_resource used to copy token text is carried as the scanner's
 * yyextra (set by kafka_ext::parse via kafka_yyset_extra).
 *
 *-------------------------------------------------------------------------
 */
#include <cctype>
#include <cstring>
#include <memory_resource>
#include <string>

#include "kafka_gram.hpp"

namespace {
    const char* arena_copy(std::pmr::memory_resource* resource, const char* text, std::size_t len) {
        char* out = static_cast<char*>(resource->allocate(len + 1));
        std::memcpy(out, text, len);
        out[len] = '\0';
        return out;
    }

    /* Case-insensitive keyword lookup; otherwise an arena-copied IDENT. */
    int keyword_or_ident(const char* text, std::size_t len, YYSTYPE* value, std::pmr::memory_resource* resource) {
        struct entry {
            const char* word;
            int token;
        };
        static const entry keywords[] = {
            {"create", KW_CREATE},
            {"drop", KW_DROP},
            {"source", KW_SOURCE},
            {"stream", KW_STREAM},
            {"with", KW_WITH},
            {"as", KW_AS},
            {"if", KW_IF},
            {"exists", KW_EXISTS},
        };
        std::string lower(text, len);
        for (char& c : lower) {
            c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
        }
        for (const entry& kw : keywords) {
            if (lower == kw.word) {
                return kw.token;
            }
        }
        value->str = arena_copy(resource, text, len);
        return IDENT;
    }
} // namespace
%}

%option reentrant
%option bison-bridge
%option noyywrap
%option nounput
%option noinput
%option prefix="kafka_yy"
%option extra-type="std::pmr::memory_resource*"

%x RAW

%%

[ \t\r\n]+              { /* skip whitespace */ }
"--"[^\n]*             { /* skip line comment */ }

"("                    { return '('; }
")"                    { return ')'; }
","                    { return ','; }
"="                    { return '='; }
";"                    { return ';'; }

[0-9]+                 { yylval->str = arena_copy(yyget_extra(yyscanner), yytext, yyleng); return ICONST; }
'[^']*'                { yylval->str = arena_copy(yyget_extra(yyscanner), yytext + 1, yyleng - 2); return SCONST; }

[A-Za-z_][A-Za-z0-9_]* {
                           int token = keyword_or_ident(yytext, yyleng, yylval, yyget_extra(yyscanner));
                           if (token == KW_AS) {
                               BEGIN(RAW); /* everything after AS is the embedded query */
                           }
                           return token;
                       }

.                      { return yytext[0]; } /* unknown char, let the parser error */

<RAW>[^;]+             { yylval->str = arena_copy(yyget_extra(yyscanner), yytext, yyleng); BEGIN(INITIAL); return RAW_SELECT; }
<RAW>";"              { BEGIN(INITIAL); return ';'; }

%%
