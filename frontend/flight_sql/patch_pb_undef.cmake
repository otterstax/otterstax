# SPDX-License-Identifier: Apache-2.0
# Copyright 2025-2026  OtterStax

# Prepends the engine-macro guard to a GENERATED protobuf header (called from
# the protoc custom_command right after generation, in the build dir).
#
# The engine's postgres-derived headers #define ERROR (a pg error code in
# pg_type_definitions.h), and FlightSql's generated
# SetSessionOptionsResult.ErrorValue enum member is spelled ERROR — a TU that
# reaches the generated header after any engine header would not parse it
# ("expected unqualified-id before numeric constant"). Undefining the macro at
# the top of every generated header makes the include order irrelevant, the
# same way catalog_manager.hpp undefs the parser's DAY/SECOND for arrow.

file(READ "${IN}" CONTENT)
file(WRITE "${IN}" "#undef ERROR\n${CONTENT}")
