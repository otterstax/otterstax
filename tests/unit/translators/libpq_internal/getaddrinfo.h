/* SPDX-License-Identifier: Apache-2.0
 * Copyright 2025-2026  OtterStax
 *
 * libpq-int.h includes "getaddrinfo.h", a PostgreSQL source-tree header that only
 * declares fallbacks for platforms without getaddrinfo(3) and is not installed with
 * the library. Every platform this project builds on has getaddrinfo(3), so an empty
 * stand-in satisfies the include.
 */
