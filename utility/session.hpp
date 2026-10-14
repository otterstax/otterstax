// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <cstddef>

// The per-call id the file / s3 managers and Kafka take
// for a statement (session::to_hash of the query session's statement id).
using session_hash_t = std::size_t;
