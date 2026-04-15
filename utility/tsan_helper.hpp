// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#if defined(__has_feature)
#if __has_feature(thread_sanitizer)
#define HAS_TSAN 1
#endif
#endif

#if defined(__SANITIZE_THREAD__)
#define HAS_TSAN 1
#endif

#if defined(HAS_TSAN)
constexpr bool TSAN_ENABLED = true;
#else
constexpr bool TSAN_ENABLED = false;
#endif
