// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
#pragma once

#ifdef TRACY_ENABLE
#   include <tracy/Tracy.hpp>

    // ── zones / frames / plots / messages ────────────────────────────────────
#   define OTX_ZONE()            ZoneScoped
#   define OTX_ZONE_N(name)      ZoneScopedN(name)
#   define OTX_FRAME()           FrameMark
#   define OTX_FRAME_N(name)     FrameMarkNamed(name)
#   define OTX_PLOT(name, val)   TracyPlot(name, static_cast<double>(val))
#   define OTX_MESSAGE_L(msg)    TracyMessageL(msg)
#   define OTX_MESSAGE(msg)      TracyMessage(msg, strlen(msg))

    // ── lock declarations ────────────────────────────────────────────────────
    // OTX_LOCKABLE(type, var)         — use in place of "type var" in a class body
    // OTX_LOCKABLE_N(type, var, desc) — same with an explicit GUI-visible name
    // OTX_LOCK_BASE(type)             — use as the template arg in lock_guard<>
    //                                   or just use CTAD: std::lock_guard g(var);
    // OTX_LOCK_MARK(var)              — optional: mark work site inside a held lock
#   define OTX_LOCKABLE(type, var)            TracyLockable(type, var)
#   define OTX_LOCKABLE_N(type, var, desc)    TracyLockableN(type, var, desc)
#   define OTX_LOCK_BASE(type)                LockableBase(type)
#   define OTX_LOCK_MARK(var)                 LockMark(var)

    // ── condition variable ───────────────────────────────────────────────────
    // std::condition_variable only accepts std::unique_lock<std::mutex>.
    // Tracy wraps the mutex as tracy::Lockable<std::mutex>, so use
    // std::condition_variable_any which accepts any BasicLockable type.
#   define OTX_CONDITION_VARIABLE             std::condition_variable_any
#else
#   define OTX_ZONE()
#   define OTX_ZONE_N(name)
#   define OTX_FRAME()
#   define OTX_FRAME_N(name)
#   define OTX_PLOT(name, val)   (void)(val)
#   define OTX_MESSAGE_L(msg)
#   define OTX_MESSAGE(msg)

#   define OTX_LOCKABLE(type, var)            type var
#   define OTX_LOCKABLE_N(type, var, desc)    type var
#   define OTX_LOCK_BASE(type)                type
#   define OTX_LOCK_MARK(var)
#   define OTX_CONDITION_VARIABLE             std::condition_variable
#endif
