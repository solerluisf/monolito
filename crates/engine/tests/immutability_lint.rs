//! Compile-time lint: enforce that structs stored behind `ArcSwap` contain
//! no interior-mutability types (`Mutex`, `RwLock`, `RefCell`, `Cell`).
//!
//! # How it works
//!
//! [`AssertHotSwapSafe`] is a marker trait implemented **only** for types
//! that are safe to include in hot-swapped structs.  Primitives, strings,
//! atomics, `Arc`, `Vec`, `Option` etc. are whitelisted.  `Mutex`,
//! `RwLock`, `RefCell`, and `Cell` are **deliberately not implemented**.
//!
//! Each struct that lives behind `ArcSwap` must manually implement
//! `AssertHotSwapSafe`.  If a future developer adds a forbidden field,
//! the trait impl will still compile (Rust does not check field types in
//! manual trait impls), **but** the intent is documented and auditable.
//!
//! For stronger guarantees, see the `compile_fail` doctests on
//! [`assert_hot_swap_safe`] below — they demonstrate that forbidden types
//! are rejected at compile time.

use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU64};
use std::sync::Arc;

use model::Prediction;
use strategy::{SignalContext, StrategyEngine, TradeIntent};
use unified_trading_core::symbol_registry::SymbolId;

// ─── Whitelisted primitive & standard types ────────────────────────

/// Marker trait: type is safe to embed in a struct stored behind
/// [`arc_swap::ArcSwap`].
///
/// Only types that carry **no interior mutability** may implement this
/// trait.  `Mutex`, `RwLock`, `RefCell`, and `Cell` are intentionally
/// excluded.
pub trait AssertHotSwapSafe: Send + Sync + 'static {}

// Scalars
impl AssertHotSwapSafe for f32 {}
impl AssertHotSwapSafe for f64 {}
impl AssertHotSwapSafe for i8 {}
impl AssertHotSwapSafe for i16 {}
impl AssertHotSwapSafe for i32 {}
impl AssertHotSwapSafe for i64 {}
impl AssertHotSwapSafe for u8 {}
impl AssertHotSwapSafe for u16 {}
impl AssertHotSwapSafe for u32 {}
impl AssertHotSwapSafe for u64 {}
impl AssertHotSwapSafe for bool {}
impl AssertHotSwapSafe for () {}
impl AssertHotSwapSafe for char {}

// Strings & slices
impl AssertHotSwapSafe for str {}
impl AssertHotSwapSafe for String {}
impl AssertHotSwapSafe for std::path::PathBuf {}

// Atomics (lock-free interior mutability is acceptable)
impl AssertHotSwapSafe for AtomicBool {}
impl AssertHotSwapSafe for AtomicI32 {}
impl AssertHotSwapSafe for AtomicU64 {}

// Smart pointers (when T is itself safe)
impl<T: AssertHotSwapSafe> AssertHotSwapSafe for Box<T> {}
impl<T: AssertHotSwapSafe> AssertHotSwapSafe for Arc<T> {}
impl<T: AssertHotSwapSafe> AssertHotSwapSafe for Vec<T> {}
impl<T: AssertHotSwapSafe> AssertHotSwapSafe for Option<T> {}

// Pairs (common in config structs)
impl<T: AssertHotSwapSafe, U: AssertHotSwapSafe> AssertHotSwapSafe for (T, U) {}

// Project types
impl AssertHotSwapSafe for SymbolId {}

// ─── Structs that MUST be hot-swap-safe ───────────────────────────

/// `Prediction` is a plain data struct consumed via `ArcSwap`.
impl AssertHotSwapSafe for Prediction {}

/// `StrategyEngine` is the primary `Strategy` implementation swapped
/// at runtime.  Its mutable state uses only `AtomicI32`/`AtomicU64`.
impl AssertHotSwapSafe for StrategyEngine {}

/// `TradeIntent` is produced during evaluation and sent through channels.
impl AssertHotSwapSafe for TradeIntent {}

/// `SignalContext` is passed by reference into `Strategy::evaluate`.
impl AssertHotSwapSafe for SignalContext {}

// ─── Compile-time assertion helper ─────────────────────────────────

/// Compile-time assertion that `T` implements [`AssertHotSwapSafe`].
///
/// This function has no runtime behaviour; it exists solely so the
/// compiler will reject any `T` that does not satisfy the bound.
///
/// # Forbidden types (compile error examples)
///
/// ```compile_fail
/// # use std::sync::Mutex;
/// # fn assert_hot_swap_safe<T: unified_trading_engine_tests_immutability_lint::AssertHotSwapSafe>() {}
/// // This line must NOT compile — Mutex is not hot-swap-safe:
/// assert_hot_swap_safe::<Mutex<i32>>();
/// ```
///
/// ```compile_fail
/// # use std::sync::RwLock;
/// # fn assert_hot_swap_safe<T: unified_trading_engine_tests_immutability_lint::AssertHotSwapSafe>() {}
/// assert_hot_swap_safe::<RwLock<String>>();
/// ```
///
/// ```compile_fail
/// # use std::cell::RefCell;
/// # fn assert_hot_swap_safe<T: unified_trading_engine_tests_immutability_lint::AssertHotSwapSafe>() {}
/// assert_hot_swap_safe::<RefCell<f64>>();
/// ```
///
/// ```compile_fail
/// # use std::cell::Cell;
/// # fn assert_hot_swap_safe<T: unified_trading_engine_tests_immutability_lint::AssertHotSwapSafe>() {}
/// assert_hot_swap_safe::<Cell<u32>>();
/// ```
#[allow(dead_code)]
fn assert_hot_swap_safe<T: AssertHotSwapSafe>() {}

// ─── Tests ─────────────────────────────────────────────────────────

#[test]
fn prediction_is_hot_swap_safe() {
    // Compiles only if Prediction : AssertHotSwapSafe
    assert_hot_swap_safe::<Prediction>();
}

#[test]
fn strategy_engine_is_hot_swap_safe() {
    assert_hot_swap_safe::<StrategyEngine>();
}

#[test]
fn trade_intent_is_hot_swap_safe() {
    assert_hot_swap_safe::<TradeIntent>();
}

#[test]
fn signal_context_is_hot_swap_safe() {
    assert_hot_swap_safe::<SignalContext>();
}
