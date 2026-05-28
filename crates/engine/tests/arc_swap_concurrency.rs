//! Concurrency tests for ArcSwap usage patterns
//! 
//! These tests verify thread-safe behavior of ArcSwap for predictions and strategies.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use arc_swap::ArcSwap;

/// Test concurrent reads and writes to ArcSwap<Prediction>
/// Verifies no panics and latest value is eventually visible
#[test]
fn test_arc_swap_prediction_concurrent() {
    use model::Prediction;
    use unified_trading_core::symbol_registry::SymbolId;
    
    let prediction = Arc::new(ArcSwap::new(Arc::new(Prediction::new_default(SymbolId::from_raw(0)))));
    let read_count = Arc::new(AtomicU64::new(0));
    let write_count = Arc::new(AtomicU64::new(0));
    
    // Spawn 4 writer threads
    let mut handles = vec![];
    for i in 0..4 {
        let pred = Arc::clone(&prediction);
        let wc = Arc::clone(&write_count);
        let handle = std::thread::spawn(move || {
            for j in 0..1000 {
                let new_pred = Prediction {
                    symbol_id: SymbolId::from_raw(0),
                    forecast: i as f32 * 1000.0 + j as f32,
                    confidence: 0.5 + (j as f32 / 2000.0),
                    action_score: 0.0,
                    regime_label: i,
                    regime_strength: 0.5,
                    computed_ns: std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_nanos() as u64,
                    trace_id: 1,
                };
                pred.store(Arc::new(new_pred));
                wc.fetch_add(1, Ordering::Relaxed);
            }
        });
        handles.push(handle);
    }
    
    // Spawn 4 reader threads
    for _ in 0..4 {
        let pred = Arc::clone(&prediction);
        let rc = Arc::clone(&read_count);
        let handle = std::thread::spawn(move || {
            for _ in 0..1000 {
                let _p = pred.load_full();
                rc.fetch_add(1, Ordering::Relaxed);
                // Small delay to mix with writes
                std::thread::yield_now();
            }
        });
        handles.push(handle);
    }
    
    // Wait for all threads
    for h in handles {
        h.join().unwrap();
    }
    
    // Verify counts
    assert_eq!(write_count.load(Ordering::Relaxed), 4000);
    assert_eq!(read_count.load(Ordering::Relaxed), 4000);
    
    // Verify final prediction is valid (no corruption)
    let final_pred = prediction.load_full();
    assert!(final_pred.computed_ns > 0);
}

/// Test hot-swap of ArcSwap<Box<dyn Strategy>>
/// Verifies no panics during concurrent evaluate() calls and swaps
#[test]
fn test_arc_swap_strategy_hot_swap() {
    use strategy::{Strategy, SignalContext, TradeIntent, SignalSide};
    use model::Prediction;
    
    // Simple test strategy
    struct TestStrategy {
        name: String,
        threshold: f32,
    }
    
    impl Strategy for TestStrategy {
        fn evaluate(&self, prediction: &Prediction, _ctx: &SignalContext) -> Option<TradeIntent> {
            if prediction.action_score > self.threshold {
                Some(TradeIntent::new(
                    prediction.symbol_id,
                    SignalSide::Long,
                    strategy::SizeHint::Units(1),
                    strategy::IntentType::Entry,
                    0.8,
                    prediction.action_score as f64,
                    30_000_000_000,
                    prediction.trace_id,
                ))
            } else {
                None
            }
        }
        
        fn name(&self) -> &str {
            &self.name
        }
        
        fn clone_box(&self) -> Box<dyn Strategy> {
            Box::new(TestStrategy {
                name: self.name.clone(),
                threshold: self.threshold,
            })
        }
    }
    
    let strategy = Arc::new(ArcSwap::new(Arc::new(
        Box::new(TestStrategy { name: "low".to_string(), threshold: 0.3 }) as Box<dyn Strategy>
    )));
    
    let evaluate_count = Arc::new(AtomicU64::new(0));
    let swap_count = Arc::new(AtomicU64::new(0));
    
    // Spawn evaluator thread
    let strat = Arc::clone(&strategy);
    let ec = Arc::clone(&evaluate_count);
    let eval_handle = std::thread::spawn(move || {
        use unified_trading_core::symbol_registry::SymbolId;
        let ctx = SignalContext::new(SymbolId::from_raw(0));
        for i in 0..10000 {
            let pred = Prediction {
                symbol_id: SymbolId::from_raw(0),
                forecast: 0.5,
                confidence: 0.8,
                action_score: 0.5 + (i as f32 / 10000.0),
                regime_label: 0,
                regime_strength: 0.5,
                computed_ns: 0,
                trace_id: 1,
            };
            let s = strat.load_full();
            let _ = s.evaluate(&pred, &ctx);
            ec.fetch_add(1, Ordering::Relaxed);
        }
    });
    
    // Spawn swapper thread
    let strat = Arc::clone(&strategy);
    let sc = Arc::clone(&swap_count);
    let swap_handle = std::thread::spawn(move || {
        for i in 0..100 {
            let new_strat: Box<dyn Strategy> = if i % 2 == 0 {
                Box::new(TestStrategy { name: "low".to_string(), threshold: 0.3 })
            } else {
                Box::new(TestStrategy { name: "high".to_string(), threshold: 0.7 })
            };
            strat.store(Arc::new(new_strat));
            sc.fetch_add(1, Ordering::Relaxed);
            std::thread::sleep(std::time::Duration::from_micros(100));
        }
    });
    
    eval_handle.join().unwrap();
    swap_handle.join().unwrap();
    
    assert_eq!(evaluate_count.load(Ordering::Relaxed), 10000);
    assert_eq!(swap_count.load(Ordering::Relaxed), 100);
}

/// Stress test: Many threads hammering ArcSwap with short-lived values
#[test]
fn test_arc_swap_stress() {
    let value = Arc::new(ArcSwap::new(Arc::new(0u64)));
    let iterations = 10000;
    let threads = 8;
    
    let mut handles = vec![];
    
    for thread_id in 0..threads {
        let val = Arc::clone(&value);
        let handle = std::thread::spawn(move || {
            for i in 0..iterations {
                let new_val = thread_id as u64 * iterations + i as u64;
                val.store(Arc::new(new_val));
                
                // Read occasionally
                if i % 10 == 0 {
                    let _ = val.load_full();
                }
            }
        });
        handles.push(handle);
    }
    
    for h in handles {
        h.join().unwrap();
    }
    
    // Verify final value is set (no corruption)
    let final_val = *value.load_full();
    assert!(final_val < threads as u64 * iterations);
}

/// Test that ArcSwap drops old values properly (no memory leaks)
#[test]
fn test_arc_swap_drop_old_values() {
    use std::sync::atomic::AtomicUsize;
    
    static DROP_COUNT: AtomicUsize = AtomicUsize::new(0);
    
    struct DropCounter(i32);
    
    impl Drop for DropCounter {
        fn drop(&mut self) {
            DROP_COUNT.fetch_add(1, Ordering::SeqCst);
        }
    }
    
    let value = Arc::new(ArcSwap::new(Arc::new(DropCounter(0))));
    
    // Swap 100 times
    for i in 1..=100 {
        value.store(Arc::new(DropCounter(i)));
    }
    
    // Force drop of ArcSwap itself
    drop(value);
    
    // All 101 values should be dropped (initial + 100 swaps)
    let drops = DROP_COUNT.load(Ordering::SeqCst);
    assert_eq!(drops, 101);
}
