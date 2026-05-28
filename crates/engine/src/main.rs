mod allocator;

use std::sync::Arc;

use unified_trading_core::config::EngineConfig;
use unified_trading_core::ws::{create_ws_router, WsState};
use unified_trading_core::large_pages::{enable_large_pages, log_large_page_result};
use unified_trading_core::CrashDetector;
use unified_trading_engine::{UnifiedEngine, ApiState, create_router};

fn load_config() -> EngineConfig {
    let config_path = std::env::var("TRADING_CONFIG")
        .unwrap_or_else(|_| "config.toml".to_string());

    if std::path::Path::new(&config_path).exists() {
        match std::fs::read_to_string(&config_path) {
            Ok(contents) => {
                match toml::from_str::<EngineConfig>(&contents) {
                    Ok(config) => {
                        tracing::info!("Loaded configuration from {}", config_path);
                        return config;
                    }
                    Err(e) => {
                        tracing::warn!("Failed to parse {}: {}, falling back to defaults", config_path, e);
                    }
                }
            }
            Err(e) => {
                tracing::warn!("Failed to read {}: {}, falling back to defaults", config_path, e);
            }
        }
    }

    let mut config = EngineConfig::default();

    if let Ok(api_key) = std::env::var("ALPACA_API_KEY") {
        config.broker_config.api_key = unified_trading_core::config::SecretString::new(api_key);
    }
    if let Ok(api_secret) = std::env::var("ALPACA_API_SECRET") {
        config.broker_config.api_secret = unified_trading_core::config::SecretString::new(api_secret);
    }
    if let Ok(paper) = std::env::var("ALPACA_PAPER") {
        config.broker_config.paper_trading = paper.parse().unwrap_or(true);
    }
    if let Ok(symbols) = std::env::var("TRADING_SYMBOLS") {
        config.asset_configs = symbols.split(',')
            .map(|s| unified_trading_core::config::AssetConfig {
                symbol: s.trim().to_string(),
                enabled: true,
                max_position: 100.0,
                tick_size: 0.01,
            })
            .collect();
    }
    if let Ok(log_level) = std::env::var("RUST_LOG") {
        tracing::info!("Log level set via RUST_LOG: {}", log_level);
    }

    tracing::info!("Using default configuration with environment overrides");
    config
}

fn main() {
    let log_level = std::env::var("RUST_LOG")
        .unwrap_or_else(|_| "info".to_string());

    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| log_level.parse().unwrap());

    tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .init();

    // Attempt to enable large pages before any heavy allocation
    let lp_result = enable_large_pages();
    log_large_page_result(&lp_result);

    tracing::info!("Unified Trading Engine starting");

    let crash_detector = CrashDetector::new(
        std::path::PathBuf::from("crash_state.json")
    );
    let enter_safe_mode = crash_detector.check_and_record_startup();

    let mut config = load_config();
    if enter_safe_mode {
        tracing::error!("Crash loop detected. Entering safe mode.");
        if let Some(reverted) = crash_detector.get_last_known_good_config() {
            tracing::info!("Reverting to last-known-good configuration");
            config = reverted;
        }
        config.safe_mode = true;
        for asset in &mut config.asset_configs {
            asset.enabled = false;
        }
    }
    let engine = Arc::new(parking_lot::Mutex::new(UnifiedEngine::new(config)));

    let (kill_switch, metrics, command_tx, config, portfolio_manager, strategy_registry, heartbeats, execution_states) = {
        let e = engine.lock();
        let kill_switch = Arc::clone(&e.kill_switch);
        let metrics = Arc::clone(&e.metrics);
        let command_tx = e.command_channel.tx.clone();
        let config = Arc::clone(&e.config);
        let portfolio_manager = Arc::clone(&e.portfolio_manager);
        let strategy_registry = Arc::clone(&e.strategy_registry);
        let heartbeats = e.heartbeats.clone();
        let execution_states = {
            let states = e.execution_states.clone();
            Arc::new(parking_lot::Mutex::new(states))
        };
        (kill_switch, metrics, command_tx, config, portfolio_manager, strategy_registry, heartbeats, execution_states)
    };

    {
        let mut e = engine.lock();
        e.start();
    }

    // Startup succeeded — save working config and clear crash counter
    crash_detector.save_working_config(&config.read());
    crash_detector.clear();

    let command_actor = UnifiedEngine::start_command_actor(Arc::clone(&engine));
    {
        let mut e = engine.lock();
        e.command_actor = Some(command_actor);
    }

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("Failed to create tokio runtime");

    rt.block_on(async {
        let api_key = config.read().api_key.clone();
        let rate_limit = config.read().control_plane_rate_limit;

        let api_state = ApiState {
            kill_switch: Arc::clone(&kill_switch),
            metrics: Arc::clone(&metrics),
            command_tx: command_tx.clone(),
            config: Arc::clone(&config),
            portfolio_manager: Arc::clone(&portfolio_manager),
            heartbeats: heartbeats.clone(),
            execution_states: Arc::clone(&execution_states),
            strategy_registry: Arc::clone(&strategy_registry),
            model_registry: Arc::new(model::ModelRegistry::new()),
            api_key,
            rate_limiter: unified_trading_engine::SimpleRateLimiter::new(rate_limit, 1),
        };

        let ws_state = WsState {
            metrics: Arc::clone(&metrics),
            kill_switch: Arc::clone(&kill_switch),
        };

        let api_app = create_router(api_state);
        let ws_app = create_ws_router(ws_state);

        let api_listener = tokio::net::TcpListener::bind("127.0.0.1:9090")
            .await
            .expect("Failed to bind API port");
        let ws_listener = tokio::net::TcpListener::bind("127.0.0.1:9091")
            .await
            .expect("Failed to bind WebSocket port");

        tracing::info!("REST API listening on http://127.0.0.1:9090");
        tracing::info!("WebSocket telemetry on ws://127.0.0.1:9091/ws/telemetry");

        let mut api_handle = tokio::spawn(async move {
            axum::serve(api_listener, api_app).await.unwrap();
        });

        let mut ws_handle = tokio::spawn(async move {
            axum::serve(ws_listener, ws_app).await.unwrap();
        });

        loop {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {
                    tracing::info!("Ctrl+C received, initiating graceful shutdown...");
                    kill_switch.activate();
                    break;
                }
                _ = &mut api_handle => {
                    tracing::warn!("API server exited unexpectedly");
                    break;
                }
                _ = &mut ws_handle => {
                    tracing::warn!("WebSocket server exited unexpectedly");
                    break;
                }
                _ = tokio::time::sleep(std::time::Duration::from_secs(1)) => {
                    if kill_switch.is_active() {
                        tracing::info!("Kill switch active, shutting down");
                        break;
                    }
                }
            }
        }

        {
            let mut e = engine.lock();
            e.shutdown();
        }

        crash_detector.save_working_config(&config.read());
        crash_detector.clear();

        api_handle.abort();
        ws_handle.abort();

        tracing::info!("Unified Trading Engine stopped");
    });
}
