use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use tokio_tungstenite::{connect_async, tungstenite::Message};
use futures_util::{SinkExt, StreamExt};

use market_data::RawTick;
use unified_trading_core::symbol_registry::SymbolId;

#[derive(Debug, Clone)]
pub enum FeedCommand {
    Subscribe { symbol: String },
    Unsubscribe { symbol: String },
}

#[derive(Debug, Clone, PartialEq)]
pub enum FeedError {
    Connect(String),
    Parse(String),
    Subscription(String),
    AuthFailed(String),
    OversizedMessage(String),
    Disconnected,
    Unknown(String),
}

impl std::fmt::Display for FeedError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FeedError::Connect(msg) => write!(f, "Connect: {}", msg),
            FeedError::Parse(msg) => write!(f, "Parse: {}", msg),
            FeedError::Subscription(msg) => write!(f, "Subscription: {}", msg),
            FeedError::AuthFailed(msg) => write!(f, "Auth failed: {}", msg),
            FeedError::OversizedMessage(msg) => write!(f, "Oversized message: {}", msg),
            FeedError::Disconnected => write!(f, "Disconnected"),
            FeedError::Unknown(msg) => write!(f, "Unknown: {}", msg),
        }
    }
}

impl std::error::Error for FeedError {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlpacaAuth {
    pub action: String,
    pub key: String,
    pub secret: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlpacaSubscribe {
    pub action: String,
    pub trades: Vec<String>,
    pub quotes: Vec<String>,
    pub bars: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "T")]
pub enum AlpacaMessage {
    #[serde(rename = "success")]
    Success { msg: String },

    #[serde(rename = "subscription")]
    Subscription {
        trades: Vec<String>,
        quotes: Vec<String>,
        bars: Vec<String>,
    },

    #[serde(rename = "t")]
    Trade(AlpacaTrade),

    #[serde(rename = "q")]
    Quote(AlpacaQuote),

    #[serde(rename = "b")]
    Bar(AlpacaBar),

    #[serde(rename = "error")]
    Error { code: u32, msg: String },

    #[serde(other)]
    Unknown,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AlpacaTrade {
    pub S: String,
    pub i: String,
    pub x: String,
    pub p: f64,
    pub s: u64,
    pub t: String,
    pub z: String,
    pub c: Vec<String>,
    pub tk: Option<String>,
    pub ts: Option<u64>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AlpacaQuote {
    pub S: String,
    pub bx: String,
    pub bp: f64,
    pub bs: u64,
    pub ax: String,
    pub ap: f64,
    pub as_size: u64,
    pub c: Vec<String>,
    pub z: String,
    pub t: String,
    pub tr: Option<u64>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AlpacaBar {
    pub S: String,
    pub o: f64,
    pub h: f64,
    pub l: f64,
    pub c: f64,
    pub v: u64,
    pub t: String,
    pub vw: Option<f64>,
    pub n: Option<u64>,
}

impl AlpacaTrade {
    pub fn to_raw_tick(&self) -> Option<RawTick> {
        let ts = self.ts.unwrap_or(0);
        Some(RawTick {
            symbol_id: SymbolId::from_raw(0),
            timestamp_ns: ts,
            bid: self.p,
            ask: self.p,
            bid_size: self.s,
            ask_size: self.s,
            last_price: self.p,
            last_size: self.s,
            exchange: self.x.clone(),
            trace_id: 0, // Will be assigned by TickReactor
        })
    }
}

impl AlpacaQuote {
    pub fn to_raw_tick(&self) -> Option<RawTick> {
        Some(RawTick {
            symbol_id: SymbolId::from_raw(0),
            timestamp_ns: self.tr.unwrap_or(0),
            bid: self.bp,
            ask: self.ap,
            bid_size: self.bs,
            ask_size: self.as_size,
            last_price: (self.bp + self.ap) / 2.0,
            last_size: self.bs.min(self.as_size),
            exchange: self.bx.clone(),
            trace_id: 0, // Will be assigned by TickReactor
        })
    }
}

#[derive(Clone)]
pub struct AlpacaFeedConfig {
    pub api_key: String,
    pub api_secret: String,
    pub paper_trading: bool,
    pub symbols: Vec<String>,
    pub subscribe_trades: bool,
    pub subscribe_quotes: bool,
    pub subscribe_bars: bool,
    pub replay_buffer_max_bytes: usize,
    pub max_message_size_bytes: usize,
}

impl AlpacaFeedConfig {
    pub fn ws_url(&self) -> String {
        if self.paper_trading {
            "wss://stream.data.alpaca.markets/v2/iex".to_string()
        } else {
            "wss://stream.data.alpaca.markets/v2/iex".to_string()
        }
    }

    pub fn channels(&self) -> Vec<String> {
        let mut channels = Vec::new();
        if self.subscribe_trades {
            for s in &self.symbols {
                channels.push(format!("T.{}", s));
            }
        }
        if self.subscribe_quotes {
            for s in &self.symbols {
                channels.push(format!("Q.{}", s));
            }
        }
        if self.subscribe_bars {
            for s in &self.symbols {
                channels.push(format!("B.{}", s));
            }
        }
        channels
    }
}

pub struct AlpacaWebSocketFeed {
    pub config: AlpacaFeedConfig,
    pub tick_tx: crossbeam_channel::Sender<RawTick>,
    pub running: Arc<AtomicBool>,
    pub connected: Arc<AtomicBool>,
    pub reconnect_delay_ms: u64,
    pub max_reconnect_attempts: u32,
    /// Buffer of recent ticks for replay on reconnect
    pub replay_buffer: parking_lot::Mutex<std::collections::VecDeque<RawTick>>,
    pub max_replay_ticks: usize,
    current_buffer_bytes: std::sync::atomic::AtomicUsize,
    /// Sender for subscription commands (used by the engine to add/remove symbols dynamically).
    pub subscription_cmd_tx: crossbeam_channel::Sender<FeedCommand>,
    /// Tracks the current set of subscribed symbols for reconnect resubscription.
    pub active_symbols: Arc<parking_lot::RwLock<Vec<String>>>,
}

impl AlpacaWebSocketFeed {
    pub fn new(
        config: AlpacaFeedConfig,
        tick_tx: crossbeam_channel::Sender<RawTick>,
    ) -> (Self, crossbeam_channel::Receiver<FeedCommand>) {
        let (sub_tx, sub_rx) = crossbeam_channel::bounded(64);
        let max_bytes = config.replay_buffer_max_bytes.max(1024);
        let active_symbols = Arc::new(parking_lot::RwLock::new(config.symbols.clone()));
        (Self {
            config,
            tick_tx,
            running: Arc::new(AtomicBool::new(false)),
            connected: Arc::new(AtomicBool::new(false)),
            reconnect_delay_ms: 1000,
            max_reconnect_attempts: 10,
            replay_buffer: parking_lot::Mutex::new(std::collections::VecDeque::new()),
            max_replay_ticks: 1000,
            current_buffer_bytes: std::sync::atomic::AtomicUsize::new(0),
            subscription_cmd_tx: sub_tx,
            active_symbols,
        }, sub_rx)
    }

    fn validate_message_size(&self, text: &str) -> Result<(), FeedError> {
        if text.len() > self.config.max_message_size_bytes {
            let error_msg = format!(
                "Message size {} bytes exceeds configured limit of {} bytes",
                text.len(),
                self.config.max_message_size_bytes
            );
            tracing::warn!("{}", error_msg);
            Err(FeedError::OversizedMessage(error_msg))
        } else {
            Ok(())
        }
    }

    pub async fn run(&self, sub_rx: crossbeam_channel::Receiver<FeedCommand>) {
        let mut attempts = 0;
        while self.running.load(Ordering::Relaxed) && attempts < self.max_reconnect_attempts as u64 {
            match self.connect_and_stream(&sub_rx).await {
                Ok(()) => {
                    tracing::info!("Alpaca WebSocket disconnected, reconnecting...");
                    attempts += 1;
                }
                Err(e) => {
                    tracing::error!("Alpaca WebSocket error: {}", e);
                    attempts += 1;
                }
            }

            if self.running.load(Ordering::Relaxed) {
                let delay = self.reconnect_delay_ms * attempts.min(30);
                tracing::info!("Reconnecting in {}ms (attempt {}/{})", delay, attempts, self.max_reconnect_attempts);
                tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
            }
        }

        if attempts >= self.max_reconnect_attempts as u64 {
            tracing::error!("Max reconnect attempts reached, stopping Alpaca feed");
        }
    }

    fn current_channels(&self) -> Vec<String> {
        let symbols = self.active_symbols.read();
        let mut channels = Vec::new();
        if self.config.subscribe_trades {
            for s in symbols.iter() {
                channels.push(format!("T.{}", s));
            }
        }
        if self.config.subscribe_quotes {
            for s in symbols.iter() {
                channels.push(format!("Q.{}", s));
            }
        }
        if self.config.subscribe_bars {
            for s in symbols.iter() {
                channels.push(format!("B.{}", s));
            }
        }
        channels
    }

    fn build_subscribe_message(&self) -> serde_json::Result<String> {
        let symbols = self.active_symbols.read();
        let subscribe = AlpacaSubscribe {
            action: "subscribe".to_string(),
            trades: symbols.iter().map(|s| format!("T.{}", s)).collect(),
            quotes: symbols.iter().map(|s| format!("Q.{}", s)).collect(),
            bars: symbols.iter().map(|s| format!("B.{}", s)).collect(),
        };
        serde_json::to_string(&vec![&subscribe])
    }

    async fn connect_and_stream(&self, sub_rx: &crossbeam_channel::Receiver<FeedCommand>) -> Result<(), FeedError> {
        let ws_url = self.config.ws_url();

        tracing::info!("Connecting to Alpaca WebSocket: {}", ws_url);

        let (ws_stream, _) = connect_async(ws_url.clone())
            .await
            .map_err(|e| FeedError::Connect(format!("WebSocket connect failed: {}", e)))?;

        self.connected.store(true, Ordering::SeqCst);
        tracing::info!("Alpaca WebSocket connected");

        let (mut write, mut read) = ws_stream.split();

        let auth = AlpacaAuth {
            action: "auth".to_string(),
            key: self.config.api_key.clone(),
            secret: self.config.api_secret.clone(),
        };
        let auth_msg = serde_json::to_string(&vec![&auth]).map_err(|e| FeedError::Parse(e.to_string()))?;
        write
            .send(Message::Text(auth_msg.into()))
            .await
            .map_err(|e| FeedError::Connect(format!("Auth send failed: {}", e)))?;
        tracing::info!("Sent auth request");

        loop {
            match read.next().await {
                Some(Ok(Message::Text(text))) => {
                    match self.handle_message(&text).await {
                        Ok(authed) => {
                            if authed {
                                break;
                            }
                        }
                        Err(e) => {
                            tracing::warn!("Message handling error: {}", e);
                        }
                    }
                }
                Some(Ok(Message::Close(_))) => {
                    tracing::info!("Alpaca WebSocket closed by server");
                    return Ok(());
                }
                Some(Ok(Message::Ping(data))) => {
                    let _ = write.send(Message::Pong(data)).await;
                }
                Some(Err(e)) => {
                    return Err(FeedError::Connect(format!("WebSocket read error: {}", e)));
                }
                None => {
                    return Ok(());
                }
                _ => {}
            }
        }

        let channels = self.current_channels();
        if channels.is_empty() {
            return Err(FeedError::Subscription("No channels to subscribe to".to_string()));
        }

        let sub_msg = self.build_subscribe_message().map_err(|e| FeedError::Parse(e.to_string()))?;
        write
            .send(Message::Text(sub_msg.into()))
            .await
            .map_err(|e| FeedError::Connect(format!("Subscribe send failed: {}", e)))?;
        tracing::info!("Sent subscription for {:?}", channels);

        self.replay_ticks();

        let mut sub_interval = tokio::time::interval(std::time::Duration::from_millis(250));
        while self.running.load(Ordering::Relaxed) {
            tokio::select! {
                msg = read.next() => {
                    match msg {
                        Some(Ok(Message::Text(text))) => {
                            if let Err(e) = self.handle_market_data(&text).await {
                                tracing::warn!("Market data error: {}", e);
                            }
                        }
                        Some(Ok(Message::Close(_))) => {
                            tracing::info!("Alpaca WebSocket closed");
                            return Ok(());
                        }
                        Some(Ok(Message::Ping(data))) => {
                            let _ = write.send(Message::Pong(data)).await;
                        }
                        Some(Err(e)) => {
                            return Err(FeedError::Connect(format!("WebSocket read error: {}", e)));
                        }
                        None => {
                            return Ok(());
                        }
                        _ => {}
                    }
                }
                _ = sub_interval.tick() => {
                    while let Ok(cmd) = sub_rx.try_recv() {
                        match cmd {
                            FeedCommand::Subscribe { symbol } => {
                                {
                                    let mut symbols = self.active_symbols.write();
                                    if !symbols.contains(&symbol) {
                                        symbols.push(symbol.clone());
                                    }
                                }
                                // Send subscribe message over the open WebSocket
                                let subscribe = AlpacaSubscribe {
                                    action: "subscribe".to_string(),
                                    trades: vec![format!("T.{}", symbol)],
                                    quotes: vec![format!("Q.{}", symbol)],
                                    bars: vec![format!("B.{}", symbol)],
                                };
                                let msg = serde_json::to_string(&vec![&subscribe])
                                    .map_err(|e| FeedError::Parse(e.to_string()))?;
                                tracing::info!(symbol = %symbol, "Sending dynamic subscribe to Alpaca feed");
                                if let Err(e) = write.send(Message::Text(msg.into())).await {
                                    tracing::warn!(symbol = %symbol, error = %e, "Failed to send subscribe message");
                                }
                            }
                            FeedCommand::Unsubscribe { symbol } => {
                                {
                                    let mut symbols = self.active_symbols.write();
                                    symbols.retain(|s| s != &symbol);
                                }
                                // Send unsubscribe message over the open WebSocket
                                let unsub = AlpacaSubscribe {
                                    action: "unsubscribe".to_string(),
                                    trades: vec![format!("T.{}", symbol)],
                                    quotes: vec![format!("Q.{}", symbol)],
                                    bars: vec![format!("B.{}", symbol)],
                                };
                                let msg = serde_json::to_string(&vec![&unsub])
                                    .map_err(|e| FeedError::Parse(e.to_string()))?;
                                tracing::info!(symbol = %symbol, "Sending dynamic unsubscribe to Alpaca feed");
                                if let Err(e) = write.send(Message::Text(msg.into())).await {
                                    tracing::warn!(symbol = %symbol, error = %e, "Failed to send unsubscribe message");
                                }
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }

    async fn handle_message(&self, text: &str) -> Result<bool, FeedError> {
        self.validate_message_size(text)?;
        let messages: Vec<serde_json::Value> = serde_json::from_str(text)
            .map_err(|e| FeedError::Parse(format!("JSON parse error: {}", e)))?;

        for msg in messages {
            if let Some(arr) = msg.as_array() {
                for item in arr {
                    if let Some(t) = item.get("T").and_then(|v| v.as_str()) {
                        match t {
                            "success" => {
                                if let Some(m) = item.get("msg").and_then(|v| v.as_str()) {
                                    tracing::info!("Alpaca success: {}", m);
                                }
                            }
                            "subscription" => {
                                tracing::info!("Alpaca subscription confirmed");
                                return Ok(true);
                            }
                            "error" => {
                                let code = item.get("code").and_then(|v| v.as_u64()).unwrap_or(0);
                                let msg = item.get("msg").and_then(|v| v.as_str()).unwrap_or("unknown");
                                return Err(FeedError::AuthFailed(format!("Alpaca error {}: {}", code, msg)));
                            }
                            _ => {}
                        }
                    }
                }
            }
        }

        Ok(false)
    }

    fn buffer_tick(&self, tick: RawTick) {
        let tick_bytes = 200;
        let max_bytes = self.config.replay_buffer_max_bytes.max(1024);
        let mut buf = self.replay_buffer.lock();
        let mut current_bytes = self.current_buffer_bytes.load(Ordering::Relaxed);

        while current_bytes + tick_bytes > max_bytes && !buf.is_empty() {
            buf.pop_front();
            current_bytes = current_bytes.saturating_sub(tick_bytes);
        }
        if buf.len() >= self.max_replay_ticks {
            buf.pop_front();
            current_bytes = current_bytes.saturating_sub(tick_bytes);
        }
        buf.push_back(tick);
        self.current_buffer_bytes.store(current_bytes + tick_bytes, Ordering::Relaxed);
    }

    fn replay_ticks(&self) {
        let ticks: Vec<RawTick> = {
            let mut buf = self.replay_buffer.lock();
            buf.drain(..).collect()
        };
        self.current_buffer_bytes.store(0, Ordering::Relaxed);
        if !ticks.is_empty() {
            tracing::info!("Replaying {} buffered ticks post-reconnect", ticks.len());
            for tick in ticks {
                let _ = self.tick_tx.try_send(tick);
            }
        }
    }

    async fn handle_market_data(&self, text: &str) -> Result<(), FeedError> {
        self.validate_message_size(text)?;
        let messages: Vec<serde_json::Value> = serde_json::from_str(text)
            .map_err(|e| FeedError::Parse(format!("JSON parse error: {}", e)))?;

        for msg in messages {
            if let Some(arr) = msg.as_array() {
                for item in arr {
                    if let Some(t) = item.get("T").and_then(|v| v.as_str()) {
                        match t {
                            "t" => {
                                if let Ok(trade) = serde_json::from_value::<AlpacaTrade>(item.clone()) {
                                    if let Some(tick) = trade.to_raw_tick() {
                                        self.buffer_tick(tick.clone());
                                        let _ = self.tick_tx.try_send(tick);
                                    }
                                }
                            }
                            "q" => {
                                if let Ok(quote) = serde_json::from_value::<AlpacaQuote>(item.clone()) {
                                    if let Some(tick) = quote.to_raw_tick() {
                                        self.buffer_tick(tick.clone());
                                        let _ = self.tick_tx.try_send(tick);
                                    }
                                }
                            }
                            "b" => {
                                if let Ok(bar) = serde_json::from_value::<AlpacaBar>(item.clone()) {
                                    let tick = RawTick {
                                        symbol_id: SymbolId::from_raw(0),
                                        timestamp_ns: 0,
                                        bid: bar.o,
                                        ask: bar.c,
                                        bid_size: bar.v,
                                        ask_size: bar.v,
                                        last_price: bar.c,
                                        last_size: bar.v,
                                        exchange: "BAR".to_string(),
                                        trace_id: 0, // Will be assigned by TickReactor
                                    };
                                    self.buffer_tick(tick.clone());
                                    let _ = self.tick_tx.try_send(tick);
                                }
                            }
                            _ => {}
                        }
                    }
                }
            }
        }

        Ok(())
    }

    pub fn start(&self, sub_rx: crossbeam_channel::Receiver<FeedCommand>) -> tokio::task::JoinHandle<()> {
        let feed = Self {
            config: self.config.clone(),
            tick_tx: self.tick_tx.clone(),
            running: Arc::clone(&self.running),
            connected: Arc::clone(&self.connected),
            reconnect_delay_ms: self.reconnect_delay_ms,
            max_reconnect_attempts: self.max_reconnect_attempts,
            replay_buffer: parking_lot::Mutex::new(std::collections::VecDeque::new()),
            max_replay_ticks: self.max_replay_ticks,
            current_buffer_bytes: std::sync::atomic::AtomicUsize::new(0),
            subscription_cmd_tx: self.subscription_cmd_tx.clone(),
            active_symbols: Arc::clone(&self.active_symbols),
        };

        tokio::spawn(async move {
            feed.run(sub_rx).await;
        })
    }

    pub fn stop(&self) {
        self.running.store(false, Ordering::SeqCst);
    }

    pub fn is_connected(&self) -> bool {
        self.connected.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_alpaca_trade_to_raw_tick() {
        let trade = AlpacaTrade {
            S: "AAPL".to_string(),
            i: "123".to_string(),
            x: "V".to_string(),
            p: 150.25,
            s: 100,
            t: "2024-01-01T00:00:00Z".to_string(),
            z: "A".to_string(),
            c: vec!["@".to_string()],
            tk: None,
            ts: Some(1704067200000000000),
        };

        let tick = trade.to_raw_tick().unwrap();
        assert_eq!(tick.symbol_id, SymbolId::from_raw(0)); // Would need symbol registry in real scenario
        assert_eq!(tick.last_price, 150.25);
        assert_eq!(tick.last_size, 100);
    }

    #[test]
    fn test_alpaca_quote_to_raw_tick() {
        let quote = AlpacaQuote {
            S: "MSFT".to_string(),
            bx: "V".to_string(),
            bp: 400.0,
            bs: 50,
            ax: "V".to_string(),
            ap: 400.05,
            as_size: 75,
            c: vec!["R".to_string()],
            z: "A".to_string(),
            t: "2024-01-01T00:00:00Z".to_string(),
            tr: Some(1704067200000000000),
        };

        let tick = quote.to_raw_tick().unwrap();
        assert_eq!(tick.symbol_id, SymbolId::from_raw(0)); // Would need symbol registry in real scenario
        assert_eq!(tick.bid, 400.0);
        assert_eq!(tick.ask, 400.05);
        assert_eq!(tick.bid_size, 50);
        assert_eq!(tick.ask_size, 75);
    }

    #[test]
    fn test_feed_config_channels() {
        let config = AlpacaFeedConfig {
            api_key: "key".to_string(),
            api_secret: "secret".to_string(),
            paper_trading: true,
            symbols: vec!["AAPL".to_string(), "MSFT".to_string()],
            subscribe_trades: true,
            subscribe_quotes: false,
            subscribe_bars: false,
            replay_buffer_max_bytes: 10 * 1024 * 1024,
            max_message_size_bytes: 1024 * 1024,
        };

        let channels = config.channels();
        assert_eq!(channels.len(), 2);
        assert_eq!(channels[0], "T.AAPL");
        assert_eq!(channels[1], "T.MSFT");
    }

    #[test]
    fn test_feed_config_all_channels() {
        let config = AlpacaFeedConfig {
            api_key: "key".to_string(),
            api_secret: "secret".to_string(),
            paper_trading: true,
            symbols: vec!["AAPL".to_string()],
            subscribe_trades: true,
            subscribe_quotes: true,
            subscribe_bars: true,
            replay_buffer_max_bytes: 10 * 1024 * 1024,
            max_message_size_bytes: 1024 * 1024,
        };

        let channels = config.channels();
        assert_eq!(channels.len(), 3);
        assert_eq!(channels[0], "T.AAPL");
        assert_eq!(channels[1], "Q.AAPL");
        assert_eq!(channels[2], "B.AAPL");
    }

    #[test]
    fn test_feed_config_ws_url() {
        let config = AlpacaFeedConfig {
            api_key: "key".to_string(),
            api_secret: "secret".to_string(),
            paper_trading: true,
            symbols: vec![],
            subscribe_trades: false,
            subscribe_quotes: false,
            subscribe_bars: false,
            replay_buffer_max_bytes: 10 * 1024 * 1024,
            max_message_size_bytes: 1024 * 1024,
        };
        assert!(config.ws_url().contains("alpaca.markets"));
    }

    #[test]
    fn test_alpaca_message_deserialize_trade() {
        let json = r#"[{"T":"t","S":"AAPL","i":"123","x":"V","p":150.25,"s":100,"t":"2024-01-01T00:00:00Z","z":"A","c":["@"]}]"#;
        let messages: Vec<serde_json::Value> = serde_json::from_str(json).unwrap();
        assert_eq!(messages.len(), 1);
        let t = messages[0].get("T").unwrap().as_str().unwrap();
        assert_eq!(t, "t");
    }

    #[test]
    fn test_alpaca_message_deserialize_success() {
        let json = r#"[{"T":"success","msg":"authenticated"}]"#;
        let messages: Vec<serde_json::Value> = serde_json::from_str(json).unwrap();
        let t = messages[0].get("T").unwrap().as_str().unwrap();
        assert_eq!(t, "success");
    }

    #[test]
    fn test_oversized_message_rejection() {
        // Create a config with a small max message size (100 bytes)
        let config = AlpacaFeedConfig {
            api_key: "key".to_string(),
            api_secret: "secret".to_string(),
            paper_trading: true,
            symbols: vec!["AAPL".to_string()],
            subscribe_trades: true,
            subscribe_quotes: true,
            subscribe_bars: true,
            replay_buffer_max_bytes: 10 * 1024 * 1024,
            max_message_size_bytes: 100,
        };

        let (tick_tx, _tick_rx) = crossbeam_channel::unbounded();
        let (feed, _sub_rx) = AlpacaWebSocketFeed::new(config, tick_tx);

        // Create a message that's larger than 100 bytes
        let oversized_message = "x".repeat(150);
        
        // Test validation method directly
        let validation_result = feed.validate_message_size(&oversized_message);
        assert!(validation_result.is_err());
        match validation_result.unwrap_err() {
            FeedError::OversizedMessage(msg) => assert!(msg.contains("exceeds configured limit")),
            _ => panic!("Expected OversizedMessage error"),
        }

        // Test with a message that's within the limit
        let small_message = "x".repeat(50);
        let validation_result = feed.validate_message_size(&small_message);
        assert!(validation_result.is_ok());
    }
}
