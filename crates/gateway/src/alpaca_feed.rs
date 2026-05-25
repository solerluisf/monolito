use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use tokio_tungstenite::{connect_async, tungstenite::Message};
use futures_util::{SinkExt, StreamExt};

use market_data::RawTick;

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
            symbol: self.S.clone(),
            timestamp_ns: ts,
            bid: self.p,
            ask: self.p,
            bid_size: self.s,
            ask_size: self.s,
            last_price: self.p,
            last_size: self.s,
            exchange: self.x.clone(),
        })
    }
}

impl AlpacaQuote {
    pub fn to_raw_tick(&self) -> Option<RawTick> {
        Some(RawTick {
            symbol: self.S.clone(),
            timestamp_ns: self.tr.unwrap_or(0),
            bid: self.bp,
            ask: self.ap,
            bid_size: self.bs,
            ask_size: self.as_size,
            last_price: (self.bp + self.ap) / 2.0,
            last_size: self.bs.min(self.as_size),
            exchange: self.bx.clone(),
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
}

impl AlpacaWebSocketFeed {
    pub fn new(
        config: AlpacaFeedConfig,
        tick_tx: crossbeam_channel::Sender<RawTick>,
    ) -> Self {
        Self {
            config,
            tick_tx,
            running: Arc::new(AtomicBool::new(false)),
            connected: Arc::new(AtomicBool::new(false)),
            reconnect_delay_ms: 1000,
            max_reconnect_attempts: 10,
        }
    }

    pub async fn run(&self) {
        let mut attempts = 0;
        while self.running.load(Ordering::Relaxed) && attempts < self.max_reconnect_attempts as u64 {
            match self.connect_and_stream().await {
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

    async fn connect_and_stream(&self) -> Result<(), String> {
        let ws_url = self.config.ws_url();

        tracing::info!("Connecting to Alpaca WebSocket: {}", ws_url);

        let (ws_stream, _) = connect_async(ws_url.clone())
            .await
            .map_err(|e| format!("WebSocket connect failed: {}", e))?;

        self.connected.store(true, Ordering::SeqCst);
        tracing::info!("Alpaca WebSocket connected");

        let (mut write, mut read) = ws_stream.split();

        let auth = AlpacaAuth {
            action: "auth".to_string(),
            key: self.config.api_key.clone(),
            secret: self.config.api_secret.clone(),
        };
        let auth_msg = serde_json::to_string(&vec![&auth]).map_err(|e| e.to_string())?;
        write
            .send(Message::Text(auth_msg.into()))
            .await
            .map_err(|e| format!("Auth send failed: {}", e))?;
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
                    return Err(format!("WebSocket read error: {}", e));
                }
                None => {
                    return Ok(());
                }
                _ => {}
            }
        }

        let channels = self.config.channels();
        if channels.is_empty() {
            return Err("No channels to subscribe to".to_string());
        }

        let subscribe = AlpacaSubscribe {
            action: "subscribe".to_string(),
            trades: self.config.symbols.iter().map(|s| format!("T.{}", s)).collect(),
            quotes: self.config.symbols.iter().map(|s| format!("Q.{}", s)).collect(),
            bars: self.config.symbols.iter().map(|s| format!("B.{}", s)).collect(),
        };
        let sub_msg = serde_json::to_string(&vec![&subscribe]).map_err(|e| e.to_string())?;
        write
            .send(Message::Text(sub_msg.into()))
            .await
            .map_err(|e| format!("Subscribe send failed: {}", e))?;
        tracing::info!("Sent subscription for {:?}", channels);

        while self.running.load(Ordering::Relaxed) {
            match read.next().await {
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
                    return Err(format!("WebSocket read error: {}", e));
                }
                None => {
                    return Ok(());
                }
                _ => {}
            }
        }

        Ok(())
    }

    async fn handle_message(&self, text: &str) -> Result<bool, String> {
        let messages: Vec<serde_json::Value> = serde_json::from_str(text)
            .map_err(|e| format!("JSON parse error: {}", e))?;

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
                                return Err(format!("Alpaca error {}: {}", code, msg));
                            }
                            _ => {}
                        }
                    }
                }
            }
        }

        Ok(false)
    }

    async fn handle_market_data(&self, text: &str) -> Result<(), String> {
        let messages: Vec<serde_json::Value> = serde_json::from_str(text)
            .map_err(|e| format!("JSON parse error: {}", e))?;

        for msg in messages {
            if let Some(arr) = msg.as_array() {
                for item in arr {
                    if let Some(t) = item.get("T").and_then(|v| v.as_str()) {
                        match t {
                            "t" => {
                                if let Ok(trade) = serde_json::from_value::<AlpacaTrade>(item.clone()) {
                                    if let Some(tick) = trade.to_raw_tick() {
                                        let _ = self.tick_tx.try_send(tick);
                                    }
                                }
                            }
                            "q" => {
                                if let Ok(quote) = serde_json::from_value::<AlpacaQuote>(item.clone()) {
                                    if let Some(tick) = quote.to_raw_tick() {
                                        let _ = self.tick_tx.try_send(tick);
                                    }
                                }
                            }
                            "b" => {
                                if let Ok(bar) = serde_json::from_value::<AlpacaBar>(item.clone()) {
                                    let tick = RawTick {
                                        symbol: bar.S.clone(),
                                        timestamp_ns: 0,
                                        bid: bar.o,
                                        ask: bar.c,
                                        bid_size: bar.v,
                                        ask_size: bar.v,
                                        last_price: bar.c,
                                        last_size: bar.v,
                                        exchange: "BAR".to_string(),
                                    };
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

    pub fn start(&self) -> tokio::task::JoinHandle<()> {
        let feed = Self {
            config: self.config.clone(),
            tick_tx: self.tick_tx.clone(),
            running: Arc::clone(&self.running),
            connected: Arc::clone(&self.connected),
            reconnect_delay_ms: self.reconnect_delay_ms,
            max_reconnect_attempts: self.max_reconnect_attempts,
        };

        tokio::spawn(async move {
            feed.run().await;
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
        assert_eq!(tick.symbol, "AAPL");
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
        assert_eq!(tick.symbol, "MSFT");
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
}
