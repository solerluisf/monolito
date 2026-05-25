use crossbeam_channel::{bounded, Receiver, Sender};
use std::thread;

#[derive(Debug, Clone)]
pub enum ControlCommand {
    SetKillSwitch(bool),
    UpdateConfig(String),
    PauseAsset(String),
    ResumeAsset(String),
    SetMode(String),
    SwapStrategy { symbol: String, strategy_type: String },
    GetStatus,
    Shutdown,
}

#[derive(Debug, Clone)]
pub enum ControlResponse {
    Ok,
    Error(String),
    Status(String),
}

pub struct CommandChannel {
    pub tx: Sender<ControlCommand>,
    pub rx: Receiver<ControlCommand>,
}

impl CommandChannel {
    pub fn new(capacity: usize) -> Self {
        let (tx, rx) = bounded(capacity);
        Self { tx, rx }
    }
}

pub struct CommandActor {
    handle: Option<thread::JoinHandle<()>>,
    running: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

impl CommandActor {
    pub fn new<F>(rx: Receiver<ControlCommand>, mut handler: F) -> Self
    where
        F: FnMut(ControlCommand) -> ControlResponse + Send + 'static,
    {
        let running = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true));
        let r = Arc::clone(&running);

        let handle = thread::spawn(move || {
            while r.load(std::sync::atomic::Ordering::Relaxed) {
                match rx.recv_timeout(std::time::Duration::from_millis(10)) {
                    Ok(cmd) => {
                        let _resp = handler(cmd);
                    }
                    Err(crossbeam_channel::RecvTimeoutError::Timeout) => {}
                    Err(crossbeam_channel::RecvTimeoutError::Disconnected) => break,
                }
            }
        });

        Self {
            handle: Some(handle),
            running,
        }
    }

    pub fn shutdown(&mut self) {
        self.running.store(false, std::sync::atomic::Ordering::SeqCst);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

use std::sync::Arc;

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    #[test]
    fn test_command_channel_send_recv() {
        let channel = CommandChannel::new(100);
        channel
            .tx
            .send(ControlCommand::SetKillSwitch(true))
            .unwrap();
        let cmd = channel.rx.recv().unwrap();
        assert!(matches!(cmd, ControlCommand::SetKillSwitch(true)));
    }

    #[test]
    fn test_command_actor_processes_commands() {
        let channel = CommandChannel::new(100);
        let count = Arc::new(AtomicU64::new(0));
        let c = Arc::clone(&count);

        let mut actor = CommandActor::new(channel.rx, move |cmd| {
            if matches!(cmd, ControlCommand::SetKillSwitch(_)) {
                c.fetch_add(1, Ordering::Relaxed);
            }
            ControlResponse::Ok
        });

        channel
            .tx
            .send(ControlCommand::SetKillSwitch(true))
            .unwrap();
        channel
            .tx
            .send(ControlCommand::SetKillSwitch(false))
            .unwrap();

        std::thread::sleep(std::time::Duration::from_millis(50));
        assert_eq!(count.load(Ordering::Relaxed), 2);

        actor.shutdown();
    }
}
