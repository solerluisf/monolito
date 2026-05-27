use std::thread;

#[cfg(windows)]
use windows::Win32::System::Threading::{
    SetThreadAffinityMask, SetThreadPriority,
    THREAD_PRIORITY_BELOW_NORMAL,
    THREAD_PRIORITY_HIGHEST,
    THREAD_PRIORITY_NORMAL,
    THREAD_PRIORITY_TIME_CRITICAL,
};

pub enum ThreadPriority {
    BelowNormal,
    Normal,
    High,
    TimeCritical,
}

#[derive(Debug)]
pub struct PinError {
    pub message: String,
}

impl std::fmt::Display for PinError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for PinError {}

pub fn pin_to_core(core_id: usize) -> Result<(), PinError> {
    #[cfg(windows)]
    {
        let current_thread = unsafe { windows::Win32::System::Threading::GetCurrentThread() };
        let mask = 1usize << core_id;
        let result = unsafe { SetThreadAffinityMask(current_thread, mask) };
        if result == 0 {
            return Err(PinError {
                message: format!("Failed to pin thread to core {}", core_id),
            });
        }
        Ok(())
    }
    #[cfg(target_os = "linux")]
    {
        let thread_id = unsafe { libc::pthread_self() };
        let mut mask: libc::cpu_set_t = unsafe { std::mem::zeroed() };
        let result = unsafe {
            libc::CPU_ZERO(&mut mask);
            libc::CPU_SET(core_id, &mut mask);
            libc::pthread_setaffinity_np(
                thread_id,
                std::mem::size_of::<libc::cpu_set_t>(),
                &mask,
            )
        };
        if result != 0 {
            return Err(PinError {
                message: format!("Failed to pin thread to core {} (errno: {})", core_id, result),
            });
        }
        Ok(())
    }
    #[cfg(all(unix, not(target_os = "linux")))]
    {
        Err(PinError {
            message: "Core pinning is only implemented for Linux on Unix platforms".to_string(),
        })
    }
}

pub fn set_thread_priority(priority: ThreadPriority) {
    #[cfg(windows)]
    {
        let current_thread = unsafe { windows::Win32::System::Threading::GetCurrentThread() };
        let win_priority = match priority {
            ThreadPriority::BelowNormal => THREAD_PRIORITY_BELOW_NORMAL,
            ThreadPriority::Normal => THREAD_PRIORITY_NORMAL,
            ThreadPriority::High => THREAD_PRIORITY_HIGHEST,
            ThreadPriority::TimeCritical => THREAD_PRIORITY_TIME_CRITICAL,
        };
        unsafe {
            let _ = SetThreadPriority(current_thread, win_priority);
        }
    }
    #[cfg(unix)]
    {
        let nice_value: i32 = match priority {
            ThreadPriority::BelowNormal => 2,
            ThreadPriority::Normal => 0,
            ThreadPriority::High => -2,
            ThreadPriority::TimeCritical => -10,
        };
        let _ = unsafe { libc::nice(nice_value) };
    }
}

pub fn spawn_pinned<F, T>(
    name: &str,
    core_id: usize,
    priority: ThreadPriority,
    f: F,
) -> Result<thread::JoinHandle<T>, PinError>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    let thread_name = name.to_string();
    let builder = thread::Builder::new().name(thread_name);
    Ok(builder
        .spawn(move || {
            if let Err(e) = pin_to_core(core_id) {
                tracing::warn!("Failed to pin thread to core {}: {}", core_id, e);
            }
            set_thread_priority(priority);
            f()
        })
        .expect("spawn_pinned failed"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_spawn_pinned() {
        let handle = spawn_pinned(
            "test_thread",
            0,
            ThreadPriority::Normal,
            || {
                42
            },
        );
        assert!(handle.is_ok());
        let result = handle.unwrap().join().unwrap();
        assert_eq!(result, 42);
    }

    #[test]
    fn test_pin_to_core() {
        let result = pin_to_core(0);
        assert!(result.is_ok());
    }

    #[test]
    fn test_thread_priorities() {
        set_thread_priority(ThreadPriority::BelowNormal);
        set_thread_priority(ThreadPriority::Normal);
        set_thread_priority(ThreadPriority::High);
    }
}
