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

pub fn pin_to_core(core_id: usize) -> Result<(), String> {
    #[cfg(windows)]
    {
        let current_thread = unsafe { windows::Win32::System::Threading::GetCurrentThread() };
        let mask = 1usize << core_id;
        let result = unsafe { SetThreadAffinityMask(current_thread, mask) };
        if result == 0 {
            return Err(format!("Failed to pin thread to core {}", core_id));
        }
        Ok(())
    }
    #[cfg(not(windows))]
    {
        // On Linux, core pinning would use libc::sched_setaffinity or the core_affinity crate.
        // For this Windows 11 target, this path is dead code at compile time.
        let _ = core_id;
        Err("Core pinning not implemented for this platform".to_string())
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
    #[cfg(not(windows))]
    {
        // On Linux, use nice() or sched_setscheduler
        let _ = priority;
    }
}

pub fn spawn_pinned<F, T>(
    name: &str,
    core_id: usize,
    priority: ThreadPriority,
    f: F,
) -> thread::JoinHandle<T>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    let thread_name = name.to_string();
    thread::Builder::new()
        .name(thread_name)
        .spawn(move || {
            let _ = pin_to_core(core_id);
            set_thread_priority(priority);
            f()
        })
        .expect("Failed to spawn thread")
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
        let result = handle.join().unwrap();
        assert_eq!(result, 42);
    }

    #[test]
    fn test_thread_priorities() {
        set_thread_priority(ThreadPriority::BelowNormal);
        set_thread_priority(ThreadPriority::Normal);
        set_thread_priority(ThreadPriority::High);
    }
}
