use std::ffi::c_void;
use std::fmt;

#[derive(Debug, Clone, PartialEq)]
pub enum MemoryError {
    AllocationFailed(String),
    InvalidLayout(String),
}

impl fmt::Display for MemoryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MemoryError::AllocationFailed(msg) => write!(f, "Allocation failed: {}", msg),
            MemoryError::InvalidLayout(msg) => write!(f, "Invalid layout: {}", msg),
        }
    }
}

impl std::error::Error for MemoryError {}

/// Result of attempting to enable large pages
#[derive(Debug, Clone)]
pub enum LargePageResult {
    /// Large pages successfully enabled
    Enabled,
    /// Large pages not available (insufficient privileges)
    PrivilegeNotHeld,
    /// Large pages not supported on this platform
    NotSupported,
    /// An error occurred during setup
    Error(String),
}

/// Attempts to enable SeLockMemoryPrivilege for the current process.
/// This is required to use MEM_LARGE_PAGES.
#[cfg(windows)]
pub fn enable_large_pages() -> LargePageResult {
    use windows::Win32::Foundation::{GetLastError, CloseHandle};
    use windows::core::PCWSTR;
    use windows::Win32::Security::{
        AdjustTokenPrivileges, LookupPrivilegeValueW, TOKEN_ADJUST_PRIVILEGES,
        LUID_AND_ATTRIBUTES, SE_PRIVILEGE_ENABLED, TOKEN_PRIVILEGES,
    };
    use windows::Win32::System::Threading::{GetCurrentProcess, OpenProcessToken};

    unsafe {
        let process = GetCurrentProcess();
        let mut token = std::mem::zeroed();
        let open_result = OpenProcessToken(
            process,
            TOKEN_ADJUST_PRIVILEGES,
            &mut token,
        );
        if open_result.is_err() {
            return LargePageResult::Error(format!(
                "OpenProcessToken failed: {:?}",
                GetLastError()
            ));
        }

        let privilege: Vec<u16> = "SeLockMemoryPrivilege\0".encode_utf16().collect();
        let mut luid = std::mem::zeroed();
        let lookup_result = LookupPrivilegeValueW(
            None,
            PCWSTR(privilege.as_ptr()),
            &mut luid,
        );
        if lookup_result.is_err() {
            let _ = CloseHandle(token);
            return LargePageResult::PrivilegeNotHeld;
        }

        let tp = TOKEN_PRIVILEGES {
            PrivilegeCount: 1,
            Privileges: [LUID_AND_ATTRIBUTES {
                Luid: luid,
                Attributes: SE_PRIVILEGE_ENABLED,
            }],
        };

        let adjust_result = AdjustTokenPrivileges(
            token,
            false,
            Some(&tp),
            0,
            None,
            None,
        );
        if adjust_result.is_err() {
            let err = GetLastError();
            let _ = CloseHandle(token);
            if err.0 == windows::Win32::Foundation::ERROR_NOT_ALL_ASSIGNED.0 {
                return LargePageResult::PrivilegeNotHeld;
            }
            return LargePageResult::Error(format!(
                "AdjustTokenPrivileges failed: {:?}",
                err
            ));
        }

        // Verify that the privilege was actually assigned
        let last_err = GetLastError();
        if last_err.0 == windows::Win32::Foundation::ERROR_NOT_ALL_ASSIGNED.0 {
            let _ = CloseHandle(token);
            return LargePageResult::PrivilegeNotHeld;
        }

        let _ = CloseHandle(token);
        LargePageResult::Enabled
    }
}

#[cfg(not(windows))]
pub fn enable_large_pages() -> LargePageResult {
    LargePageResult::NotSupported
}

#[cfg(windows)]
pub fn allocate_large_pages(size: usize) -> Result<*mut c_void, MemoryError> {
    use windows::Win32::Foundation::GetLastError;
    use windows::Win32::System::Memory::{
        VirtualAlloc, MEM_COMMIT, MEM_LARGE_PAGES, MEM_RESERVE, PAGE_READWRITE,
    };

    unsafe {
        let ptr = VirtualAlloc(
            None,
            size,
            MEM_COMMIT | MEM_RESERVE | MEM_LARGE_PAGES,
            PAGE_READWRITE,
        );
        if ptr.is_null() {
            let err = GetLastError();
            tracing::warn!(
                "VirtualAlloc with MEM_LARGE_PAGES failed ({:?}), falling back to standard pages",
                err
            );
            allocate_standard(size)
        } else {
            Ok(ptr)
        }
    }
}

#[cfg(not(windows))]
pub fn allocate_large_pages(size: usize) -> Result<*mut c_void, MemoryError> {
    allocate_standard(size)
}

fn allocate_standard(size: usize) -> Result<*mut c_void, MemoryError> {
    let layout = std::alloc::Layout::from_size_align(size, 4096)
        .map_err(|e| MemoryError::InvalidLayout(format!("Invalid layout: {}", e)))?;
    
    let ptr = unsafe { std::alloc::alloc(layout) };
    
    if ptr.is_null() {
        Err(MemoryError::AllocationFailed("Allocation failed".to_string()))
    } else {
        Ok(ptr as *mut c_void)
    }
}

/// Logs the result of large page initialization
pub fn log_large_page_result(result: &LargePageResult) {
    match result {
        LargePageResult::Enabled => {
            tracing::info!("Large pages (MEM_LARGE_PAGES) enabled successfully");
        }
        LargePageResult::PrivilegeNotHeld => {
            tracing::warn!("Large pages not available: SeLockMemoryPrivilege not held. Run as administrator or adjust privileges.");
        }
        LargePageResult::NotSupported => {
            tracing::info!("Large pages not supported on this platform (or feature not enabled)");
        }
        LargePageResult::Error(e) => {
            tracing::error!("Failed to enable large pages: {}", e);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_enable_large_pages_returns_result() {
        let result = enable_large_pages();
        // Should not panic, result depends on platform and privileges
        log_large_page_result(&result);
    }

    #[test]
    fn test_allocate_large_pages() {
        let result = allocate_large_pages(4096);
        assert!(result.is_ok());
        
        // Cleanup
        if let Ok(ptr) = result {
            if !ptr.is_null() {
                // Note: In production, you'd want to properly free this memory
                // For testing, we just verify allocation succeeded
            }
        }
    }
}
