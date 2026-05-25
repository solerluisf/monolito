//! Large page (huge page) memory allocation support for Windows
//! 
//! This module attempts to enable MEM_LARGE_PAGES (2MB pages on x64)
//! for improved TLB performance and reduced page table overhead.

use std::ffi::c_void;

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
    // Windows implementation requires SeLockMemoryPrivilege
    // For production use, this should be enabled via Group Policy or run as Administrator
    // 
    // NOTE: Full implementation requires additional Windows crate features:
    // - Win32_Security
    // - Win32_System_Memory  
    // - Win32_System_Threading
    //
    // The full implementation would call:
    // - OpenProcessToken
    // - LookupPrivilegeValueW
    // - AdjustTokenPrivileges
    //
    // For now, return NotSupported to avoid complex feature dependencies
    LargePageResult::NotSupported
}

#[cfg(not(windows))]
pub fn enable_large_pages() -> LargePageResult {
    LargePageResult::NotSupported
}

/// Attempts to allocate memory with large pages enabled.
/// Falls back to regular allocation if large pages are not available.
#[cfg(windows)]
pub fn allocate_large_pages(size: usize) -> Result<*mut c_void, String> {
    // NOTE: Full implementation would use VirtualAlloc with MEM_LARGE_PAGES
    // For now, use standard allocation
    allocate_standard(size)
}

#[cfg(not(windows))]
pub fn allocate_large_pages(size: usize) -> Result<*mut c_void, String> {
    allocate_standard(size)
}

fn allocate_standard(size: usize) -> Result<*mut c_void, String> {
    let layout = std::alloc::Layout::from_size_align(size, 4096)
        .map_err(|e| format!("Invalid layout: {}", e))?;
    
    let ptr = unsafe { std::alloc::alloc(layout) };
    
    if ptr.is_null() {
        Err("Allocation failed".to_string())
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
