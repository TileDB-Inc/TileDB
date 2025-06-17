#[cxx::bridge]
mod ffi {
    #[namespace = "tiledb::sm"]
    unsafe extern "C++" {
        include!("tiledb/common/memory_tracker.h");
        type MemoryTracker;
    }

    impl SharedPtr<MemoryTracker> {}
}

pub use self::ffi::MemoryTracker;
