//! Platform-aware mutex for interior mutability.
//!
//! Uses `std::sync::Mutex` on native targets (thread-safe),
//! falls back to `RefCell` on WASI (single-threaded, no need for locks).

#[cfg(not(target_arch = "wasm32"))]
mod inner {
    use std::sync::Mutex as StdMutex;

    pub struct Mutex<T>(StdMutex<T>);

    impl<T> Mutex<T> {
        pub fn new(val: T) -> Self {
            Self(StdMutex::new(val))
        }

        #[track_caller]
        pub fn lock(&self) -> std::sync::MutexGuard<'_, T> {
            self.0.lock().expect("Mutex poisoned")
        }
    }

    // SAFETY: Mutex provides thread-safe interior mutability via std::sync::Mutex.
    unsafe impl<T: Send> Send for Mutex<T> {}
    unsafe impl<T: Send> Sync for Mutex<T> {}
}

#[cfg(target_arch = "wasm32")]
mod inner {
    use std::cell::RefCell;

    pub struct Mutex<T>(RefCell<T>);

    impl<T> Mutex<T> {
        pub fn new(val: T) -> Self {
            Self(RefCell::new(val))
        }

        #[track_caller]
        pub fn lock(&self) -> std::cell::RefMut<'_, T> {
            self.0.borrow_mut()
        }
    }
}

pub(crate) use inner::Mutex;
