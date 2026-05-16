//! Platform-aware mutex for interior mutability.
//!
//! Uses `std::sync::RwLock` on native targets (thread-safe, allows concurrent reads),
//! falls back to `RefCell` on WASI (single-threaded, no need for locks).

#[cfg(not(target_arch = "wasm32"))]
mod inner {
    use std::sync::RwLock as StdRwLock;

    pub struct RwLock<T>(StdRwLock<T>);

    impl<T> RwLock<T> {
        pub fn new(val: T) -> Self {
            Self(StdRwLock::new(val))
        }

        #[track_caller]
        pub fn read(&self) -> std::sync::RwLockReadGuard<'_, T> {
            self.0.read().expect("RwLock poisoned")
        }

        #[track_caller]
        pub fn write(&self) -> std::sync::RwLockWriteGuard<'_, T> {
            self.0.write().expect("RwLock poisoned")
        }
    }

    // SAFETY: RwLock provides thread-safe interior mutability via std::sync::RwLock.
    unsafe impl<T: Send> Send for RwLock<T> {}
    unsafe impl<T: Send> Sync for RwLock<T> {}
}

#[cfg(target_arch = "wasm32")]
mod inner {
    use std::cell::RefCell;

    pub struct RwLock<T>(RefCell<T>);

    impl<T> RwLock<T> {
        pub fn new(val: T) -> Self {
            Self(RefCell::new(val))
        }

        #[track_caller]
        pub fn read(&self) -> std::cell::Ref<'_, T> {
            self.0.borrow()
        }

        #[track_caller]
        pub fn write(&self) -> std::cell::RefMut<'_, T> {
            self.0.borrow_mut()
        }
    }
}

pub(crate) use inner::RwLock;

// Backward-compatible alias for existing code using Mutex API
#[cfg(not(target_arch = "wasm32"))]
mod mutex_compat {
    use super::RwLock;
    use std::ops::{Deref, DerefMut};

    pub struct Mutex<T>(RwLock<T>);

    impl<T> Mutex<T> {
        pub fn new(val: T) -> Self {
            Self(RwLock::new(val))
        }

        #[track_caller]
        pub fn lock(&self) -> MutexGuard<'_, T> {
            MutexGuard(self.0.write())
        }
    }

    pub struct MutexGuard<'a, T>(std::sync::RwLockWriteGuard<'a, T>);

    impl<T> Deref for MutexGuard<'_, T> {
        type Target = T;
        fn deref(&self) -> &T {
            &self.0
        }
    }

    impl<T> DerefMut for MutexGuard<'_, T> {
        fn deref_mut(&mut self) -> &mut T {
            &mut self.0
        }
    }

    unsafe impl<T: Send> Send for Mutex<T> {}
    unsafe impl<T: Send> Sync for Mutex<T> {}
}

#[cfg(target_arch = "wasm32")]
mod mutex_compat {
    use super::RwLock;
    use std::ops::{Deref, DerefMut};

    pub struct Mutex<T>(RwLock<T>);

    impl<T> Mutex<T> {
        pub fn new(val: T) -> Self {
            Self(RwLock::new(val))
        }

        #[track_caller]
        pub fn lock(&self) -> MutexGuard<'_, T> {
            MutexGuard(self.0.write())
        }
    }

    pub struct MutexGuard<'a, T>(std::cell::RefMut<'a, T>);

    impl<T> Deref for MutexGuard<'_, T> {
        type Target = T;
        fn deref(&self) -> &T {
            &self.0
        }
    }

    impl<T> DerefMut for MutexGuard<'_, T> {
        fn deref_mut(&mut self) -> &mut T {
            &mut self.0
        }
    }
}

pub(crate) use mutex_compat::Mutex;
