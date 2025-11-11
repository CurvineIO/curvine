// Copyright 2025 OPPO.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![allow(clippy::should_implement_trait, clippy::mut_from_ref)]

use std::fmt;
use std::fmt::{Debug, Pointer};
use std::marker::PhantomData;
use std::mem::MaybeUninit;
use std::ops::{Deref, DerefMut};

// Encapsulated raw pointer, similar to the NotNull function in the rust standard library.
pub struct RawPtr<T> {
    owned: bool,
    ptr: *mut T,
    _marker: PhantomData<T>,
}

impl<T> RawPtr<T> {
    fn new(owned: bool, ptr: *mut T) -> Self {
        Self {
            owned,
            ptr,
            _marker: PhantomData,
        }
    }

    // Create a raw pointer based on the instance. RawPtr will hold ownership of the variable.
    pub fn from_owned(r: T) -> Self {
        Self::new(true, Box::into_raw(Box::new(r)))
    }

    // Create a raw pointer from a reference
    pub fn from_ref(r: &T) -> Self {
        Self::new(false, r as *const T as *mut T)
    }

    pub fn from_raw(r: *const T) -> Self {
        Self::new(false, r as *mut T)
    }

    pub fn from_uninit(r: MaybeUninit<*mut T>) -> Self {
        Self::from_raw(unsafe { r.assume_init() })
    }

    pub fn as_mut(&self) -> &mut T {
        unsafe { &mut *self.ptr }
    }

    pub fn as_ref(&self) -> &T {
        unsafe { &*self.ptr }
    }

    pub fn as_mut_ptr(&self) -> *mut T {
        self.ptr
    }

    pub fn as_ptr(&self) -> *const T {
        self.ptr as *const _
    }

    pub fn add(&mut self, offset: usize) {
        unsafe {
            self.ptr = self.ptr.add(offset);
        }
    }

    pub fn replace(&self, value: T) -> T {
        if self.owned {
            unsafe { std::ptr::replace(self.ptr, value) }
        } else {
            panic!("Cannot replace a pointer that owns the value")
        }
    }

    pub fn take(&self) -> T {
        if self.owned {
            unsafe { *Box::from_raw(self.ptr) }
        } else {
            panic!("Cannot take a pointer that owns the value")
        }
    }
}

impl<T> Clone for RawPtr<T> {
    #[inline(always)]
    fn clone(&self) -> Self {
        // lone naked pointer, owned must be false; otherwise it may cause double release
        Self {
            owned: false,
            ptr: self.ptr,
            _marker: self._marker,
        }
    }
}

unsafe impl<T> Send for RawPtr<T> {}

unsafe impl<T> Sync for RawPtr<T> {}

impl<T> Drop for RawPtr<T> {
    fn drop(&mut self) {
        unsafe {
            if self.owned {
                // The box raw pointer needs to return ownership to the Box to clean up the data correctly.
                let data = Box::from_raw(self.ptr);
                drop(data)
            }
        }
    }
}

impl<T> Deref for RawPtr<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.as_ref()
    }
}

impl<T> DerefMut for RawPtr<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.as_mut()
    }
}

impl<T> Debug for RawPtr<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Pointer::fmt(&self.ptr, f)
    }
}

impl<T> Pointer for RawPtr<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Pointer::fmt(&self.ptr, f)
    }
}

#[cfg(test)]
mod tests {
    use crate::sys::RawPtr;

    struct Person {
        name: String,
        age: u32,
    }

    impl Drop for Person {
        fn drop(&mut self) {
            println!("drop Person")
        }
    }

    #[test]
    fn ptr_owned() {
        let p = Person {
            name: "curvine".to_owned(),
            age: 1,
        };
        let ptr = RawPtr::from_owned(p);

        println!("{} {}", ptr.name, ptr.age)
    }

    #[test]
    fn ptr_ref() {
        let p = Person {
            name: "curvine".to_owned(),
            age: 1,
        };
        {
            let ptr = RawPtr::from_ref(&p);
            println!("{} {}", ptr.name, ptr.age)
        }
    }

    #[test]
    fn ptr_replace() {
        let p1 = Person {
            name: "curvine".to_owned(),
            age: 1,
        };
        let ptr = RawPtr::from_owned(p1);

        println!("Before replace: {} {}", ptr.name, ptr.age);

        let p2 = Person {
            name: "oppo".to_owned(),
            age: 2,
        };
        let old = ptr.replace(p2);

        println!("After replace: {} {}", ptr.name, ptr.age);
        println!("Old value: {} {}", old.name, old.age);

        assert_eq!(ptr.name, "oppo");
        assert_eq!(ptr.age, 2);
        assert_eq!(old.name, "curvine");
        assert_eq!(old.age, 1);

        drop(old);
        println!("Old value dropped");
    }
}
