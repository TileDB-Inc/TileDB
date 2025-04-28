use cxx::ExternType;

#[repr(transparent)]
pub struct PtrBoxDynFilter(*mut Box<dyn Filter>);

unsafe impl ExternType for PtrBoxDynFilter {
    type Id = cxx::type_id!("tiledb::oxidized::PtrBoxDynFilter");
    type Kind = cxx::kind::Trivial;
}

unsafe impl ExternType for Box<dyn Filter> {
    type Id = cxx::type_id!("tiledb::oxidized::BoxDynFilter");
    type Kind = cxx::kind::Trivial;
}

#[cxx::bridge]
mod ffi {
    #[namespace = "tiledb::oxidized"]
    extern "C++" {
        include!("tiledb/oxidized/cpp/filter/filter.h");
        type BoxDynFilter = Box<dyn crate::Filter>;
        type PtrBoxDynFilter = crate::PtrBoxDynFilter;
    }

    #[namespace = "tiledb::oxidized"]
    extern "Rust" {
        unsafe fn drop_trait_object_in_place(ptr: PtrBoxDynFilter);
    }
}

unsafe fn drop_trait_object_in_place(ptr: PtrBoxDynFilter) {
    unsafe { std::ptr::drop_in_place(ptr.0) }
}

pub trait Filter {}

pub type DynFilter = Box<dyn Filter>;
