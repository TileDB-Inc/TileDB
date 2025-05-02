#[cxx::bridge]
mod ffi {
    #[namespace = "tiledb::sm"]
    extern "C++" {
        type Datatype = crate::sm::enums::Datatype;
    }

    #[namespace = "tiledb::sm"]
    unsafe extern "C++" {
        include!("tiledb/sm/array_schema/attribute.h");

        type Attribute;
        fn name(&self) -> &CxxString;

        #[cxx_name = "type"]
        fn datatype(&self) -> Datatype;
    }

    #[namespace = "tiledb::sm"]
    unsafe extern "C++" {
        include!("tiledb/sm/array_schema/dimension.h");

        type Dimension;
        fn name(&self) -> &CxxString;

        #[cxx_name = "type"]
        fn datatype(&self) -> Datatype;
    }

    #[namespace = "tiledb::sm"]
    unsafe extern "C++" {
        include!("tiledb/sm/array_schema/domain.h");

        type Domain;

        fn shared_dimension(&self, name: &CxxString) -> SharedPtr<Dimension>;
    }

    #[namespace = "tiledb::sm"]
    unsafe extern "C++" {
        include!("tiledb/sm/array_schema/array_schema.h");

        type ArraySchema;
        fn domain(&self) -> &Domain;
        fn attribute(&self, name: &CxxString) -> *const Attribute;
    }
}

use std::str::Utf8Error;

pub use ffi::{ArraySchema, Attribute, Datatype, Dimension, Domain};

pub enum Field<'a> {
    Attribute(&'a Attribute),
    Dimension(&'a Dimension),
}

impl Field<'_> {
    pub fn name(&self) -> Result<&str, Utf8Error> {
        let cxx_name = match self {
            Self::Attribute(a) => a.name(),
            Self::Dimension(d) => d.name(),
        };
        cxx_name.to_str()
    }

    pub fn datatype(&self) -> Datatype {
        match self {
            Self::Attribute(a) => a.datatype(),
            Self::Dimension(d) => d.datatype(),
        }
    }
}

impl ArraySchema {
    pub fn field<'a>(&'a self, name: &str) -> Option<Field<'a>> {
        cxx::let_cxx_string!(cxxname = name);
        self.field_cxx(&cxxname)
    }

    pub fn field_cxx<'a>(&'a self, name: &cxx::CxxString) -> Option<Field<'a>> {
        let maybe_attribute = self.attribute(name);
        if !maybe_attribute.is_null() {
            return Some(Field::Attribute(unsafe { &*maybe_attribute }));
        }

        let maybe_dimension = self.domain().shared_dimension(name);
        maybe_dimension.as_ref().map(|d| {
            // SAFETY: the domain owns another shared_ptr ref, so this
            // will live at least as long as the domain, which lives
            // at least as long as `self`
            Field::Dimension(unsafe { core::mem::transmute::<_, &'a Dimension>(d) })
        })
    }
}
