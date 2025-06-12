#[cxx::bridge]
mod ffi {
    #[namespace = "tiledb::sm"]
    extern "C++" {
        type Datatype = crate::sm::enums::Datatype;
        type Layout = crate::sm::enums::Layout;
    }

    #[namespace = "tiledb::sm"]
    unsafe extern "C++" {
        include!("tiledb/oxidize/oxidize/cc/array_schema.h");
        type ConstAttribute;
    }

    #[namespace = "tiledb::sm"]
    unsafe extern "C++" {
        include!("tiledb/sm/array_schema/attribute.h");

        type Attribute;
        fn name(&self) -> &CxxString;
        fn nullable(&self) -> bool;

        #[cxx_name = "cell_val_num"]
        fn cell_val_num_cxx(&self) -> u32;

        #[cxx_name = "type"]
        fn datatype(&self) -> Datatype;

        fn set_cell_val_num(self: Pin<&mut Attribute>, cell_val_num: u32);
    }

    #[namespace = "tiledb::sm"]
    unsafe extern "C++" {
        include!("tiledb/sm/array_schema/dimension.h");

        type Dimension;
        fn name(&self) -> &CxxString;

        #[cxx_name = "cell_val_num"]
        fn cell_val_num_cxx(&self) -> u32;

        #[cxx_name = "type"]
        fn datatype(&self) -> Datatype;
    }

    #[namespace = "tiledb::sm"]
    unsafe extern "C++" {
        include!("tiledb/sm/array_schema/domain.h");

        type Domain;

        fn dim_num(&self) -> u32;
        fn dimension_ptr(&self, d: u32) -> *const Dimension;
        fn shared_dimension(&self, name: &CxxString) -> SharedPtr<Dimension>;

        fn add_dimension(self: Pin<&mut Domain>, dim: SharedPtr<Dimension>);
    }

    #[namespace = "tiledb::sm"]
    unsafe extern "C++" {
        include!("tiledb/sm/array_schema/array_schema.h");

        type ArraySchema;

        fn domain(&self) -> &Domain;
        fn attribute_num(&self) -> u32;

        #[cxx_name = "attribute"]
        fn attribute_by_idx(&self, idx: u32) -> *const Attribute;

        #[cxx_name = "attribute"]
        fn attribute_by_name(&self, name: &CxxString) -> *const Attribute;

        fn var_size(&self, name: &CxxString) -> bool;

        fn set_domain(self: Pin<&mut ArraySchema>, domain: SharedPtr<Domain>);
        fn add_attribute(
            self: Pin<&mut ArraySchema>,
            attribute: SharedPtr<ConstAttribute>,
            check_special: bool,
        );
        fn set_tile_order(self: Pin<&mut ArraySchema>, order: Layout);
        fn set_cell_order(self: Pin<&mut ArraySchema>, order: Layout);
        fn set_capacity(self: Pin<&mut ArraySchema>, capacity: u64);
        fn set_allows_dups(self: Pin<&mut ArraySchema>, allows_dups: bool);
    }

    impl SharedPtr<Attribute> {}
    impl SharedPtr<Dimension> {}
    impl SharedPtr<Domain> {}
    impl SharedPtr<ArraySchema> {}
    impl UniquePtr<Attribute> {}
    impl UniquePtr<Dimension> {}
    impl UniquePtr<Domain> {}
    impl UniquePtr<ArraySchema> {}
}

use std::fmt::{Display, Formatter, Result as FmtResult};
use std::num::NonZeroU32;
use std::str::Utf8Error;

pub use ffi::{ArraySchema, Attribute, ConstAttribute, Datatype, Dimension, Domain};

#[derive(Debug)]
pub enum CellValNum {
    /// Cells of this field each contain exactly one value.
    Single,
    /// Cells of this field each contain a fixed number of values.
    Fixed(NonZeroU32),
    /// Cells of this field each contain a variable number of values.
    Var,
}

impl CellValNum {
    pub fn from_cxx(cell_val_num: u32) -> Option<Self> {
        match cell_val_num {
            1 => Some(Self::Single),
            u32::MAX => Some(Self::Var),
            n => Some(Self::Fixed(NonZeroU32::new(n)?)),
        }
    }
}

impl Display for CellValNum {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        match self {
            CellValNum::Single => write!(f, "1"),
            CellValNum::Fixed(nz) => write!(f, "{nz}"),
            CellValNum::Var => write!(f, "{}", u32::MAX),
        }
    }
}

impl From<CellValNum> for u32 {
    fn from(value: CellValNum) -> Self {
        match value {
            CellValNum::Single => 1,
            CellValNum::Fixed(nz) => nz.get(),
            CellValNum::Var => u32::MAX,
        }
    }
}

impl Domain {
    pub fn dimensions(&self) -> impl Iterator<Item = &Dimension> {
        (0..self.dim_num()).map(|d| unsafe {
            // SAFETY: index is valid by the range
            &*self.dimension_ptr(d)
        })
    }
}

impl Dimension {
    pub fn cell_val_num(&self) -> CellValNum {
        let cxx = self.cell_val_num_cxx();

        // SAFETY: non-zero would have been validated by the ArraySchema
        CellValNum::from_cxx(cxx).unwrap()
    }
}

impl Attribute {
    pub fn cell_val_num(&self) -> CellValNum {
        let cxx = self.cell_val_num_cxx();

        // SAFETY: non-zero would have been validated by the ArraySchema
        CellValNum::from_cxx(cxx).unwrap()
    }
}

pub enum Field<'a> {
    Attribute(&'a Attribute),
    Dimension(&'a Dimension),
}

impl Field<'_> {
    pub fn name(&self) -> Result<&str, Utf8Error> {
        self.name_cxx().to_str()
    }

    pub fn name_cxx(&self) -> &cxx::CxxString {
        match self {
            Self::Attribute(a) => a.name(),
            Self::Dimension(d) => d.name(),
        }
    }

    pub fn datatype(&self) -> Datatype {
        match self {
            Self::Attribute(a) => a.datatype(),
            Self::Dimension(d) => d.datatype(),
        }
    }

    pub fn cell_val_num(&self) -> CellValNum {
        match self {
            Self::Attribute(a) => a.cell_val_num(),
            Self::Dimension(d) => d.cell_val_num(),
        }
    }

    pub fn nullable(&self) -> bool {
        match self {
            Self::Attribute(a) => a.nullable(),
            Self::Dimension(_) => false,
        }
    }
}

impl ArraySchema {
    pub fn field<'a>(&'a self, name: &str) -> Option<Field<'a>> {
        cxx::let_cxx_string!(cxxname = name);
        self.field_cxx(&cxxname)
    }

    pub fn field_cxx<'a>(&'a self, name: &cxx::CxxString) -> Option<Field<'a>> {
        let maybe_attribute = self.attribute_by_name(name);
        if !maybe_attribute.is_null() {
            return Some(Field::Attribute(unsafe { &*maybe_attribute }));
        }

        let maybe_dimension = self.domain().shared_dimension(name);
        maybe_dimension.as_ref().map(|d| {
            // SAFETY: the domain owns another shared_ptr ref, so this
            // will live at least as long as the domain, which lives
            // at least as long as `self`
            // FIXME: that's not quite true because `ArraySchema::set_domain`
            // frees the old one, though this is not likely to occur
            Field::Dimension(unsafe { core::mem::transmute::<&Dimension, &'a Dimension>(d) })
        })
    }

    pub fn attributes(&self) -> impl Iterator<Item = &Attribute> {
        (0..self.attribute_num()).map(|a| unsafe {
            // SAFETY: valid index by the loop range
            &*self.attribute_by_idx(a)
        })
    }

    /// Returns an iterator over all of the fields of this schema.
    pub fn fields(&self) -> impl Iterator<Item = Field> {
        self.domain()
            .dimensions()
            .map(Field::Dimension)
            .chain(self.attributes().map(Field::Attribute))
    }
}
