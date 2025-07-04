#[cxx::bridge]
mod ffi {
    #[namespace = "tiledb::sm"]
    extern "C++" {
        type Datatype = crate::sm::enums::Datatype;
        type Layout = crate::sm::enums::Layout;
    }

    #[namespace = "tiledb::oxidize::sm::attribute"]
    unsafe extern "C++" {
        include!("tiledb/oxidize/cxx-interface/cc/array_schema.h");
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

        #[namespace = "tiledb::oxidize::sm::attribute"]
        fn enumeration_name_cxx(attr: &Attribute) -> *const CxxString;

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

        #[namespace = "tiledb::oxidize::sm::dimension"]
        fn set_domain(dimension: Pin<&mut Dimension>, domain: &[u8]) -> Result<()>;

        #[namespace = "tiledb::oxidize::sm::dimension"]
        fn set_tile_extent(dimension: Pin<&mut Dimension>, extent: &[u8]) -> Result<()>;
    }

    #[namespace = "tiledb::sm"]
    unsafe extern "C++" {
        include!("tiledb/sm/array_schema/domain.h");

        type Domain;

        fn dim_num(&self) -> u32;
        fn dimension_ptr(&self, d: u32) -> *const Dimension;
        fn shared_dimension(&self, name: &CxxString) -> SharedPtr<Dimension>;
        fn get_dimension_index(&self, name: &CxxString) -> u32;

        fn add_dimension(self: Pin<&mut Domain>, dim: SharedPtr<Dimension>);
    }

    #[namespace = "tiledb::sm"]
    unsafe extern "C++" {
        include!("tiledb/sm/array_schema/enumeration.h");

        type Enumeration;

        #[cxx_name = "cell_val_num"]
        fn cell_val_num_cxx(&self) -> u32;

        #[cxx_name = "type"]
        fn datatype(&self) -> Datatype;
    }

    #[namespace = "tiledb::sm"]
    unsafe extern "C++" {
        include!("tiledb/sm/array_schema/array_schema.h");

        type ArraySchema;

        fn domain(&self) -> &Domain;
        fn attribute_num(&self) -> u32;

        fn is_attr(&self, name: &CxxString) -> bool;
        fn is_dim(&self, name: &CxxString) -> bool;

        #[cxx_name = "attribute"]
        fn attribute_by_idx(&self, idx: u32) -> *const Attribute;

        #[cxx_name = "attribute"]
        fn attribute_by_name(&self, name: &CxxString) -> *const Attribute;

        #[cxx_name = "cell_val_num"]
        fn cell_val_num_cxx(&self, name: &CxxString) -> u32;

        fn var_size(&self, name: &CxxString) -> bool;

        fn is_nullable(&self, name: &CxxString) -> bool;

        fn set_domain(self: Pin<&mut ArraySchema>, domain: SharedPtr<Domain>) -> Result<()>;
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

use std::fmt::{Debug, Display, Formatter, Result as FmtResult};
use std::num::NonZeroU32;
use std::pin::Pin;
use std::str::Utf8Error;

use num_traits::ToBytes;

pub use ffi::{ArraySchema, Attribute, ConstAttribute, Datatype, Dimension, Domain, Enumeration};

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

    pub fn set_domain<T>(
        self: Pin<&mut Self>,
        lower_bound: T,
        upper_bound: T,
    ) -> Result<(), cxx::Exception>
    where
        T: ToBytes,
    {
        let bytes = lower_bound
            .to_le_bytes()
            .as_ref()
            .iter()
            .chain(upper_bound.to_le_bytes().as_ref().iter())
            .copied()
            .collect::<Vec<u8>>();
        ffi::set_domain(self, &bytes)
    }

    pub fn set_tile_extent<T>(self: Pin<&mut Self>, extent: T) -> Result<(), cxx::Exception>
    where
        T: ToBytes,
    {
        let bytes = extent.to_le_bytes();
        ffi::set_tile_extent(self, bytes.as_ref())
    }
}

impl Debug for Dimension {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        write!(
            f,
            "{} {}[{}]",
            self.name(),
            self.datatype(),
            self.cell_val_num()
        )
    }
}

impl Attribute {
    pub fn cell_val_num(&self) -> CellValNum {
        let cxx = self.cell_val_num_cxx();

        // SAFETY: non-zero would have been validated by the ArraySchema
        CellValNum::from_cxx(cxx).unwrap()
    }

    pub fn enumeration_name_cxx(&self) -> *const cxx::CxxString {
        ffi::enumeration_name_cxx(self)
    }

    pub fn enumeration_name(&self) -> Option<Result<&str, Utf8Error>> {
        let ptr = self.enumeration_name_cxx();
        if ptr.is_null() {
            return None;
        }
        let cxx = unsafe { &*ptr };
        Some(cxx.to_str())
    }
}

impl Debug for Attribute {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        write!(
            f,
            "{} {}[{}]",
            self.name(),
            self.datatype(),
            self.cell_val_num()
        )
    }
}

#[derive(Debug)]
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

    pub fn enumeration_name(&self) -> Option<Result<&str, Utf8Error>> {
        match self {
            Self::Attribute(a) => a.enumeration_name(),
            Self::Dimension(_) => None,
        }
    }
}

impl Enumeration {
    pub fn cell_val_num(&self) -> CellValNum {
        let cxx = self.cell_val_num_cxx();

        // SAFETY: non-zero would have been validated by the ArraySchema
        CellValNum::from_cxx(cxx).unwrap()
    }
}

impl ArraySchema {
    pub fn cell_val_num(&self, name: &cxx::CxxString) -> CellValNum {
        let cxx = self.cell_val_num_cxx(name);

        // SAFETY: non-zero would have been validated by the ArraySchema
        CellValNum::from_cxx(cxx).unwrap()
    }

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
