use cxx::let_cxx_string;
use proptest::prelude::*;
use proptest::test_runner::FileFailurePersistence;

#[cxx::bridge]
mod ffi {
    #[namespace = "tiledb::oxidized::test"]
    extern "Rust" {
        fn enumeration_name_prop() -> bool;
        fn check_empty_enumeration_name() -> bool;
    }

    #[namespace = "tiledb::sm"]
    unsafe extern "C++" {
        include!("tiledb/sm/array_schema/enumeration.h");
        include!("tiledb/oxidized/cpp/array_schema/enumeration.h");
        type ConstEnumeration;

        fn name(self: &ConstEnumeration) -> &CxxString;
    }

    #[namespace = "tiledb::sm::test"]
    unsafe extern "C++" {
        fn create_enumeration(
            name: &CxxString,
        ) -> Result<SharedPtr<ConstEnumeration>>;
    }
}

pub fn check_empty_enumeration_name() -> bool {
    let_cxx_string!(error = "");
    let res = ffi::create_enumeration(&error);
    assert!(res.is_err());
    assert!(format!("{}", res.err().unwrap()).contains("must not be empty"));
    true
}

pub fn prop_enumeration_name() -> impl Strategy<Value = String> {
    proptest::string::string_regex("..*")
        .expect("Error creating enumeration name strategy")
}

pub fn enumeration_name_prop() -> bool {
    let config = ProptestConfig {
        source_file: None,
        failure_persistence: Some(Box::new(FileFailurePersistence::Off)),
        ..Default::default()
    };
    proptest!(config, |(name in prop_enumeration_name())| {
        run_enumeration_name_prop(name).expect("Error testing property.")
    });

    true
}

fn run_enumeration_name_prop(name: String) -> anyhow::Result<()> {
    let_cxx_string!(name = name);
    let enmr = ffi::create_enumeration(&name)?;
    assert_eq!(name.to_str(), enmr.name().to_str());
    Ok(())
}
