use std::env;
use std::path::PathBuf;

fn main() {
    let dst = PathBuf::from(env::var_os("OUT_DIR").unwrap());

    // Tell cargo to tell rustc to link the library.
    println!("cargo:rustc-link-search=native={}/lib", dst.display());
    println!("cargo:rustc-link-lib=ucp");
    // println!("cargo:rustc-link-lib=uct");
    // println!("cargo:rustc-link-lib=ucs");
    // println!("cargo:rustc-link-lib=ucm");
}
