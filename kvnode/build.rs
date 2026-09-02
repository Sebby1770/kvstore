// Link the kvnode binary against the static C library built in ../kvengine.
fn main() {
    let manifest = std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR");
    let lib_dir = std::path::Path::new(&manifest)
        .parent()
        .expect("workspace root")
        .join("kvengine");

    println!("cargo:rustc-link-search=native={}", lib_dir.display());
    println!("cargo:rustc-link-lib=static=kvengine");
    println!(
        "cargo:rerun-if-changed={}",
        lib_dir.join("libkvengine.a").display()
    );
}
