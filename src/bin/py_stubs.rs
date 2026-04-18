use cosmogram::pylib::stub_info;

fn main() {
    stub_info().expect("failed to create stub info")
        .generate()
        .expect("Failed to generate stubs");
}