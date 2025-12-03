use protobuf_codegen::CodeGen;

fn main() {
    CodeGen::new()
        .inputs(["test.proto"])
        .include("proto")
        .generate_and_compile()
        .unwrap();
}
