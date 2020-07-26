extern crate lalrpop;

fn main() {
    lalrpop::process_root().unwrap();
    tonic_build::compile_protos("proto/client/client.proto").unwrap();
    tonic_build::compile_protos("proto/id_provider.proto").unwrap();
    tonic_build::compile_protos("proto/node.proto").unwrap();
}
