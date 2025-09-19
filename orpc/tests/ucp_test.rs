use orpc::ucp::Config;

#[test]
fn config() {
    let conf = Config::default();
    conf.print()
}