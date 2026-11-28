// Copyright 2026 OPPO.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

use rcgen::{
    BasicConstraints, CertificateParams, CertifiedIssuer, DnType, ExtendedKeyUsagePurpose, IsCa,
    KeyPair, KeyUsagePurpose,
};
use std::fs;
use tempfile::TempDir;

/// Owns a test's CA, client and server files. Keep it alive while a peer may read them.
pub struct TlsFixtures {
    dir: TempDir,
}

impl TlsFixtures {
    pub fn generate() -> Self {
        let mut builder = tempfile::Builder::new();
        builder.prefix("curvine-grpc-tls-");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            builder.permissions(fs::Permissions::from_mode(0o700));
        }
        let dir = builder.tempdir().unwrap();
        let ca = test_ca("Curvine test CA");
        fs::write(dir.path().join("ca.pem"), ca.pem()).unwrap();
        fs::write(
            dir.path().join("wrong-ca.pem"),
            test_ca("Untrusted test CA").pem(),
        )
        .unwrap();

        for (name, usage, dns_names) in [
            (
                "server",
                ExtendedKeyUsagePurpose::ServerAuth,
                vec!["localhost".into()],
            ),
            ("client", ExtendedKeyUsagePurpose::ClientAuth, vec![]),
        ] {
            let mut params = CertificateParams::new(dns_names).unwrap();
            params.distinguished_name.push(DnType::CommonName, name);
            params.key_usages.push(KeyUsagePurpose::DigitalSignature);
            params.extended_key_usages.push(usage);
            let key = KeyPair::generate().unwrap();
            let cert = params.signed_by(&key, &ca).unwrap();
            fs::write(dir.path().join(format!("{name}.pem")), cert.pem()).unwrap();
            fs::write(dir.path().join(format!("{name}.key")), key.serialize_pem()).unwrap();
        }
        Self { dir }
    }

    pub fn path(&self, name: &str) -> String {
        self.dir.path().join(name).display().to_string()
    }
}

fn test_ca(name: &str) -> CertifiedIssuer<'static, KeyPair> {
    let mut params = CertificateParams::default();
    params.distinguished_name.push(DnType::CommonName, name);
    params.is_ca = IsCa::Ca(BasicConstraints::Constrained(0));
    params.key_usages.push(KeyUsagePurpose::KeyCertSign);
    CertifiedIssuer::self_signed(params, KeyPair::generate().unwrap()).unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn each_fixture_generates_fresh_keys_and_removes_them_on_drop() {
        let first = TlsFixtures::generate();
        let second = TlsFixtures::generate();
        for name in ["client.key", "server.key"] {
            assert!(fs::read(first.path(name)).unwrap() != fs::read(second.path(name)).unwrap());
        }
        let directory = first.dir.path().to_owned();
        drop(first);
        assert!(!directory.exists());
        assert!(Path::new(&second.path("server.key")).is_file());
    }
}
