use crate::master::meta::feature::AclFeature;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DirFeature {
    pub(crate) x_attr: HashMap<String, Vec<u8>>,
    pub(crate) acl: AclFeature,
}

impl DirFeature {
    pub fn new() -> Self {
        Self {
            x_attr: HashMap::new(),
            acl: AclFeature::default(),
        }
    }
}

impl Default for DirFeature {
    fn default() -> Self {
        Self::new()
    }
}
