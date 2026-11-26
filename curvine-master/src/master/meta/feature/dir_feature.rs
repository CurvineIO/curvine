use crate::master::meta::feature::AclFeature;
use curvine_codec::{CodecResult, Decode, Encode, Reader, Writer};
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

impl Encode for DirFeature {
    #[inline(always)]
    fn encode(&self, writer: &mut Writer) -> CodecResult<()> {
        self.x_attr.encode(writer)?;
        self.acl.encode(writer)
    }
}

impl Decode for DirFeature {
    #[inline(always)]
    fn decode(reader: &mut Reader<'_>) -> CodecResult<Self> {
        Ok(Self {
            x_attr: HashMap::decode(reader)?,
            acl: AclFeature::decode(reader)?,
        })
    }
}
