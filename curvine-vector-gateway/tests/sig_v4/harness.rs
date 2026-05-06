use curvine_vector_gateway::sig_v4::VHeader;

pub struct StaticHeaders(pub Vec<(String, String)>);

impl VHeader for StaticHeaders {
    fn get_header(&self, key: &str) -> Option<String> {
        self.0
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(key))
            .map(|(_, v)| v.clone())
    }

    fn set_header(&mut self, key: &str, val: &str) {
        let needle = key.to_ascii_lowercase();
        if let Some((_, value)) = self
            .0
            .iter_mut()
            .find(|(k, _)| k.eq_ignore_ascii_case(&needle))
        {
            *value = val.to_string();
            return;
        }
        self.0.push((needle, val.to_string()));
    }

    fn delete_header(&mut self, key: &str) {
        self.0.retain(|(k, _)| !k.eq_ignore_ascii_case(key));
    }

    fn rng_header(&self, mut cb: impl FnMut(&str, &str) -> bool) {
        for (k, v) in &self.0 {
            if !cb(k, v) {
                return;
            }
        }
    }
}
