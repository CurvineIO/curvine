// Copyright 2025 OPPO.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::io::IOResult;
use crate::{err_box, CommonResult};
use std::collections::{HashMap, LinkedList};
use std::fs::Metadata;
use std::path::{Path, PathBuf};
use std::time::UNIX_EPOCH;
use std::{env, fs};

// A tool class for file or directory operations
pub struct FileUtils;

impl FileUtils {
    // Delete directories or files.
    pub fn delete_path<P: AsRef<Path>>(path: P, recursive: bool) -> CommonResult<()> {
        let path = path.as_ref();
        if path.exists() {
            if path.is_file() {
                fs::remove_file(path)?;
            } else if recursive {
                fs::remove_dir_all(path)?;
            } else {
                fs::remove_dir(path)?;
            }
        }
        Ok(())
    }

    pub fn rename<P: AsRef<Path>>(src: P, dst: P) -> IOResult<()> {
        fs::rename(src, dst)?;
        Ok(())
    }

    pub fn copy_dir<P: AsRef<Path>>(src: P, dst: P) -> IOResult<()> {
        let src = src.as_ref();
        let dst = dst.as_ref();
        if !dst.exists() {
            fs::create_dir_all(dst)?;
        }

        for entry in fs::read_dir(src)? {
            let entry = entry?;
            let src_path = entry.path();
            let dst_path = dst.join(entry.file_name());
            if src_path.is_dir() {
                Self::copy_dir(&src_path, &dst_path)?;
            } else {
                fs::copy(&src_path, &dst_path)?;
            }
        }
        Ok(())
    }

    // Create a directory
    pub fn create_dir<P: AsRef<Path>>(path: P, recursive: bool) -> CommonResult<()> {
        let path = Path::new(path.as_ref());
        if !path.exists() {
            if recursive {
                fs::create_dir_all(path)?;
            } else {
                fs::create_dir(path)?;
            }
        }

        Ok(())
    }

    pub fn exists<P: AsRef<Path>>(path: P) -> bool {
        let path = Path::new(path.as_ref());
        path.exists()
    }

    pub fn metadata<P: AsRef<Path>>(path: P) -> CommonResult<Metadata> {
        let path = Path::new(path.as_ref());
        let meta = path.metadata()?;
        Ok(meta)
    }

    // Create a directory
    pub fn create_parent_dir<P: AsRef<Path>>(path: P, recursive: bool) -> CommonResult<()> {
        let path = match path.as_ref().parent() {
            None => return Ok(()),
            Some(v) => v,
        };

        if !path.exists() {
            if recursive {
                fs::create_dir_all(path)?;
            } else {
                fs::create_dir(path)?;
            }
        }

        Ok(())
    }

    // Recursively get all files in the directory.
    // contains_parent is true, and the returned path protects the parent directory name, otherwise it will not be included.
    pub fn list_files<P: AsRef<Path>>(path: P, contains_parent: bool) -> CommonResult<Vec<String>> {
        let path = Path::new(path.as_ref());
        if !path.exists() {
            return err_box!("Dir {} not exists", path.display());
        }
        if !path.is_dir() {
            return err_box!("Not a dir, path {}", path.display());
        }

        let mut res = vec![];
        let mut stack = LinkedList::new();
        stack.push_back(path.to_path_buf());

        while let Some(p) = stack.pop_front() {
            if p.is_file() {
                let path_str = if contains_parent {
                    p.as_path()
                } else {
                    p.strip_prefix(path)?
                };
                res.push(format!("{}", path_str.display()));
            } else {
                let dir_list = p.read_dir()?;
                for entry in dir_list {
                    let child_path = entry?.path();
                    stack.push_back(child_path.to_path_buf());
                }
            }
        }

        Ok(res)
    }

    pub fn mtime(meta: &Metadata) -> CommonResult<u64> {
        let time = meta.modified()?;
        let since = time.duration_since(UNIX_EPOCH)?;
        Ok(since.as_millis() as u64)
    }

    pub fn ctime(meta: &Metadata) -> CommonResult<u64> {
        let time = meta.created()?;
        let since = time.duration_since(UNIX_EPOCH)?;
        Ok(since.as_millis() as u64)
    }

    pub fn join_path<P: AsRef<Path>>(parent: P, child: P) -> String {
        let path = parent.as_ref().join(child);
        format!("{}", path.display())
    }

    // Get the file name
    pub fn filename<P: AsRef<Path>>(path: P) -> Option<String> {
        if let Some(v) = path.as_ref().file_name() {
            v.to_str().map(|v| v.to_string())
        } else {
            None
        }
    }

    pub fn absolute_path(path: impl AsRef<Path>) -> IOResult<PathBuf> {
        let path = path.as_ref();

        let absolute_path = if path.is_absolute() {
            path.to_path_buf()
        } else {
            env::current_dir()?.join(path)
        };

        Ok(absolute_path)
    }

    pub fn absolute_path_string(path: impl AsRef<Path>) -> IOResult<String> {
        let string = Self::absolute_path(path)?.to_string_lossy().to_string();
        Ok(string)
    }

    pub fn read_toml_as_map(file: impl AsRef<str>) -> CommonResult<HashMap<String, String>> {
        let context = fs::read_to_string(file.as_ref())?;
        let toml_value = toml::from_str::<toml::Value>(&context)?;
        let mut result = HashMap::new();
        Self::flatten_toml_value(&mut result, &toml_value, "");
        Ok(result)
    }

    fn flatten_toml_value(map: &mut HashMap<String, String>, value: &toml::Value, prefix: &str) {
        match value {
            toml::Value::Table(table) => {
                for (key, val) in table {
                    let new_prefix = if prefix.is_empty() {
                        key.clone()
                    } else {
                        format!("{}.{}", prefix, key)
                    };
                    Self::flatten_toml_value(map, val, &new_prefix);
                }
            }
            toml::Value::String(s) => {
                map.insert(prefix.to_string(), s.clone());
            }
            _ => (),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::common::FileUtils;
    use crate::CommonResult;

    #[test]
    fn list_files_test() -> CommonResult<()> {
        let list = FileUtils::list_files("../curvine-client/", false)?;
        for item in list {
            println!("{}", item);
        }

        Ok(())
    }
}
