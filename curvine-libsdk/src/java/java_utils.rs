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

use curvine_common::error::FsError;
use jni::objects::{JObject, JString, JThrowable, JValue};
use jni::sys::{jarray, jboolean, jstring, JNI_TRUE};
use jni::JNIEnv;
use orpc::CommonResult;

pub struct JavaUtils;

impl JavaUtils {
    pub const EXCEPTION_CLASS: &'static str = "io/curvine/exception/CurvineException";
    pub const EXCEPTION_METHOD: &'static str = "create";
    pub const EXCEPTION_SIG: &'static str = "(ILjava/lang/String;)Ljava/io/IOException;";

    pub fn string_to_jstring<'a>(
        env: &mut JNIEnv<'a>,
        s: impl AsRef<str>,
    ) -> CommonResult<JObject<'a>> {
        Ok(env.new_string(s.as_ref().to_string())?.into())
    }

    pub fn jstring_to_string(env: &mut JNIEnv, s: &JString) -> CommonResult<String> {
        let res = unsafe { env.get_string_unchecked(s)? };
        Ok(res.into())
    }

    pub fn set_int(env: &mut JNIEnv, obj: &JObject, name: &str, value: i32) -> CommonResult<()> {
        let value = JValue::Int(value);
        env.set_field(obj, name, "I", value)?;
        Ok(())
    }

    pub fn set_long(env: &mut JNIEnv, obj: &JObject, name: &str, value: i64) -> CommonResult<()> {
        let value = JValue::Long(value);
        env.set_field(obj, name, "L", value)?;
        Ok(())
    }

    fn throw0(env: &mut JNIEnv, error: &FsError) -> CommonResult<()> {
        let kind = JValue::Int(error.kind() as i32);
        let msg = env.new_string(format!("{:}", error))?;

        let exception = env
            .call_static_method(
                Self::EXCEPTION_CLASS,
                Self::EXCEPTION_METHOD,
                Self::EXCEPTION_SIG,
                &[kind, JValue::Object(&msg)],
            )
            .unwrap()
            .l()
            .unwrap();
        env.throw(JThrowable::from(exception))?;
        Ok(())
    }

    // Throw a java exception.
    pub(crate) fn throw(env: &mut JNIEnv, error: &FsError) {
        match Self::throw0(env, error) {
            Ok(_) => (),
            Err(e) => env.fatal_error(e.to_string()),
        }
    }

    pub fn get_obj_string(env: &mut JNIEnv, obj: &JObject<'_>, name: &str) -> CommonResult<String> {
        let field = env.get_field(obj, name, "Ljava/lang/String;")?;
        let value = env
            .get_string(&field.l()?.into())?
            .to_string_lossy()
            .to_string();
        Ok(value)
    }

    pub fn get_obj_int(env: &mut JNIEnv, obj: &JObject<'_>, name: &str) -> CommonResult<i32> {
        let field = env.get_field(obj, name, "I")?;
        let value = field.i()?;
        Ok(value)
    }

    pub fn set_obj_int(
        env: &mut JNIEnv,
        obj: &JObject<'_>,
        name: &str,
        value: i32,
    ) -> CommonResult<()> {
        let value = JValue::Int(value);
        env.set_field(obj, name, "I", value)?;
        Ok(())
    }

    pub fn jbool_to_bool(v: jboolean) -> bool {
        v == JNI_TRUE
    }

    pub fn new_jarray(env: &mut JNIEnv, slice: &[u8]) -> CommonResult<jarray> {
        let array = env.byte_array_from_slice(slice)?;
        Ok(array.into_raw())
    }

    pub fn new_jstring(env: &mut JNIEnv, r_string: Option<String>) -> CommonResult<jstring> {
        if let Some(v) = r_string {
            let string = env.new_string(v)?;
            Ok(string.into_raw())
        } else {
            Ok(JObject::null().into_raw())
        }
    }
}
