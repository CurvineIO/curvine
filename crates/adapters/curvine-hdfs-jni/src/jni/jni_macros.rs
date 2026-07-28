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

#[macro_export]
macro_rules! gen_class_name {
    // A single part class name will be prefixed with `java.lang.`
    ($single_part_class:ident) => {
        $crate::gen_class_name! { @inner java.lang.$single_part_class }
    };
    ($($class:ident).+) => {
        $crate::gen_class_name! { @inner $($class).+ }
    };
    (@inner $last:ident) => {
        stringify! {$last}
    };
    (@inner $first:ident . $($rest:ident).+) => {
        concat! {stringify! {$first}, "/", $crate::gen_class_name! {@inner $($rest).+} }
    }
}

/// Generate the type signature of a single type
#[macro_export]
macro_rules! gen_jni_type_sig {
    (boolean) => { "Z" };
    (byte) => { "B" };
    (char) => { "C" };
    (short) => { "S" };
    (int) => { "I" };
    (long) => { "J" };
    (float) => { "F" };
    (double) => { "D" };
    (void) => { "V" };
    (String) => { "Ljava/lang/String;" };
    (Runtime) => { "Ljava/lang/Runtime;" };
    (Thread) => { "Ljava/lang/Thread;" };
    (ClassLoader) => { "Ljava/lang/ClassLoader;" };
    (Throwable) => { "Ljava/lang/Throwable;" };
    ($($class_part:ident).+) => {
        concat! {"L", $crate::gen_class_name! {$($class_part).+}, ";"}
    };
    ($($class_part:ident).+ []) => {
        concat! { "[", $crate::gen_jni_type_sig! {$($class_part).+}}
    };
}

/// Cast a `JValueGen` to a concrete type by the given type
#[macro_export]
macro_rules! cast_jvalue {
    ({ boolean }, $value:expr) => {{
        $value.z().expect("should be bool")
    }};
    ({ byte }, $value:expr) => {{
        $value.b().expect("should be byte")
    }};
    ({ char }, $value:expr) => {{
        $value.c().expect("should be char")
    }};
    ({ double }, $value:expr) => {{
        $value.d().expect("should be double")
    }};
    ({ float }, $value:expr) => {{
        $value.f().expect("should be float")
    }};
    ({ int }, $value:expr) => {{
        $value.i().expect("should be int")
    }};
    ({ long }, $value:expr) => {{
        $value.j().expect("should be long")
    }};
    ({ short }, $value:expr) => {{
        $value.s().expect("should be short")
    }};
    ({ void }, $value:expr) => {{
        $value.v().expect("should be void")
    }};
    ({ $($class:tt)+ }, $value:expr) => {{
        $value.l().expect("should be object")
    }};
}

/// Convert values to JValue
#[macro_export]
macro_rules! to_jvalue {
    ({ boolean }, $value:expr) => {{
        jni::objects::JValue::Bool($value as _)
    }};
    ({ byte }, $value:expr) => {{
        jni::objects::JValue::Byte($value as _)
    }};
    ({ char }, $value:expr) => {{
        jni::objects::JValue::Char($value as _)
    }};
    ({ double }, $value:expr) => {{
        jni::objects::JValue::Double($value as _)
    }};
    ({ float }, $value:expr) => {{
        jni::objects::JValue::Float($value as _)
    }};
    ({ int }, $value:expr) => {{
        jni::objects::JValue::Int($value as _)
    }};
    ({ long }, $value:expr) => {{
        jni::objects::JValue::Long($value as _)
    }};
    ({ short }, $value:expr) => {{
        jni::objects::JValue::Short($value as _)
    }};
    ({ $($class:ident)+ }, $value:expr) => {{
        jni::objects::JValue::Object($value as _)
    }};
}

/// Generate the jni signature of a given function
#[macro_export]
macro_rules! gen_jni_sig {
    ({$($ret:tt)*}, {}) => {
        concat!("()", $crate::gen_jni_type_sig!{$($ret)*})
    };
    ({$($ret:tt)*}, {$($args:tt)*}) => {
        concat!("(", $crate::gen_jni_type_sig!{$($args)*}, ")", $crate::gen_jni_type_sig!{$($ret)*})
    };
}

#[macro_export]
macro_rules! call_static_method {
    ($env:expr, {$($class:ident).+}, {$ret:tt $method:ident()}) => {{
        $env.call_static_method(
            $crate::gen_class_name!($($class).+),
            stringify!($method),
            $crate::gen_jni_sig!({$ret}, {}),
            &[],
        ).map(|jvalue| {
            $crate::cast_jvalue!({$ret}, jvalue)
        })
    }};
}

#[macro_export]
macro_rules! call_method {
    ($env:expr, $obj:expr, {$ret:tt $method:ident()}) => {{
        $env.call_method(
            $obj,
            stringify!($method),
            $crate::gen_jni_sig!({ $ret }, {}),
            &[],
        )
        .map(|jvalue| $crate::cast_jvalue!({ $ret }, jvalue))
    }};
    ($env:expr, $obj:expr, {void $method:ident($param:tt)}, $arg:expr) => {{
        $env.call_method(
            $obj,
            stringify!($method),
            $crate::gen_jni_sig!({ void }, { $param }),
            &[$crate::to_jvalue!({ $param }, $arg)],
        )
        .map(|jvalue| $crate::cast_jvalue!({ void }, jvalue))
    }};
    ($env:expr, $obj:expr, {$ret:tt $method:ident()}) => {{
        $env.call_method(
            $obj,
            stringify!($method),
            $crate::gen_jni_sig!({ $ret }, {}),
            &[],
        )
        .map(|jvalue| $crate::cast_jvalue!({ $ret }, jvalue))
    }};
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_gen_class_name() {
        assert_eq!(gen_class_name!(String), "java/lang/String");
        assert_eq!(gen_class_name!(java.lang.String), "java/lang/String");
        assert_eq!(gen_class_name!(java.lang.Runtime), "java/lang/Runtime");
    }

    #[test]
    fn test_gen_jni_type_sig() {
        assert_eq!(gen_jni_type_sig!(boolean), "Z");
        assert_eq!(gen_jni_type_sig!(int), "I");
        assert_eq!(gen_jni_type_sig!(long), "J");
        assert_eq!(gen_jni_type_sig!(String), "Ljava/lang/String;");
        assert_eq!(gen_jni_type_sig!(Runtime), "Ljava/lang/Runtime;");
    }

    #[test]
    fn test_gen_jni_sig() {
        assert_eq!(gen_jni_sig!({ Runtime }, {}), "()Ljava/lang/Runtime;");
        assert_eq!(gen_jni_sig!({ long }, {}), "()J");
        assert_eq!(gen_jni_sig!({ String }, {}), "()Ljava/lang/String;");
        assert_eq!(
            gen_jni_sig!({ void }, { ClassLoader }),
            "(Ljava/lang/ClassLoader;)V"
        );
    }
}
