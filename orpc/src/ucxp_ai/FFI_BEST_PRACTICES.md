# FFI 输出参数最佳实践

## 🎯 核心问题

在 Rust FFI 中处理 C 函数的输出参数时，应该使用什么方法？

## 📋 常见的 C 输出参数模式

### 模式 1: 指针输出参数
```c
// C 函数签名
int create_object(object_t** out_ptr);
//                ^^^^^^^^^^ 
//                输出参数：指向指针的指针
```

### 模式 2: 结构体输出参数  
```c
// C 函数签名
int get_info(info_struct_t* out_info);
//           ^^^^^^^^^^^^^^^^^
//           输出参数：指向结构体的指针
```

## ✅ 最佳实践

### 对于指针输出参数 (模式 1)
```rust
// ✅ 推荐：使用 ptr::null_mut()
let mut handle: *mut ObjectType = ptr::null_mut();
let status = unsafe {
    create_object(&mut handle)
};

if status == 0 {
    if handle.is_null() {
        return Err("对象创建失败");
    }
    // 使用 handle...
} else {
    return Err("函数调用失败");
}
```

**为什么这样做：**
- 初始状态明确（空指针）
- C 函数失败时状态可预测
- 易于验证结果（`is_null()` 检查）

### 对于结构体输出参数 (模式 2)
```rust
// ✅ 推荐：使用 MaybeUninit
let mut info = MaybeUninit::<InfoStruct>::uninit();
let status = unsafe {
    get_info(info.as_mut_ptr())
};

if status == 0 {
    let info = unsafe { info.assume_init() };
    // 使用 info...
} else {
    return Err("获取信息失败");
}
```

**为什么这样做：**
- 避免在栈上构造大结构体
- 类型系统保证内存安全
- 适合值类型的输出

## 🚨 错误模式

### ❌ 错误：对指针输出使用 MaybeUninit
```rust
// ❌ 不推荐
let mut handle = MaybeUninit::<*mut ObjectType>::uninit();
let status = unsafe {
    create_object(handle.as_mut_ptr())  // 类型复杂
};
let handle = unsafe { handle.assume_init() };  // 可能未定义行为
```

### ❌ 错误：对大结构体使用零初始化
```rust
// ❌ 不推荐
let mut info = InfoStruct { ..Default::default() };  // 可能很大且低效
let status = unsafe {
    get_info(&mut info)
};
```

## 📊 决策流程图

```
输出参数类型？
├── 指针类型 (*mut T)
│   └── 使用 ptr::null_mut()
├── 小结构体 (<= 128 bytes)
│   └── 使用 Default::default() 或零初始化
├── 大结构体 (> 128 bytes)
│   └── 使用 MaybeUninit
└── 数组或复杂类型
    └── 使用 MaybeUninit
```

## 🧪 实际示例

### UCX 配置创建
```rust
// ✅ 正确方式
let mut config: *mut ucp_config_t = ptr::null_mut();
let status = unsafe {
    ucp_config_read(ptr::null(), ptr::null(), &mut config)
};

if status == UCS_OK && !config.is_null() {
    // 使用配置...
    Ok(UcpConfig { handle: config })
} else {
    Err("UCX 配置创建失败")
}
```

### 网络地址查询
```rust
// ✅ 对于小结构体
let mut addr = sockaddr_in {
    sin_family: 0,
    sin_port: 0,
    sin_addr: in_addr { s_addr: 0 },
    sin_zero: [0; 8],
};
let status = unsafe {
    get_socket_address(socket_fd, &mut addr)
};
```

### 大缓冲区分配
```rust
// ✅ 对于大数据结构
let mut buffer = MaybeUninit::<[u8; 4096]>::uninit();
let status = unsafe {
    read_large_data(buffer.as_mut_ptr() as *mut u8, 4096)
};

if status > 0 {
    let buffer = unsafe { buffer.assume_init() };
    // 使用前 status 个字节...
}
```

## 🛡️ 安全检查清单

### 调用 C 函数前
- [ ] 输出参数已正确初始化
- [ ] 指针参数不为野指针
- [ ] 缓冲区大小足够

### 调用 C 函数后
- [ ] 检查返回状态/错误码
- [ ] 验证输出参数的有效性
- [ ] 指针参数不为空（如果期望非空）
- [ ] 结构体字段在预期范围内

### 使用输出值前
- [ ] 确认 C 函数调用成功
- [ ] 对于 MaybeUninit，只在确认初始化后调用 assume_init()
- [ ] 对于指针，检查非空

## ⚡ 性能考虑

### 指针输出参数
```rust
// ✅ 高效：直接操作
let mut ptr: *mut T = ptr::null_mut();
c_function(&mut ptr);

// ❌ 低效：额外包装
let mut ptr = MaybeUninit::<*mut T>::uninit();
c_function(ptr.as_mut_ptr());
let ptr = ptr.assume_init();
```

### 大结构体输出
```rust
// ✅ 高效：避免初始化开销
let mut data = MaybeUninit::<LargeStruct>::uninit();
c_function(data.as_mut_ptr());

// ❌ 低效：不必要的初始化
let mut data = LargeStruct::default();  // 可能很慢
c_function(&mut data);
```

## 🧹 资源管理

### RAII 包装
```rust
pub struct SafeHandle {
    ptr: *mut HandleType,
}

impl SafeHandle {
    pub fn new() -> Result<Self> {
        let mut ptr: *mut HandleType = ptr::null_mut();
        let status = unsafe { create_handle(&mut ptr) };
        
        if status == 0 && !ptr.is_null() {
            Ok(SafeHandle { ptr })
        } else {
            Err("句柄创建失败")
        }
    }
}

impl Drop for SafeHandle {
    fn drop(&mut self) {
        if !self.ptr.is_null() {
            unsafe {
                destroy_handle(self.ptr);
                self.ptr = ptr::null_mut();
            }
        }
    }
}
```

## 🎯 总结

**指导原则：**
1. **指针输出参数** → 使用 `ptr::null_mut()`
2. **小值类型输出** → 使用默认初始化
3. **大值类型输出** → 使用 `MaybeUninit`
4. **总是检查** C 函数返回值
5. **验证输出** 参数的有效性
6. **使用 RAII** 进行资源管理

这样可以确保 FFI 代码既安全又高效！
