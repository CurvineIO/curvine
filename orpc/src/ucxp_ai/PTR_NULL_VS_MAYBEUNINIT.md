# ptr::null_mut() vs MaybeUninit 安全性分析

## 🤔 问题核心

用 `ptr::null_mut()` 替换 `MaybeUninit` 是否会有问题？

**简短答案**: 在 FFI 输出参数场景中，`ptr::null_mut()` **不仅安全，而且更合适**。

## 🔍 详细分析

### 场景对比

#### 当前 UCX 的使用场景：
```rust
// C 函数签名 (概念上):
// int ucp_config_read(const char* env_prefix, const char* filename, ucp_config_t** config_p);
//                                                                    ^^^^^^^^^^^
//                                                                    输出参数
```

这是一个典型的 **C 输出参数模式**：
- C 函数接收一个指向指针的指针
- 函数内部分配内存并设置指针值
- 调用者提供指针的地址供函数写入

### 方案对比

#### ❌ 方案 A: MaybeUninit (过度复杂)
```rust
let mut handle = MaybeUninit::<*mut ucp_config_t>::uninit();
let status = unsafe {
    ucp_config_read(
        ptr::null(), 
        ptr::null(), 
        handle.as_mut_ptr()  // *mut (*mut ucp_config_t)
    )
};
let handle = unsafe { handle.assume_init() };  // 🚨 潜在危险
```

**问题：**
- `assume_init()` 假设内存已被正确初始化
- 如果 C 函数失败但仍然写入了垃圾数据，会导致未定义行为
- 过度复杂化了简单的指针传递

#### ✅ 方案 B: ptr::null_mut() (推荐)
```rust
let mut handle: *mut ucp_config_t = ptr::null_mut();
let status = unsafe {
    ucp_config_read(
        ptr::null(), 
        ptr::null(), 
        &mut handle  // *mut (*mut ucp_config_t)
    )
};
// handle 现在包含有效指针（如果成功）或保持为空（如果失败）
```

**优势：**
- 初始状态明确：空指针
- C 函数失败时，指针保持为空（可检查）
- 无需 `assume_init()`，避免未定义行为
- 代码更清晰简洁

## 🧪 实际测试

让我创建一个测试来证明这个观点：
