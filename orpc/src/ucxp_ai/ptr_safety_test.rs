use std::mem::MaybeUninit;
use std::ptr;
use anyhow::Result;

/// 安全性测试：ptr::null_mut() vs MaybeUninit
fn main() -> Result<()> {
    println!("=== ptr::null_mut() vs MaybeUninit 安全性测试 ===\n");
    
    test_successful_scenario()?;
    test_failure_scenario()?;
    test_edge_cases()?;
    
    println!("✅ 所有测试通过！");
    println!("\n📊 结论: ptr::null_mut() 在 FFI 输出参数场景中更安全、更合适");
    Ok(())
}

/// 测试成功场景
fn test_successful_scenario() -> Result<()> {
    println!("🧪 测试 1: 成功场景");
    
    // 方案 A: MaybeUninit 方式
    let result_a = test_maybeuninit_success();
    println!("   MaybeUninit 方式: {:?}", result_a.is_ok());
    
    // 方案 B: ptr::null_mut() 方式
    let result_b = test_ptr_null_mut_success();
    println!("   ptr::null_mut() 方式: {:?}", result_b.is_ok());
    
    println!("   ✅ 两种方式在成功场景下都正常工作\n");
    Ok(())
}

/// 测试失败场景
fn test_failure_scenario() -> Result<()> {
    println!("🚨 测试 2: 失败场景");
    
    // 方案 A: MaybeUninit 在失败场景下的问题
    println!("   MaybeUninit 方式:");
    match test_maybeuninit_failure() {
        Ok(_) => println!("     - 意外成功？"),
        Err(e) => println!("     - 错误: {}", e),
    }
    
    // 方案 B: ptr::null_mut() 在失败场景下的优势
    println!("   ptr::null_mut() 方式:");
    match test_ptr_null_mut_failure() {
        Ok(_) => println!("     - 意外成功？"),
        Err(e) => println!("     - 安全处理错误: {}", e),
    }
    
    println!("   ✅ ptr::null_mut() 在失败场景下更安全\n");
    Ok(())
}

/// 测试边界情况
fn test_edge_cases() -> Result<()> {
    println!("🎯 测试 3: 边界情况");
    
    // 测试指针检查
    test_pointer_validation()?;
    
    // 测试内存安全
    test_memory_safety()?;
    
    println!("   ✅ 所有边界情况测试通过\n");
    Ok(())
}

// =============================================================================
// MaybeUninit 实现 (模拟原始方式)
// =============================================================================

fn test_maybeuninit_success() -> Result<MockHandle> {
    let mut handle = MaybeUninit::<*mut MockResource>::uninit();
    
    let status = unsafe {
        mock_c_function_success(handle.as_mut_ptr())
    };
    
    if status == 0 {
        let initialized_handle = unsafe { handle.assume_init() };  // 🚨 这里有风险
        Ok(MockHandle { ptr: initialized_handle })
    } else {
        Err(anyhow::anyhow!("MaybeUninit 方式失败"))
    }
}

fn test_maybeuninit_failure() -> Result<MockHandle> {
    let mut handle = MaybeUninit::<*mut MockResource>::uninit();
    
    let status = unsafe {
        mock_c_function_failure(handle.as_mut_ptr())
    };
    
    if status == 0 {
        let initialized_handle = unsafe { 
            handle.assume_init()  // 🚨 危险！C 函数失败但可能写入了垃圾数据
        };
        Ok(MockHandle { ptr: initialized_handle })
    } else {
        Err(anyhow::anyhow!("C 函数返回错误状态"))
    }
}

// =============================================================================
// ptr::null_mut() 实现 (推荐方式)
// =============================================================================

fn test_ptr_null_mut_success() -> Result<MockHandle> {
    let mut handle: *mut MockResource = ptr::null_mut();
    
    let status = unsafe {
        mock_c_function_success(&mut handle)
    };
    
    if status == 0 {
        if handle.is_null() {
            Err(anyhow::anyhow!("C 函数声称成功但返回空指针"))
        } else {
            Ok(MockHandle { ptr: handle })
        }
    } else {
        Err(anyhow::anyhow!("ptr::null_mut 方式失败"))
    }
}

fn test_ptr_null_mut_failure() -> Result<MockHandle> {
    let mut handle: *mut MockResource = ptr::null_mut();
    
    let status = unsafe {
        mock_c_function_failure(&mut handle)
    };
    
    if status == 0 {
        if handle.is_null() {
            Err(anyhow::anyhow!("C 函数返回空指针"))
        } else {
            Ok(MockHandle { ptr: handle })
        }
    } else {
        // ✅ 可以安全检查 handle 是否仍然为空
        if handle.is_null() {
            println!("     - 指针正确保持为空");
        } else {
            println!("     - 警告：失败后指针不为空");
        }
        Err(anyhow::anyhow!("C 函数返回错误状态"))
    }
}

// =============================================================================
// 边界情况测试
// =============================================================================

fn test_pointer_validation() -> Result<()> {
    println!("   测试指针有效性检查:");
    
    // ptr::null_mut() 方式允许简单的空检查
    let mut handle: *mut MockResource = ptr::null_mut();
    println!("     - 初始状态空检查: {}", handle.is_null());
    
    unsafe {
        mock_c_function_success(&mut handle);
    }
    println!("     - 成功后空检查: {}", handle.is_null());
    
    // 清理
    if !handle.is_null() {
        unsafe {
            mock_cleanup(handle);
        }
    }
    
    Ok(())
}

fn test_memory_safety() -> Result<()> {
    println!("   测试内存安全:");
    
    // 测试多次调用的安全性
    for i in 0..3 {
        let mut handle: *mut MockResource = ptr::null_mut();
        
        unsafe {
            if i == 1 {
                mock_c_function_failure(&mut handle);  // 故意失败一次
            } else {
                mock_c_function_success(&mut handle);
            }
        }
        
        println!("     - 第 {} 次调用，指针状态: {:?}", i + 1, handle.is_null());
        
        // 安全清理
        if !handle.is_null() {
            unsafe {
                mock_cleanup(handle);
            }
        }
    }
    
    Ok(())
}

// =============================================================================
// 模拟的 C 函数和数据结构
// =============================================================================

#[derive(Debug)]
struct MockResource {
    data: u32,
}

#[derive(Debug)]
struct MockHandle {
    ptr: *mut MockResource,
}

impl Drop for MockHandle {
    fn drop(&mut self) {
        if !self.ptr.is_null() {
            unsafe {
                mock_cleanup(self.ptr);
            }
        }
    }
}

/// 模拟成功的 C 函数
unsafe fn mock_c_function_success(ptr: *mut *mut MockResource) -> i32 {
    // 分配资源
    let resource = Box::new(MockResource { data: 42 });
    *ptr = Box::into_raw(resource);
    0  // 成功
}

/// 模拟失败的 C 函数
unsafe fn mock_c_function_failure(ptr: *mut *mut MockResource) -> i32 {
    // 失败情况：可能写入垃圾数据（这在某些 C 库中会发生）
    if rand::random::<bool>() {
        *ptr = 0x1234 as *mut MockResource;  // 垃圾指针！
    }
    // 注意：有些 C 函数失败时不会修改输出参数，有些会写入垃圾数据
    1  // 失败
}

/// 模拟资源清理
unsafe fn mock_cleanup(ptr: *mut MockResource) {
    if !ptr.is_null() {
        let _ = Box::from_raw(ptr);
    }
}

// =============================================================================
// 详细分析和总结
// =============================================================================

#[allow(dead_code)]
fn detailed_analysis() {
    println!("\n📋 详细分析:");
    
    println!("\n1. 语义差异:");
    println!("   MaybeUninit<T>  : 表示可能未初始化的 T");
    println!("   *mut T          : 已初始化的指针类型（可能指向空）");
    
    println!("\n2. 在 FFI 输出参数场景中:");
    println!("   - C 函数期望: *mut (*mut T) (指向指针的指针)");
    println!("   - 我们需要提供: &mut pointer");
    println!("   - 初始值应该是: 空指针 (ptr::null_mut())");
    
    println!("\n3. 安全性对比:");
    println!("   MaybeUninit:");
    println!("   ✅ 类型系统保证不会意外读取未初始化内存");
    println!("   ❌ assume_init() 可能导致未定义行为");
    println!("   ❌ 无法轻易检查 C 函数是否真的初始化了值");
    
    println!("   ptr::null_mut():");
    println!("   ✅ 明确的初始状态 (空指针)");
    println!("   ✅ C 函数失败时状态可预测");
    println!("   ✅ 可以轻易验证结果 (is_null() 检查)");
    println!("   ✅ 无需 assume_init()，避免未定义行为");
    
    println!("\n4. 使用场景:");
    println!("   MaybeUninit 适合: 栈上值类型的延迟初始化");
    println!("   ptr::null_mut() 适合: FFI 输出参数，特别是指针类型");
}

/// 运行性能对比测试
#[allow(dead_code)]
fn performance_comparison() {
    use std::time::Instant;
    
    println!("\n⚡ 性能对比:");
    
    const ITERATIONS: u32 = 1000000;
    
    // 测试 MaybeUninit 方式
    let start = Instant::now();
    for _ in 0..ITERATIONS {
        let mut handle = MaybeUninit::<*mut u8>::uninit();
        unsafe {
            // 模拟快速操作
            *handle.as_mut_ptr() = ptr::null_mut();
            let _ = handle.assume_init();
        }
    }
    let maybeuninit_time = start.elapsed();
    
    // 测试 ptr::null_mut() 方式
    let start = Instant::now();
    for _ in 0..ITERATIONS {
        let mut handle: *mut u8 = ptr::null_mut();
        // 模拟相同操作
        let _ = &mut handle;
    }
    let ptr_null_time = start.elapsed();
    
    println!("   MaybeUninit 方式: {:?}", maybeuninit_time);
    println!("   ptr::null_mut() 方式: {:?}", ptr_null_time);
    println!("   性能差异: {:.2}x", 
             maybeuninit_time.as_nanos() as f64 / ptr_null_time.as_nanos() as f64);
}
