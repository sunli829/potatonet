#[doc(hidden)]
#[macro_export]
macro_rules! msg_and_kvs {
    ($fmt:literal, $($tail:tt)+) => {
        msg_and_kvs!(@fmt $fmt @args [] $($tail)+)
    };

    ($fmt:literal) => {
        ($fmt.to_string(), Option::<$crate::KvsHashMap>::None)
    };

    // 开始kv参数
    (@fmt $fmt:literal @args [$($args:expr,)*] $key:ident = $value:expr, $($tail:tt)+) => {
        msg_and_kvs!(@fmt $fmt @args [$($args,)*] @kvs [$key = $value,] $($tail)+)
    };

    // kv参数
    (@fmt $fmt:literal @args [$($args:expr,)*] @kvs [$($pkey:ident = $pvalue:expr,)*] $key:ident = $value:expr, $($tail:tt)+) => {
        msg_and_kvs!(@fmt $fmt @args [$($args,)*] @kvs [$($pkey = $pvalue,)* $key = $value,] $($tail)+)
    };

    // 最后一个kv参数
    (@fmt $fmt:literal @args [$($args:expr,)*] $key:ident = $value:expr) => {
        msg_and_kvs!(@finish @fmt $fmt @args [$($args,)*] @kvs [$key = $value,])
    };

    // 最后一个kv参数
    (@fmt $fmt:literal @args [$($args:expr,)*] @kvs [$($pkey:ident = $pvalue:expr,)*] $key:ident = $value:expr) => {
        msg_and_kvs!(@finish @fmt $fmt @args [$($args,)*] @kvs [$($pkey = $pvalue,)* $key = $value,])
    };

    // 解析格式化参数
    (@fmt $fmt:literal @args [$($args:expr,)*] $value:expr, $($tail:tt)+) => {
        msg_and_kvs!(@fmt $fmt @args [$($args,)* $value,] $($tail)+)
    };

    // 最后一个格式化参数
    (@fmt $fmt:literal @args [$($args:expr,)*] $value:expr) => {
        msg_and_kvs!(@finish @fmt $fmt @args [$($args,)* $value,] @kvs [])
    };

    // 生成代码
    (@finish @fmt $fmt:literal @args [$($args:expr,)*] @kvs [$($key:ident = $value:expr,)*]) => {
        {
            #[allow(unused_mut)] // TODO: 这是编译器的一个bug，认为无需mut
            let mut kvs = $crate::KvsHashMap::new();
            $(
                kvs.insert(stringify!($key).to_string(), $value.into());
            )*
            let msg = format!($fmt, $($args,)*);
            (msg, if kvs.is_empty() { None } else { Some(kvs) })
        }
    };
}

/// 输出日志
#[macro_export]
macro_rules! log {
    ($ctx:expr, $level:expr, $($tail:tt)*) => {
        let (message, kvs) = msg_and_kvs!($($tail)*);
        let item = $crate::Item {
            time: $crate::chrono::Utc::now(),
            service: $ctx.service_name().to_string(),
            level: $level,
            message,
            kvs,
        };
        $crate::LoggerClient::new($ctx).log(item).await;
    };
}

/// 输出TRACE日志
#[macro_export]
macro_rules! trace {
    ($ctx:expr, $fmt:literal, $($tail:tt)*) => {
        log!($ctx, $crate::Level::Trace, $fmt, $($tail)*);
    }
}

/// 输出DEBUG日志
#[macro_export]
macro_rules! debug {
    ($ctx:expr, $fmt:literal, $($tail:tt)*) => {
        log!($ctx, $crate::Level::Debug, $fmt, $($tail)*);
    }
}

/// 输出INFO日志
#[macro_export]
macro_rules! info {
    ($ctx:expr, $fmt:literal, $($tail:tt)*) => {
        log!($ctx, $crate::Level::Info, $fmt, $($tail)*);
    }
}

/// 输出WARN日志
#[macro_export]
macro_rules! warn {
    ($ctx:expr, $fmt:literal, $($tail:tt)*) => {
        log!($ctx, $crate::Level::Warn, $fmt, $($tail)*);
    }
}

/// 输出ERROR日志
#[macro_export]
macro_rules! error {
    ($ctx:expr, $fmt:literal, $($tail:tt)*) => {
        log!($ctx, $crate::Level::Error, $fmt, $($tail)*);
    }
}

#[test]
fn test() {
    assert_eq!(msg_and_kvs!("haha"), ("haha".to_string(), None));
    assert_eq!(msg_and_kvs!("haha {}", 1), ("haha 1".to_string(), None));
    assert_eq!(
        msg_and_kvs!("haha {} {}", 1, 2),
        ("haha 1 2".to_string(), None)
    );
    assert_eq!(
        msg_and_kvs!("haha", a = 10),
        (
            "haha".to_string(),
            Some({
                let mut kvs = crate::KvsHashMap::new();
                kvs.insert("a".to_string(), 10.into());
                kvs
            })
        )
    );
    assert_eq!(
        msg_and_kvs!("haha {}", 1, a = 10),
        (
            "haha 1".to_string(),
            Some({
                let mut kvs = crate::KvsHashMap::new();
                kvs.insert("a".to_string(), 10.into());
                kvs
            })
        )
    );

    assert_eq!(
        msg_and_kvs!("haha {}", 1, a = 10, b = 20),
        (
            "haha 1".to_string(),
            Some({
                let mut kvs = crate::KvsHashMap::new();
                kvs.insert("a".to_string(), 10.into());
                kvs.insert("b".to_string(), 20.into());
                kvs
            })
        )
    );
    assert_eq!(
        msg_and_kvs!("haha {} {}", 1, 2, a = 10, b = 20),
        (
            "haha 1 2".to_string(),
            Some({
                let mut kvs = crate::KvsHashMap::new();
                kvs.insert("a".to_string(), 10.into());
                kvs.insert("b".to_string(), 20.into());
                kvs
            })
        )
    );
}
