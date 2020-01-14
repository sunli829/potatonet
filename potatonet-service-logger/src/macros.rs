/// 输出日志
#[macro_export]
macro_rules! log {
    // 开始kv参数
    ($ctx:expr, $level:expr, $fmt:literal, @args [$($args:expr,)*] $key:ident = $value:expr, $($tail:tt)+) => {
        log!($ctx, $level, $fmt, @args [$($args,)*] @kvs [$key = $value,] $($tail)+)
    };

    // kv参数
    ($ctx:expr, $level:expr, $fmt:literal, @args [$($args:expr,)*] @kvs [$($pkey:ident = $pvalue:expr,)*] $key:ident = $value:expr, $($tail:tt)+) => {
        log!($ctx, $level, $fmt, @args [$($args,)*] @kvs [$($pkey = $pvalue,)* $key = $value,] $($tail)+)
    };

    // 最后一个kv参数
    ($ctx:expr, $level:expr, $fmt:literal, @args [$($args:expr,)*] @kvs [$($pkey:ident = $pvalue:expr,)*] $key:ident = $value:expr) => {
        log!($ctx, $level, $fmt, @args [$($args,)*] @kvs [$($pkey = $pvalue,)* $key = $value,])
    };

    // 格式化参数
    ($ctx:expr, $level:expr, $fmt:literal, @args [$($args:expr,)*] $value:expr, $($tail:tt)+) => {
        log!($ctx, $level, $fmt, @args [$($args,)* $value,] $($tail)+)
    };

    // 生成代码
    ($ctx:expr, $level:expr, $fmt:literal, @args [$($args:expr,)*] @kvs [$($key:ident = $value:expr,)*]) => {
        let mut kvs = Vec::new();
        $(
            kvs.push((stringify!($key), $value.into()));
        )*

        let item = $crate::Item {
            time: $crate::chrono::Utc::now(),
            service: $ctx.service_name().to_string(),
            level: $level,
            message: format!($fmt, $($args,)*),
            kvs: if kvs.is_empty() { None } else { kvs },
        };
        $crate::LoggerClient::new($ctx).log(item);
    };

    // 开始解析格式化参数
    ($ctx:expr, $level:expr, $fmt:literal, $($tail:tt)+) => {
        log!($ctx, $level, $fmt, @args [] $($tail)+)
    };
}

/// 输出TRACE日志
#[macro_export]
macro_rules! trace {
    ($ctx:expr, $fmt:literal, $($tail:tt)+) => {
        log!($ctx, $crate::Level::Trace, $fmt, $($tail)+);
    }
}

/// 输出DEBUG日志
#[macro_export]
macro_rules! debug {
    ($ctx:expr, $fmt:literal, $($tail:tt)+) => {
        log!($ctx, $crate::Level::Debug, $fmt, $($tail)+);
    }
}

/// 输出INFO日志
#[macro_export]
macro_rules! info {
    ($ctx:expr, $fmt:literal, $($tail:tt)+) => {
        log!($ctx, $crate::Level::Info, $fmt, $($tail)+);
    }
}

/// 输出WARN日志
#[macro_export]
macro_rules! warn {
    ($ctx:expr, $fmt:literal, $($tail:tt)+) => {
        log!($ctx, $crate::Level::Warn, $fmt, $($tail)+);
    }
}

/// 输出ERROR日志
#[macro_export]
macro_rules! error {
    ($ctx:expr, $fmt:literal, $($tail:tt)+) => {
        log!($ctx, $crate::Level::Error, $fmt, $($tail)+);
    }
}
