#[cfg(feature = "debug")]
#[macro_export]
macro_rules! jbd_assert {
    ($e:expr) => {
        assert!(
            $e,
            "jbd-rs: unexpected error at {}:{}:{}",
            file!(),
            line!(),
            column!()
        );
    };
}

#[cfg(not(feature = "debug"))]
#[macro_export]
macro_rules! jbd_assert {
    ($e:expr) => {};
}
