use std::panic::{AssertUnwindSafe, PanicHookInfo};
use std::sync::Mutex;

use anyhow::anyhow;

/// Wraps a function call to catch panics and transform them into
/// `Err` (which `cxx` will then transform into a C++ exception).
pub fn panic_to_exception<F, R>(f: F) -> anyhow::Result<R>
where
    F: FnOnce() -> anyhow::Result<R>,
{
    let e = Mutex::new(String::new());
    let hook_closure = Box::new(|p: &PanicHookInfo<'_>| {
        if let Ok(mut e) = e.lock() {
            *e = p.to_string();
        }
    });

    let catch = {
        let _registration = PanicHook::new(hook_closure);
        std::panic::catch_unwind(AssertUnwindSafe(f))
    };
    match catch {
        Ok(Ok(r)) => Ok(r),
        Ok(Err(e)) => Err(e),
        Err(_) => Err(anyhow!(e.into_inner()?)),
    }
}

type FnPanicHook<'a> = Box<dyn Fn(&PanicHookInfo<'_>) + Sync + Send + 'a>;

struct PanicHook<'a> {
    prev: Option<FnPanicHook<'static>>,
    _lifetime: std::marker::PhantomData<&'a ()>,
}

impl<'a> PanicHook<'a> {
    pub fn new(hook: FnPanicHook<'a>) -> Self {
        let prev = std::panic::take_hook();

        let hook_scoped =
            unsafe { std::mem::transmute::<FnPanicHook<'a>, FnPanicHook<'static>>(hook) };
        std::panic::set_hook(hook_scoped);

        Self {
            prev: Some(prev),
            _lifetime: Default::default(),
        }
    }
}

impl<'a> Drop for PanicHook<'a> {
    fn drop(&mut self) {
        let _ = std::panic::take_hook();

        if let Some(prev) = self.prev.take() {
            std::panic::set_hook(prev);
        }
    }
}
