use super::*;

#[derive(Clone)]
pub struct Interceptor<C, I> {
    call: C,
    interceptor: I,
}

impl<C, I> Interceptor<C, I> {
    pub fn new(callable: C, call_interceptor: I) -> Self {
        Self {
            call: callable,
            interceptor: call_interceptor,
        }
    }
}

impl<C, I> Call for Interceptor<C, I>
where
    C: Call,
    I: CallInterceptor,
{
    fn call(&self, method: String, args: Args) -> (impl ClientSendStream, impl ClientRecvStream) {
        self.interceptor.call(method, args, &self.call)
    }
}

pub struct InterceptorOnce<C, I> {
    call: C,
    interceptor: I,
}

impl<C, I> InterceptorOnce<C, I> {
    pub fn new(call: C, interceptor: I) -> Self {
        Self { call, interceptor }
    }
}

impl<C: CallOnce, I: CallInterceptorOnce> CallOnce for InterceptorOnce<C, I> {
    fn call_once(
        self,
        method: String,
        args: Args,
    ) -> (impl ClientSendStream, impl ClientRecvStream) {
        self.interceptor.call_once(method, args, self.call)
    }
}

pub trait CallExt: Call + Sized {
    fn with_interceptor(self, interceptor: impl CallInterceptor) -> impl Call;
}

impl<C: Call> CallExt for C {
    fn with_interceptor(self, interceptor: impl CallInterceptor) -> impl Call {
        Interceptor::new(self, interceptor)
    }
}

pub trait CallOnceExt: CallOnce + Sized {
    fn with_interceptor(self, interceptor: impl CallInterceptorOnce) -> impl CallOnce;
}

impl<C: CallOnce> CallOnceExt for C {
    fn with_interceptor(self, interceptor: impl CallInterceptorOnce) -> impl CallOnce {
        InterceptorOnce::new(self, interceptor)
    }
}
