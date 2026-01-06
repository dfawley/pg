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
    fn call<E: Encoder, D: Decoder>(
        &self,
        descriptor: MethodDescriptor<E, D>,
        args: Args,
    ) -> impl Future<Output = (impl SendStream<E>, impl RecvStream<D>)> + Send {
        self.interceptor.call(descriptor, args, &self.call)
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
    fn call<E: Encoder, D: Decoder>(
        self,
        descriptor: MethodDescriptor<E, D>,
        args: Args,
    ) -> impl Future<Output = (impl SendStream<E>, impl RecvStream<D>)> + Send {
        self.interceptor.call(descriptor, args, self.call)
    }
}

pub trait CallExt: Call + Sized {
    fn with_interceptor<I: CallInterceptor>(self, interceptor: I) -> impl Call;
}

impl<C: Call> CallExt for C {
    fn with_interceptor<I: CallInterceptor>(self, interceptor: I) -> impl Call {
        Interceptor::new(self, interceptor)
    }
}

pub trait CallOnceExt: CallOnce + Sized {
    fn with_interceptor<I: CallInterceptorOnce>(self, interceptor: I) -> impl CallOnce;
}

impl<C: CallOnce> CallOnceExt for C {
    fn with_interceptor<I: CallInterceptorOnce>(self, interceptor: I) -> impl CallOnce {
        InterceptorOnce::new(self, interceptor)
    }
}
