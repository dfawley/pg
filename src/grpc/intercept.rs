use super::*;

#[derive(Clone)]
pub struct CallableInterceptor<C, I> {
    callable: C,
    call_interceptor: I,
}

impl<C, I> CallableInterceptor<C, I> {
    pub fn new(callable: C, call_interceptor: I) -> Self {
        Self {
            callable,
            call_interceptor,
        }
    }
}

struct InterceptorCall<'a, C, I> {
    callable: &'a CallableInterceptor<C, I>,
}

impl<'a, C, I, E, D> Call<E, D> for InterceptorCall<'a, C, I>
where
    C: Callable,
    I: CallInterceptor,
    E: Encoder,
    D: Decoder,
{
    fn start(
        self,
        descriptor: MethodDescriptor<E, D>,
        args: Args,
    ) -> impl Future<Output = (impl SendStream<E>, impl RecvStream<D>)> + Send {
        self.callable
            .call_interceptor
            .start(descriptor, args, &self.callable.callable)
    }
}

impl<C: Callable, I: CallInterceptor> Callable for CallableInterceptor<C, I> {
    fn call<E: Encoder, D: Decoder>(&self) -> impl Call<E, D> {
        InterceptorCall { callable: self }
    }
}

pub struct InterceptorOnce<C, I> {
    callable: C,
    call_interceptor: I,
}

impl<C, I> InterceptorOnce<C, I> {
    pub fn new(callable: C, call_interceptor: I) -> Self {
        Self {
            callable,
            call_interceptor,
        }
    }
}

impl<C: CallableOnce, I: CallInterceptorOnce> CallableOnce for InterceptorOnce<C, I> {
    fn call<E: Encoder, D: Decoder>(self) -> impl Call<E, D> {
        self
    }
}

impl<C, I, E, D> Call<E, D> for InterceptorOnce<C, I>
where
    C: CallableOnce,
    I: CallInterceptorOnce,
    E: Encoder,
    D: Decoder,
{
    fn start(
        self,
        descriptor: MethodDescriptor<E, D>,
        args: Args,
    ) -> impl Future<Output = (impl SendStream<E>, impl RecvStream<D>)> + Send {
        self.call_interceptor.start(descriptor, args, self.callable)
    }
}

pub trait CallableExt: Callable + Sized {
    fn with_interceptor<I: CallInterceptor>(self, interceptor: I) -> CallableInterceptor<Self, I>;
}

impl<C: Callable> CallableExt for C {
    fn with_interceptor<I: CallInterceptor>(self, interceptor: I) -> CallableInterceptor<Self, I> {
        CallableInterceptor {
            callable: self,
            call_interceptor: interceptor,
        }
    }
}
