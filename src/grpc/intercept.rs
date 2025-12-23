use super::*;

#[derive(Clone)]
pub struct Interceptor<C: Clone, I: Clone> {
    callable: C,
    call_interceptor: I,
}

impl<C: Clone, I: Clone> Interceptor<C, I> {
    pub fn new(callable: C, call_interceptor: I) -> Self {
        Self {
            callable,
            call_interceptor,
        }
    }
}

struct InterceptorCall<'a, C: Clone, I: Clone> {
    interceptor: &'a Interceptor<C, I>,
}

impl<'a, C: Callable + Clone, I: CallInterceptor + Clone, E: Encoder, D: Decoder> Call<E, D>
    for InterceptorCall<'a, C, I>
{
    fn start(
        self,
        descriptor: MethodDescriptor<E, D>,
        args: Args,
    ) -> impl Future<Output = (impl SendStream<E>, impl RecvStream<D>)> + Send {
        self.interceptor
            .call_interceptor
            .start(descriptor, args, &self.interceptor.callable)
    }
}

impl<C: Callable + Clone, I: CallInterceptor + Clone> Callable for Interceptor<C, I> {
    fn call<E: Encoder, D: Decoder>(&self) -> impl Call<E, D> {
        InterceptorCall { interceptor: self }
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

impl<C: CallableOnce, I: CallInterceptorOnce, E: Encoder, D: Decoder> Call<E, D>
    for InterceptorOnce<C, I>
{
    fn start(
        self,
        descriptor: MethodDescriptor<E, D>,
        args: Args,
    ) -> impl Future<Output = (impl SendStream<E>, impl RecvStream<D>)> + Send {
        self.call_interceptor.start(descriptor, args, self.callable)
    }
}
