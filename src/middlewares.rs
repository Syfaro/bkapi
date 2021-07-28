use std::collections::HashMap;
use std::convert::TryFrom;

use opentelemetry::{
    global::get_text_map_propagator,
    trace::{FutureExt, Span, SpanKind, TraceContextExt, Tracer},
    Context,
};
use opentelemetry_semantic_conventions::trace;
use prometheus::{Encoder, TextEncoder};
use tide::{
    http::{
        headers::{HeaderName, HeaderValue},
        mime,
    },
    Middleware, Request, Response,
};

pub struct TidePrometheusMiddleware;

impl TidePrometheusMiddleware {
    const ROUTE: &'static str = "/metrics";
}

#[tide::utils::async_trait]
impl<State: Clone + Send + Sync + 'static> Middleware<State> for TidePrometheusMiddleware {
    async fn handle(&self, req: Request<State>, next: tide::Next<'_, State>) -> tide::Result {
        let path = req.url().path().to_owned();

        if path == Self::ROUTE {
            let mut buffer = Vec::new();
            let encoder = TextEncoder::new();

            let metric_families = prometheus::gather();
            encoder.encode(&metric_families, &mut buffer).unwrap();

            return Ok(Response::builder(200)
                .body(buffer)
                .content_type(mime::PLAIN)
                .build());
        }

        let method = req.method().to_string();

        let start = std::time::Instant::now();
        let res = next.run(req).await;
        let end = std::time::Instant::now().duration_since(start);

        let status_code = res.status().to_string();

        let labels: Vec<&str> = vec![&path, &method, &status_code];

        crate::HTTP_REQUEST_COUNT.with_label_values(&labels).inc();
        crate::HTTP_REQUEST_DURATION
            .with_label_values(&labels)
            .observe(end.as_secs_f64());

        Ok(res)
    }
}

pub struct TideOpentelemMiddleware<T: Tracer> {
    tracer: T,
}

impl<T: Tracer> TideOpentelemMiddleware<T> {
    pub fn new(tracer: T) -> Self {
        Self { tracer }
    }
}

#[tide::utils::async_trait]
impl<T: Tracer + Send + Sync, State: Clone + Send + Sync + 'static> Middleware<State>
    for TideOpentelemMiddleware<T>
{
    async fn handle(&self, req: Request<State>, next: tide::Next<'_, State>) -> tide::Result {
        let parent_cx = get_parent_cx(&req);

        let method = req.method().to_string();
        let url = req.url();

        let attributes = vec![
            trace::HTTP_METHOD.string(method.clone()),
            trace::HTTP_SCHEME.string(url.scheme().to_string()),
            trace::HTTP_URL.string(url.to_string()),
        ];

        let mut span_builder = self
            .tracer
            .span_builder(format!("{} {}", method, url.path()))
            .with_kind(SpanKind::Server)
            .with_attributes(attributes);

        if parent_cx.span().span_context().is_remote() {
            tracing::trace!("incoming request has remote span: {:?}", parent_cx);
            span_builder = span_builder.with_parent_context(parent_cx);
        }

        let mut span = span_builder.start(&self.tracer);
        span.add_event("request.started".to_owned(), vec![]);
        let cx = &Context::current_with_span(span);

        let mut res = next.run(req).with_context(cx.clone()).await;

        let span = cx.span();
        span.add_event("request.completed".to_owned(), vec![]);
        span.set_attribute(trace::HTTP_STATUS_CODE.i64(u16::from(res.status()).into()));

        if let Some(len) = res.len().and_then(|len| i64::try_from(len).ok()) {
            span.set_attribute(trace::HTTP_RESPONSE_CONTENT_LENGTH.i64(len));
        }

        let mut injector = HashMap::new();
        get_text_map_propagator(|propagator| propagator.inject_context(&cx, &mut injector));

        for (key, value) in injector {
            let header_name = HeaderName::from_bytes(key.into_bytes());
            let header_value = HeaderValue::from_bytes(value.into_bytes());

            if let (Ok(name), Ok(value)) = (header_name, header_value) {
                res.insert_header(name, value);
            } else {
                tracing::error!("injected header data was invalid");
            }
        }

        Ok(res)
    }
}

fn get_parent_cx<State>(req: &Request<State>) -> Context {
    let mut req_headers = HashMap::new();

    for (key, value) in req.iter() {
        req_headers.insert(key.to_string(), value.last().to_string());
    }

    get_text_map_propagator(|propagator| propagator.extract(&req_headers))
}
