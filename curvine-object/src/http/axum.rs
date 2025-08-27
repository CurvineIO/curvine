use std::collections::HashMap;

use axum::response::IntoResponse;
use futures::StreamExt;

use crate::auth::sig_v4;
use crate::s3::s3_api::VRequest;
use axum::body;
// use std::sync::Arc; // Used via std::sync::Arc in code

pub struct Request {
    request: axum::http::Request<axum::body::Body>,
    query: Option<HashMap<String, String>>,
}
impl From<Request> for axum::extract::Request {
    fn from(val: Request) -> Self {
        val.request
    }
}
impl From<axum::extract::Request> for Request {
    fn from(value: axum::extract::Request) -> Self {
        let query = value.uri().query().map(|query| {
            query
                .split("&")
                .map(|item| {
                    item.find("=")
                        .map_or((item.to_string(), "".to_string()), |pos| {
                            (item[..pos].to_string(), item[pos + 1..].to_string())
                        })
                })
                .collect::<std::collections::HashMap<String, String>>()
        });
        Self {
            request: value,
            query,
        }
    }
}
impl sig_v4::VHeader for Request {
    fn get_header(&self, key: &str) -> Option<String> {
        self.request
            .headers()
            .get(key)
            .and_then(|value| value.to_str().ok().map(|value| value.to_string()))
    }

    fn set_header(&mut self, key: &str, val: &str) {
        let key: axum::http::HeaderName = key.to_string().parse().unwrap();
        self.request.headers_mut().insert(key, val.parse().unwrap());
    }

    fn delete_header(&mut self, key: &str) {
        self.request.headers_mut().remove(key);
    }

    fn rng_header(&self, mut cb: impl FnMut(&str, &str) -> bool) {
        for (k, v) in self.request.headers().iter() {
            if !cb(k.as_str(), unsafe {
                std::str::from_utf8_unchecked(v.as_bytes())
            }) {
                return;
            }
        }
    }
}
impl crate::s3::s3_api::VRequest for Request {
    fn method(&self) -> String {
        self.request.method().as_str().to_string()
    }

    fn url_path(&self) -> String {
        self.request.uri().path().to_string()
    }

    fn get_query(&self, k: &str) -> Option<String> {
        self.query
            .as_ref()
            .and_then(|query| query.get(k))
            .map(|v| v.to_string())
    }

    fn all_query(&self, mut cb: impl FnMut(&str, &str) -> bool) {
        self.query
            .as_ref()
            .map(|query| query.iter().all(|(k, v)| cb(k, v)));
    }
}
impl crate::s3::s3_api::VRequestPlus for Request {
    fn body<'a>(
        self,
    ) -> std::pin::Pin<
        Box<dyn 'a + Send + std::future::Future<Output = Result<Vec<u8>, std::io::Error>>>,
    > {
        Box::pin(async move {
            let mut bodystream = self.request.into_body().into_data_stream();
            let mut ret = Vec::new();
            while let Some(bodystream) = bodystream.next().await {
                let bytes = bodystream.map_err(std::io::Error::other)?;
                ret.extend_from_slice(bytes.iter().as_slice());
            }
            Ok(ret)
        })
    }
}
pub struct BodyReader(body::BodyDataStream);

impl crate::utils::io::PollRead for BodyReader {
    fn poll_read<'a>(
        &'a mut self,
    ) -> std::pin::Pin<
        Box<dyn 'a + Send + std::future::Future<Output = Result<Option<Vec<u8>>, String>>>,
    > {
        Box::pin(async move {
            let data = self.0.next().await;
            match data {
                Some(ret) => match ret {
                    Ok(ret) => {
                        let vec_data = ret.to_vec();
                        // DEBUG: Log body data for debugging
                        log::debug!("HTTP-BODY-CHUNK: {} bytes", vec_data.len());
                        if vec_data.len() > 0 {
                            log::debug!(
                                "HTTP-BODY-HEX: {:?}",
                                vec_data
                                    .iter()
                                    .take(30)
                                    .map(|b| format!("{:02x}", b))
                                    .collect::<Vec<_>>()
                                    .join(" ")
                            );
                        }
                        Ok(Some(vec_data))
                    }
                    Err(err) => Err(err.to_string()),
                },
                None => {
                    log::debug!("HTTP-BODY-CHUNK: END OF STREAM");
                    Ok(None)
                }
            }
        })
    }
}

impl crate::s3::s3_api::BodyReader for Request {
    type BodyReader = BodyReader;

    fn get_body_reader<'b>(
        self,
    ) -> std::pin::Pin<
        Box<dyn 'b + Send + std::future::Future<Output = Result<Self::BodyReader, String>>>,
    > {
        Box::pin(async move {
            let ret: axum::body::Body = self.request.into_body();
            Ok(BodyReader(ret.into_data_stream()))
        })
    }
}
pub struct HeaderWarp(axum::http::HeaderMap);
impl sig_v4::VHeader for HeaderWarp {
    fn get_header(&self, key: &str) -> Option<String> {
        self.0
            .get(key)
            .and_then(|value| value.to_str().ok().map(|value| value.to_string()))
    }

    fn set_header(&mut self, key: &str, val: &str) {
        let key: axum::http::HeaderName = key.to_string().parse().unwrap();
        self.0.insert(key, val.parse().unwrap());
    }

    fn delete_header(&mut self, key: &str) {
        self.0.remove(key);
    }

    fn rng_header(&self, mut cb: impl FnMut(&str, &str) -> bool) {
        for (k, v) in self.0.iter() {
            if !cb(k.as_str(), unsafe {
                std::str::from_utf8_unchecked(v.as_bytes())
            }) {
                return;
            }
        }
    }
}

impl crate::s3::s3_api::HeaderTaker for Request {
    type Head = HeaderWarp;

    fn take_header(&self) -> Self::Head {
        HeaderWarp(self.request.headers().clone())
    }
}
pub struct Response {
    status: u16,
    headers: axum::http::HeaderMap,
    body: Vec<u8>,
}
impl Default for Response {
    fn default() -> Self {
        Self {
            status: Default::default(),
            headers: Default::default(),
            body: Default::default(),
        }
    }
}
impl From<Response> for axum::response::Response {
    fn from(val: Response) -> Self {
        let mut respbuilder = axum::response::Response::builder().status(if val.status == 0 {
            200
        } else {
            val.status
        });
        if !val.headers.is_empty() {
            if let Some(header) = respbuilder.headers_mut() {
                *header = val.headers;
            }
        }
        let raw = val.body;
        log::info!("Response body length: {}", raw.len());
        respbuilder.body(raw.into()).unwrap()
    }
}
impl sig_v4::VHeader for Response {
    fn get_header(&self, key: &str) -> Option<String> {
        self.headers
            .get(key)
            .and_then(|v| v.to_str().ok().map(|v| v.to_string()))
    }

    fn set_header(&mut self, key: &str, val: &str) {
        log::info!("set header {key} {val}");
        self.headers.insert(
            key.to_string().parse::<axum::http::HeaderName>().unwrap(),
            val.parse().unwrap(),
        );
    }

    fn delete_header(&mut self, key: &str) {
        self.headers.remove(key);
    }

    fn rng_header(&self, mut cb: impl FnMut(&str, &str) -> bool) {
        self.headers.iter().all(|(k, v)| {
            cb(k.as_str(), unsafe {
                std::str::from_utf8_unchecked(v.as_bytes())
            })
        });
    }
}
pub struct BodyWriter<'a>(&'a mut Vec<u8>);
impl<'b> crate::utils::io::PollWrite for BodyWriter<'b> {
    fn poll_write<'a>(
        &'a mut self,
        buff: &'a [u8],
    ) -> std::pin::Pin<
        Box<dyn 'a + Send + std::future::Future<Output = Result<usize, std::io::Error>>>,
    > {
        Box::pin(async move {
            // append to body buffer
            self.0.extend_from_slice(buff);
            Ok(buff.len())
        })
    }
}
impl crate::s3::s3_api::BodyWriter for Response {
    type BodyWriter<'a>
        = BodyWriter<'a>
    where
        Self: 'a;

    fn get_body_writer<'b>(
        &'b mut self,
    ) -> std::pin::Pin<
        Box<dyn 'b + Send + std::future::Future<Output = Result<Self::BodyWriter<'b>, String>>>,
    > {
        Box::pin(async move { Ok(BodyWriter(&mut self.body)) })
    }
}
impl crate::s3::s3_api::VResponse for Response {
    fn set_status(&mut self, status: u16) {
        self.status = status;
    }

    fn send_header(&mut self) {}
}
pub async fn handle_fn(
    req: axum::extract::Request<axum::body::Body>,
    _next: axum::middleware::Next,
) -> axum::response::Response {
    use crate::s3::s3_api::*;
    use axum::http::StatusCode;
    use std::sync::Arc;
    let multipart_obj = req
        .extensions()
        .get::<Arc<dyn crate::s3::s3_api::MultiUploadObjectHandler + Send + Sync>>()
        .cloned();
    match *req.method() {
        axum::http::Method::PUT => {
            let put_obj = req
                .extensions()
                .get::<std::sync::Arc<dyn crate::s3::s3_api::PutObjectHandler + Sync + Send>>()
                .cloned();
            let create_bkt_obj = req
                .extensions()
                .get::<std::sync::Arc<dyn crate::s3::s3_api::CreateBucketHandler + Sync + Send>>()
                .cloned();
            let v4head = req
                .extensions()
                .get::<crate::auth::sig_v4::V4Head>()
                .cloned();
            let path = req.uri().path();
            let rpath = path
                .trim_start_matches('/')
                .splitn(2, '/')
                .collect::<Vec<&str>>();
            let rpath_len = rpath.len();
            if rpath_len == 0 {
                log::info!("args length invalid");
                (StatusCode::BAD_REQUEST, b"").into_response()
            } else {
                let is_create_bkt = rpath_len == 1 || (rpath_len == 2 && rpath[1].is_empty());
                let req = Request::from(req);
                let mut resp = Response::default();
                if is_create_bkt {
                    //create bucket
                    match create_bkt_obj {
                        Some(create_bkt_obj) => {
                            crate::s3::s3_api::handle_create_bucket(
                                req,
                                &mut resp,
                                &create_bkt_obj,
                            )
                            .await;
                        }
                        None => {
                            log::warn!("not open create bucket method");
                            return (StatusCode::FORBIDDEN, b"").into_response();
                        }
                    }
                } else {
                    let xid = req.get_query("x-id");
                    let upload_id = req.get_query("uploadId");
                    let part_number = req.get_query("partNumber");

                    // Check if this is an UploadPart request
                    let is_upload_part = (xid.is_some()
                        && xid.as_ref().unwrap().as_str() == "UploadPart")
                        || (upload_id.is_some() && part_number.is_some());

                    if is_upload_part {
                        let mut resp = Response::default();

                        // let upload_id = req.get_query("uploadId");
                        // let part_number = req.get_query("partNumber");
                        // if upload_id.is_none() || part_number.is_none() {
                        //     return (axum::http::StatusCode::BAD_REQUEST, b"").into_response();
                        // }

                        return match multipart_obj {
                            Some(multipart_obj) => {
                                crate::s3::s3_api::handle_multipart_upload_part(
                                    req,
                                    &mut resp,
                                    &multipart_obj,
                                )
                                .await;
                                resp.into()
                            }
                            None => {
                                (axum::http::StatusCode::INTERNAL_SERVER_ERROR, b"").into_response()
                            }
                        };
                    }

                    //put object
                    match put_obj {
                        Some(put_obj) => {
                            crate::s3::s3_api::handle_put_object(
                                v4head.unwrap(),
                                req,
                                &mut resp,
                                &put_obj,
                            )
                            .await;
                        }
                        None => {
                            log::warn!("not open put object method");
                            return (StatusCode::FORBIDDEN, b"").into_response();
                        }
                    }
                }
                resp.into()
            }
        }
        axum::http::Method::GET => {
            if req.uri().path().starts_with("/probe-bsign") {
                return (axum::http::StatusCode::OK, b"").into_response();
            }
            let get_obj = req
                .extensions()
                .get::<Arc<dyn crate::s3::s3_api::GetObjectHandler + Send + Sync>>()
                .cloned();
            let listbkt_obj = req
                .extensions()
                .get::<Arc<dyn crate::s3::s3_api::ListBucketHandler + Send + Sync>>()
                .cloned();
            let listobj_obj = req
                .extensions()
                .get::<Arc<dyn crate::s3::s3_api::ListObjectHandler + Send + Sync>>()
                .cloned();
            let getbkt_loc_obj = req
                .extensions()
                .get::<Arc<dyn crate::s3::s3_api::GetBucketLocationHandler + Send + Sync>>()
                .cloned();
            let req = Request::from(req);
            let url_path = req.url_path();
            log::info!("path is {}", url_path.trim_start_matches('/').is_empty());
            if let Some(lt) = req.get_query("list-type") {
                if lt == "2" {
                    if url_path.trim_start_matches('/').is_empty() {
                        log::info!("is list buckets");
                        match listbkt_obj {
                            Some(listbkt_obj) => {
                                let mut resp = Response::default();
                                crate::s3::s3_api::handle_get_list_buckets(
                                    req,
                                    &mut resp,
                                    &listbkt_obj,
                                )
                                .await;
                                return resp.into();
                            }
                            None => {
                                log::warn!("not open list buckets method");
                                let ret = (StatusCode::FORBIDDEN, b"").into_response();
                                return ret;
                            }
                        }
                    } else {
                        log::info!("is list objects");
                        match listobj_obj {
                            Some(listobj_obj) => {
                                let mut resp = Response::default();
                                crate::s3::s3_api::handle_get_list_object(
                                    req,
                                    &mut resp,
                                    &listobj_obj,
                                )
                                .await;
                                return resp.into();
                            }
                            None => {
                                log::warn!("not open list objects method");
                                let ret = (StatusCode::FORBIDDEN, b"").into_response();
                                return ret;
                            }
                        }
                    }
                }
            } else if url_path.trim_start_matches('/').is_empty() {
                // root listing without list-type param => ListBuckets
                match listbkt_obj {
                    Some(listbkt_obj) => {
                        let mut resp = Response::default();
                        crate::s3::s3_api::handle_get_list_buckets(req, &mut resp, &listbkt_obj)
                            .await;
                        return resp.into();
                    }
                    None => {
                        let ret = (StatusCode::FORBIDDEN, b"").into_response();
                        return ret;
                    }
                }
            }

            if let Some(loc) = req.get_query("location") {
                //get bucket location
                return match getbkt_loc_obj {
                    Some(bkt) => {
                        match bkt
                            .handle(if loc.is_empty() { None } else { Some(&loc) })
                            .await
                        {
                            Ok(loc) => {
                                let lc = match loc {
                                    Some(loc) => bucket::LocationConstraint::new(loc),
                                    None => bucket::LocationConstraint::new(""),
                                };
                                match quick_xml::se::to_string(&lc) {
                                    Ok(content) => (StatusCode::OK, content).into_response(),
                                    Err(err) => {
                                        log::error!("xml encode error {err}");
                                        (StatusCode::INTERNAL_SERVER_ERROR, b"").into_response()
                                    }
                                }
                            }
                            Err(_) => {
                                log::error!("get bucket location error");
                                (StatusCode::INTERNAL_SERVER_ERROR, b"").into_response()
                            }
                        }
                    }
                    None => {
                        log::warn!("not open get bucket location method");
                        (StatusCode::FORBIDDEN, b"").into_response()
                    }
                };
            }
            //get object
            match get_obj {
                Some(obj) => {
                    let mut resp = Response::default();
                    crate::s3::s3_api::handle_get_object(req, &mut resp, &obj).await;
                    resp.into()
                }
                None => {
                    log::warn!("not open get object method");
                    (StatusCode::FORBIDDEN, b"").into_response()
                }
            }
        }
        axum::http::Method::DELETE => {
            let path = req.uri().path().trim_start_matches('/');
            if path.is_empty() {
                return (StatusCode::BAD_REQUEST, b"").into_response();
            }
            let rr = path.split("/").collect::<Vec<&str>>();
            let rr_len = rr.len();
            if rr_len == 1 || (rr_len == 2 && rr[1].is_empty()) {
                match req
                    .extensions()
                    .get::<Arc<dyn crate::s3::s3_api::DeleteBucketHandler + Send + Sync>>()
                    .cloned()
                {
                    Some(delete_bkt_obj) => {
                        let mut resp = Response::default();
                        crate::s3::s3_api::handle_delete_bucket(
                            Request::from(req),
                            &mut resp,
                            &delete_bkt_obj,
                        )
                        .await;
                        resp.into()
                    }
                    None => {
                        log::warn!("not open get delete bucket method");
                        (StatusCode::FORBIDDEN, b"").into_response()
                    }
                }
            } else {
                match req
                    .extensions()
                    .get::<Arc<dyn crate::s3::s3_api::DeleteObjectHandler + Send + Sync>>()
                    .cloned()
                {
                    Some(delete_obj_obj) => {
                        let mut resp = Response::default();
                        crate::s3::s3_api::handle_delete_object(
                            Request::from(req),
                            &mut resp,
                            &delete_obj_obj,
                        )
                        .await;
                        resp.into()
                    }
                    None => {
                        log::warn!("not open get delete bucket method");
                        (StatusCode::FORBIDDEN, b"").into_response()
                    }
                }
            }
        }
        axum::http::Method::HEAD => {
            // RE-ENABLE HEAD for AWS CLI compatibility
            // return (StatusCode::METHOD_NOT_ALLOWED, b"HEAD temporarily disabled").into_response();
            let head_obj = req
                .extensions()
                .get::<std::sync::Arc<dyn crate::s3::s3_api::HeadHandler + Sync + Send>>()
                .cloned();
            let listbkt_obj = req
                .extensions()
                .get::<std::sync::Arc<dyn crate::s3::s3_api::ListBucketHandler + Sync + Send>>()
                .cloned();
            if head_obj.is_none() {
                log::warn!("not open head features");
                return (StatusCode::INTERNAL_SERVER_ERROR, b"").into_response();
            }
            let head_obj = head_obj.unwrap();
            let req = Request::from(req);
            let raw_path = req.url_path();
            let args = raw_path
                .trim_start_matches('/')
                .splitn(2, '/')
                .collect::<Vec<&str>>();
            if args.len() == 1 {
                // HEAD bucket
                if let Some(listbkt_obj) = listbkt_obj {
                    let opt = crate::s3::ListBucketsOption {
                        bucket_region: None,
                        continuation_token: None,
                        max_buckets: None,
                        prefix: None,
                    };
                    match listbkt_obj.handle(&opt).await {
                        Ok(buckets) => {
                            let exists = buckets.iter().any(|b| b.name == args[0]);
                            if exists {
                                (StatusCode::OK, b"").into_response()
                            } else {
                                (StatusCode::NOT_FOUND, b"").into_response()
                            }
                        }
                        Err(_) => (StatusCode::INTERNAL_SERVER_ERROR, b"").into_response(),
                    }
                } else {
                    (StatusCode::FORBIDDEN, b"").into_response()
                }
            } else if args.len() != 2 {
                return (StatusCode::BAD_REQUEST, b"").into_response();
            } else {
                match head_obj.lookup(args[0], args[1]).await {
                    Ok(metadata) => match metadata {
                        Some(head) => {
                            use crate::auth::sig_v4::VHeader;
                            let mut resp = Response::default();
                            if let Some(v) = head.content_length {
                                resp.set_header("content-length", v.to_string().as_str())
                            }
                            if let Some(v) = head.etag {
                                resp.set_header("etag", &v);
                            }
                            if let Some(v) = head.content_type {
                                resp.set_header("content-type", &v);
                            }
                            if let Some(v) = head.last_modified {
                                resp.set_header("last-modified", &v);
                            }
                            // Fix for leading zeros: prevent HEAD response mixing with GET response
                            resp.set_header("Connection", "close");
                            // Keep original content-length for S3 compatibility
                            resp.set_header("X-Head-Response", "true"); // Mark as HEAD response
                            resp.set_status(200);
                            resp.send_header();
                            resp.into()
                        }
                        None => (StatusCode::NOT_FOUND, b"").into_response(),
                    },
                    Err(err) => {
                        log::error!("lookup object metadata error {err}");
                        (StatusCode::INTERNAL_SERVER_ERROR, b"").into_response()
                    }
                }
            }
        }
        axum::http::Method::POST => match multipart_obj {
            Some(multipart_obj) => {
                let mut resp = Response::default();
                let query_string = req.uri().query();
                let is_create_session = if let Some(query) = query_string {
                    let has_uploads = query.contains("uploads=") || query.contains("uploads");
                    has_uploads
                } else {
                    false
                };
                let req = Request::from(req);
                if is_create_session {
                    crate::s3::s3_api::handle_multipart_create_session(
                        req,
                        &mut resp,
                        &multipart_obj,
                    )
                    .await;
                } else if req.get_query("uploadId").is_some() {
                    crate::s3::s3_api::handle_multipart_complete_session(
                        req,
                        &mut resp,
                        &multipart_obj,
                    )
                    .await;
                } else {
                    return (StatusCode::BAD_REQUEST, b"").into_response();
                }
                resp.into()
            }
            None => {
                log::warn!("not open multipart object features");
                (StatusCode::INTERNAL_SERVER_ERROR, b"").into_response()
            }
        },
        _ => (StatusCode::METHOD_NOT_ALLOWED, b"").into_response(),
    }
}
pub async fn handle_authorization_middleware(
    req: axum::extract::Request<axum::body::Body>,
    next: axum::middleware::Next,
) -> impl axum::response::IntoResponse {
    let ret = req
        .extensions()
        .get::<std::sync::Arc<dyn crate::auth::AccesskeyStore + Send + Sync>>()
        .cloned();

    let ak_store = match ret {
        Some(ret) => ret,
        None => {
            return (axum::http::StatusCode::INTERNAL_SERVER_ERROR, b"").into_response();
        }
    };

    let req = Request::from(req);
    let base_arg = match crate::auth::sig_v4::extract_args(&req) {
        Ok(arg) => arg,
        Err(_) => {
            return (axum::http::StatusCode::BAD_REQUEST, b"").into_response();
        }
    };

    // Get raw query string for signature calculation
    let mut query = Vec::new();
    if let Some(query_str) = req.request.uri().query() {
        // Parse query string manually to preserve original encoding
        for pair in query_str.split('&') {
            if let Some(eq_pos) = pair.find('=') {
                let key = &pair[..eq_pos];
                let value = &pair[eq_pos + 1..];
                query.push(crate::utils::BaseKv {
                    key: key.to_string(),
                    val: value.to_string(),
                });
            } else {
                // Handle key without value (like "uploads")
                query.push(crate::utils::BaseKv {
                    key: pair.to_string(),
                    val: String::new(),
                });
            }
        }
    }

    let secretkey = match ak_store.get(&base_arg.access_key).await {
        Ok(secretkey) => {
            if secretkey.is_none() {
                return (axum::http::StatusCode::FORBIDDEN, b"").into_response();
            }
            secretkey.unwrap()
        }
        Err(_) => {
            return (axum::http::StatusCode::INTERNAL_SERVER_ERROR, b"").into_response();
        }
    };

    let ret = crate::auth::sig_v4::get_v4_signature(
        &req,
        req.method().as_str(),
        &base_arg.region,
        &base_arg.service,
        req.url_path().as_str(),
        &secretkey,
        &base_arg.content_hash,
        &base_arg.signed_headers,
        query,
    );

    let circle_hasher = match ret {
        Ok((sig, circle_hasher)) => {
            if sig != base_arg.signature {
                return (axum::http::StatusCode::FORBIDDEN, b"").into_response();
            }
            circle_hasher
        }
        Err(_) => {
            return (axum::http::StatusCode::FORBIDDEN, b"").into_response();
        }
    };

    let v4head = sig_v4::V4Head::new(
        base_arg.signature,
        base_arg.region,
        base_arg.access_key,
        circle_hasher,
    );

    let mut req: axum::http::Request<axum::body::Body> = req.into();
    req.extensions_mut().insert(v4head);
    next.run(req).await
}
mod bucket {

    #[derive(serde::Serialize, Debug)]
    #[serde(rename = "LocationConstraint", rename_all = "PascalCase")]
    pub struct LocationConstraint {
        #[serde(rename = "$value")]
        region: String,

        #[serde(rename = "xmlns")]
        _xmlns: &'static str,
    }
    impl LocationConstraint {
        pub fn new<T: Into<String>>(region: T) -> Self {
            Self {
                region: region.into(),
                _xmlns: "http://s3.amazonaws.com/doc/2006-03-01/",
            }
        }
    }
}
