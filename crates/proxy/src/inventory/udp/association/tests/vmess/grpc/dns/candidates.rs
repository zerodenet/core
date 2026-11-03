use super::*;
use std::{
    future::{poll_fn, Future},
    task::Poll,
};
#[tokio::test]
async fn ordinary_vmess_grpc_domain_multiple_addresses_close_failed_original_before_next_tls_udp() {
    timeout(Duration::from_secs(15),async {
        let mut dns=Dns::new().await;let endpoint=Endpoint::new().await;
        let (proxy,running,_)=start_domain(endpoint.config(&dns,false)).await;
        let server=endpoint.serve(false,1,vec![b"owned".to_vec()]);
        let opening=open(proxy.clone(),1,b"owned");tokio::pin!(opening);
        let request=tokio::select! {biased;_= &mut opening=>panic!("DNS must still be pending"),request=dns.request()=>request};
        let query=pending(&proxy).await;
        request.answer_ips(false,false,&[zero_traits::IpAddress::V4([127,0,0,2]),zero_traits::IpAddress::V4([127,0,0,1])]);
        query.task.wait().await;dns_closed(&query);
        // First poll retains the first actual candidate controller.
        // It may already report connection refusal; that is not a close receipt.
        poll_fn(|cx| {assert!(opening.as_mut().poll(cx).is_pending());Poll::Ready(())}).await;
        let registration=original(&proxy);let observed=registration.observe().unwrap();
        assert_eq!(observed.slots().len(),1);
        let first=&observed.slots()[0];
        assert!(first.native().is_some(), "retain the actual first native controller");
        let prior=first.original().clone();
        let dispatch=opening.await;
        assert!(prior.matches_closed(&prior.closed().expect("actual failed candidate original close")));
        let current=registration.observe().unwrap();assert_eq!(current.slots().len(),1);
        assert!(current.slots()[0].ordinal()>first.ordinal());
        let native=current.slots()[0].original().clone();
        retire(&proxy,dispatch).await;
        assert!(native.matches_closed(&native.closed().unwrap()));
        server.await.unwrap();running.shutdown().await.unwrap();dns.close().await;
    }).await.unwrap();
}
