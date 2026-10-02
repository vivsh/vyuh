use super::{Service, ServiceError, ServiceInstance};
use crate::{Site, SiteConf, SiteError, bundles, callables::CallError};

struct Unavailable;

impl Service for Unavailable {}

/// The command-aware entrypoint preserves factory errors before starting the serve command.
#[tokio::test]
async fn run_propagates_factory_error() {
    let factory = || async {
        Err::<ServiceInstance<Unavailable>, _>(ServiceError::CallError(CallError::InvalidArgument(
            "invalid service configuration".into(),
        )))
    };
    let conf = SiteConf {
        log_init: false,
        ..SiteConf::default()
    };
    let result = Site::run_with_args(
        conf,
        bundles::bundle([bundles::service(factory)]),
        ["serve".to_owned()],
    )
    .await;
    assert!(matches!(
        result,
        Err(SiteError::ServiceError(ServiceError::CallError(
            CallError::InvalidArgument(_)
        )))
    ));
}
