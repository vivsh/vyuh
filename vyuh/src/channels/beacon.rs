//! Declarative authenticated signal subscriptions exposed as HTTP routes.

use std::{
    any::{Any, TypeId},
    sync::Arc,
    time::Duration,
};

use crate::{
    Error, OperationId, Site,
    auth::{AuthError, AuthUser, Scope},
    callables::DataValue,
    routes::Subscriber,
};

use super::types::{DeliverySpec, SignalDecoder};
use super::{ALL_TRANSPORTS, ChannelResponse, ChannelTransport, UserKey};

type BeaconPredicate = Arc<dyn Fn(&AuthUser, &dyn Any) -> bool + Send + Sync>;

/// Configuration for one Beacon HTTP endpoint.
#[derive(Clone, Debug)]
pub struct BeaconConf {
    /// Logical route name used for reversal and OpenAPI.
    pub name: &'static str,
    /// Route path before bundle-prefix composition.
    pub path: &'static str,
    /// Transports the endpoint accepts.
    pub modes: ChannelTransport,
    /// Whether an alternate trailing slash is normalized before dispatch.
    pub trim: bool,
}

impl BeaconConf {
    /// Creates a Beacon endpoint with every channel transport enabled.
    pub const fn new(name: &'static str, path: &'static str) -> Self {
        Self {
            name,
            path,
            modes: ALL_TRANSPORTS,
            trim: true,
        }
    }

    /// Restricts the endpoint to a channel transport bitmask.
    pub const fn modes(mut self, modes: ChannelTransport) -> Self {
        self.modes = modes;
        self
    }

    /// Rejects the alternate trailing-slash form when disabled.
    pub const fn trim(mut self, enabled: bool) -> Self {
        self.trim = enabled;
        self
    }
}

/// Validation failure while assembling one [`Beacon`].
#[derive(Debug, Clone, thiserror::Error)]
pub enum BeaconError {
    /// A signal type was registered more than once.
    #[error("Beacon already has a rule for signal type '{0}'")]
    DuplicateRule(String),
    /// A debounce target has no matching rule.
    #[error("Beacon has no rule for debounced signal type '{0}'")]
    UnknownDebounce(String),
    /// A rule was configured with debounce more than once.
    #[error("Beacon already configures debounce for signal type '{0}'")]
    DuplicateDebounce(String),
    /// A debounce duration cannot schedule a delivery.
    #[error("Beacon debounce duration must be greater than zero")]
    ZeroDebounce,
    /// A route cannot negotiate an empty transport mask.
    #[error("Beacon must allow at least one channel transport")]
    EmptyModes,
}

/// Typed signal subscription rules for one authenticated live endpoint.
#[derive(Clone)]
pub struct Beacon {
    rules: Vec<BeaconRule>,
    errors: Vec<BeaconError>,
}

#[derive(Clone)]
struct BeaconRule {
    type_id: TypeId,
    signal_key: super::fanout::SignalKey,
    scopes: Arc<[Scope]>,
    predicate: BeaconPredicate,
    decoder: SignalDecoder,
    debounce: Option<Duration>,
}

/// Builder for [`Beacon`] rules.
pub struct BeaconBuilder {
    beacon: Beacon,
}

impl Beacon {
    /// Starts a declarative signal-subscription builder.
    pub fn builder() -> BeaconBuilder {
        BeaconBuilder {
            beacon: Self {
                rules: Vec::new(),
                errors: Vec::new(),
            },
        }
    }

    /// Validates static Beacon configuration before route registration.
    pub fn validate(&self, conf: &BeaconConf) -> Result<(), BeaconError> {
        if conf.modes & ALL_TRANSPORTS == 0 {
            return Err(BeaconError::EmptyModes);
        }
        self.errors.first().cloned().map_or(Ok(()), Err)
    }

    /// Opens the endpoint-scoped channel subscription for one authenticated user.
    pub(crate) async fn open(
        &self,
        site: Site,
        operation: OperationId,
        user: AuthUser,
        subscriber: Subscriber,
        modes: ChannelTransport,
    ) -> Result<ChannelResponse, Error> {
        let user_key = UserKey::new(user.subject()).map_err(Error::from)?;
        let channel = site
            .channels()
            .beacon_channel(operation)
            .ok_or_else(|| Error::from(super::ChannelError::SubscriptionUnavailable))?;
        let mut stream = site.channels().user(user_key).channel(channel);
        let mut rules = 0usize;
        for rule in &self.rules {
            if !user.has_all(&rule.scopes) {
                continue;
            }
            stream = stream.deliver_spec(rule.delivery(user.clone()));
            rules = rules.saturating_add(1);
        }
        if rules == 0 {
            return Err(Error::from(AuthError::Forbidden));
        }
        subscriber.attach(stream).allow(modes).await
    }
}

impl BeaconRule {
    fn delivery(&self, user: AuthUser) -> DeliverySpec {
        let predicate = Arc::clone(&self.predicate);
        let delivery = Arc::new(move |value: &dyn Any| predicate(&user, value));
        DeliverySpec {
            type_id: self.type_id,
            signal_key: self.signal_key.clone(),
            predicate: delivery,
            decoder: self.decoder,
            debounce: self.debounce,
        }
    }
}

impl BeaconBuilder {
    /// Delivers every signal of type `T` to identities holding all supplied scopes.
    pub fn rule<T>(mut self, scopes: impl IntoIterator<Item = &'static str>) -> Self
    where
        T: DataValue,
    {
        self.add_rule::<T, _>(scopes, |_, _| true);
        self
    }

    /// Delivers `T` only when scopes and the additional typed predicate both allow it.
    pub fn rule_with<T>(
        mut self,
        scopes: impl IntoIterator<Item = &'static str>,
        predicate: impl Fn(&AuthUser, &T) -> bool + Send + Sync + 'static,
    ) -> Self
    where
        T: DataValue,
    {
        self.add_rule::<T, _>(scopes, predicate);
        self
    }

    /// Configures trailing-edge debounce for the declared rule of type `T`.
    pub fn debounce<T>(mut self, window: Duration) -> Self
    where
        T: DataValue,
    {
        if window.is_zero() {
            self.beacon.errors.push(BeaconError::ZeroDebounce);
            return self;
        }
        let name = std::any::type_name::<T>();
        let Some(rule) = self
            .beacon
            .rules
            .iter_mut()
            .find(|rule| rule.type_id == TypeId::of::<T>())
        else {
            self.beacon
                .errors
                .push(BeaconError::UnknownDebounce(name.to_string()));
            return self;
        };
        if rule.debounce.is_some() {
            self.beacon
                .errors
                .push(BeaconError::DuplicateDebounce(name.to_string()));
        } else {
            rule.debounce = Some(window);
        }
        self
    }

    /// Completes the accumulated declarative subscription configuration.
    pub fn build(self) -> Beacon {
        self.beacon
    }

    fn add_rule<T, F>(&mut self, scopes: impl IntoIterator<Item = &'static str>, predicate: F)
    where
        T: DataValue,
        F: Fn(&AuthUser, &T) -> bool + Send + Sync + 'static,
    {
        let type_id = TypeId::of::<T>();
        let type_name = std::any::type_name::<T>();
        if self.beacon.rules.iter().any(|rule| rule.type_id == type_id) {
            self.beacon
                .errors
                .push(BeaconError::DuplicateRule(type_name.to_string()));
            return;
        }
        let predicate = Arc::new(move |user: &AuthUser, value: &dyn Any| {
            value
                .downcast_ref::<T>()
                .is_some_and(|value| predicate(user, value))
        });
        self.beacon.rules.push(BeaconRule {
            type_id,
            signal_key: super::fanout::SignalKey::from_type_name(type_name),
            scopes: scopes.into_iter().map(Scope::of).collect(),
            predicate,
            decoder: super::decode_signal::<T>,
            debounce: None,
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, serde::Deserialize, serde::Serialize, schemars::JsonSchema)]
    struct NoteChanged {
        owner: String,
    }

    /// Verifies Beacon construction reports duplicate rules and invalid debounce declarations.
    #[test]
    fn builder_accumulates_rule_and_debounce_errors() {
        let duplicate = Beacon::builder()
            .rule::<NoteChanged>(["notes:read"])
            .rule::<NoteChanged>(["notes:read"])
            .build();
        assert!(matches!(
            duplicate.validate(&BeaconConf::new("live", "/live")),
            Err(BeaconError::DuplicateRule(_))
        ));

        let invalid = Beacon::builder()
            .debounce::<NoteChanged>(Duration::from_millis(1))
            .build();
        assert!(matches!(
            invalid.validate(&BeaconConf::new("live", "/live")),
            Err(BeaconError::UnknownDebounce(_))
        ));
    }

    /// Verifies rules require every scope and invoke their typed predicate before delivery.
    #[test]
    fn rules_apply_all_scopes_and_typed_predicates() {
        let beacon = Beacon::builder()
            .rule_with::<NoteChanged>(["notes:read", "notes:live"], |user, event| {
                event.owner == user.subject()
            })
            .build();
        let user = AuthUser::new("42").with_scope(Scope::of("notes:read"));
        let rule = beacon.rules.first();
        assert!(rule.is_some_and(|rule| !user.has_all(&rule.scopes)));

        let user = user.with_scope(Scope::of("notes:live"));
        let delivery = beacon.rules[0].delivery(user);
        let own = NoteChanged { owner: "42".into() };
        let other = NoteChanged { owner: "7".into() };
        assert!((delivery.predicate)(&own));
        assert!(!(delivery.predicate)(&other));
    }

    /// Verifies an empty transport mask is rejected before bundle route registration.
    #[test]
    fn validation_rejects_empty_transport_modes() {
        let beacon = Beacon::builder().rule::<NoteChanged>([]).build();
        assert!(matches!(
            beacon.validate(&BeaconConf::new("live", "/live").modes(0)),
            Err(BeaconError::EmptyModes)
        ));
    }
}
