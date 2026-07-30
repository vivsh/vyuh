# Authentication

Vyuh authentication is built around complete credential providers. A provider owns how an
access credential is authenticated, whether refresh is supported, how both
credentials are encoded and delivered, and which application checks run after
cryptographic verification. Handlers see the same `AuthUser` regardless of the
provider or token format.

Identity proof is a separate, composable layer. Named login methods support
password and HTTP Basic token exchange, password plus MFA, and OIDC
Authorization Code with PKCE. Applications still own user rows, password and
factor storage, account linking, and every authentication route.

## Audiences and identity

An `Audience` names an API surface. Declare it once and use the same descriptor
when issuing credentials and composing protected bundles:

```rust
use vyuh::auth::{Audience, AuthUser};
use vyuh::prelude::*;

const API: Audience = Audience::new("api");
const REPORTS: Audience = Audience::new("reports");

async fn me(user: AuthUser) -> Json<String> {
    Json(user.key.to_string())
}

let api = bundles::bundle! { me }
    .with_audience(API);
```

Audience names are validated during site construction and login. Duplicate
audiences are removed while preserving order. An audience controls where a
credential is accepted; roles control what the identity may do within that
surface.

Vyuh safely maps omitted audiences to `DEFAULT_AUDIENCE` (`"default"`). This
keeps existing Django applications concise without making an audience-less
token unrestricted:

```rust
// Both use the configured default audience.
site.auth().login(user, &[]).await?;
let routes = bundles::bundle! { me };

// Applications can choose a more meaningful default.
let auth = AuthConf::default().default_audience(API);
```

A legacy authenticated token with no `aud` claim owns only that default. An
explicit `aud: []` is invalid, and the default is never added to explicitly
listed audiences. Applications that require every route and call site to name
an audience use `AuthConf::require_explicit_audiences()`; site construction then
rejects authenticated routes without `.with_audience(...)`, and empty login or
refresh slices fail.

Construct identities with role builders:

```rust
let user = AuthUser::new("user-123")
    .with_role(UserRole::User);

let editor = AuthUser::new("user-456")
    .with_roles([UserRole::User, UserRole::Editor]);

let restored = AuthUser::new("user-789")
    .with_role_mask(database_role_mask);
```

`AuthUser` exposes its stable `key`, role mask, and accepting provider. A token
or key verifier can attach request-only data with `with_extra`; handlers recover
it with `extra::<T>()`. Extras are not serialized into tokens and are redacted
from diagnostics. Use `permit!` for static role gates.

### Authentication assurance

`AuthenticationContext` describes how and when the current identity was
proven. It is not a separate route extractor. A protected handler obtains it
from its extracted `AuthUser`:

```rust
async fn assurance(user: AuthUser) -> Json<bool> {
    let authentication = user.authentication();
    Json(authentication.has_method("totp"))
}
```

The context exposes:

- `auth_time() -> Option<DateTime<Utc>>`, the time identity proof completed;
- `methods()`, authenticated method-reference names such as `password`, `oidc`,
  or `totp`;
- `has_method(name)`, a convenient method-reference check;
- `acr()`, an optional authentication-context class asserted by the login
  method or external identity provider.

`auth_time` is optional because opaque keys and externally decoded credentials
may not provide a trustworthy proof time. Credentials created through Vyuh
login methods carry their authenticated assurance through access-token issuance
and refresh. Assurance describes identity proof; audiences and roles remain the
authorization controls.

## Login methods

`AuthProvider` answers *which credential system issues the result*.
`LoginMethod` answers *how identity is proven*. Select them consistently:

```rust
const PASSWORD: LoginMethod<PasswordCredentials> =
    LoginMethod::new("password");

let auth = AuthConf::default().method(
    PASSWORD,
    PasswordLogin::new(AccountPasswords),
);

site.auth()
    .via(PASSWORD)
    .login(credentials, &[API])
    .await?;
```

For a non-default token provider, compose both selectors:

```rust
site.auth()
    .using(APP_AUTH)
    .via(PASSWORD)
    .login(credentials, &[API])
    .await?;
```

`using` and `via` are infallible selectors. They retain typed descriptors;
provider names, registrations, method types, and capabilities are checked by
the terminal `login`, `begin`, `complete`, `refresh`, or `logout` operation.
Configuration itself is validated by `Site::build`.

One-step methods expose `login`. Inherently multi-step methods expose only
`begin` and `complete`; the type system prevents completing password or Basic
login and prevents using one-step `login` on OIDC or MFA. Login methods are
registered centrally through `AuthConf::method`; route bundles do not repeat
that registration as runtime or documentation metadata.

### Password and Basic exchange

Applications implement `PasswordVerifier`, returning an `AuthUser` only after
safe account lookup and password verification. Both body credentials and Basic
exchange can share that verifier:

```rust
let auth = AuthConf::default()
    .method(PASSWORD, PasswordLogin::new(AccountPasswords))
    .method(BASIC, BasicLogin::new(AccountPasswords));
```

`BasicCredentials` extracts `Authorization: Basic`, but only on the login route
that requests it. Basic is exchanged for the normal access/refresh pair and is
never accepted by protected `AuthUser` handlers.

### Password plus MFA

Compose the primary verifier with application-owned factor verification:

```rust
const PASSWORD_MFA: LoginMethod<PasswordCredentials, MfaResponse> =
    LoginMethod::new("password-mfa");

let method = PasswordLogin::new(AccountPasswords).then(
    MfaLogin::new(AccountFactors).totp().recovery_codes(),
);
```

The first route calls `begin(credentials, &[API])` and returns
`LoginChallenge`. The completion route calls
`complete(MfaResponse::totp(challenge, code))`. No credential is issued before
the factor succeeds. An optional `AuthConf::login_state_store` atomically
consumes continuation IDs for strict replay protection.

### OIDC

Enable the `oidc` feature and register one discovery-backed method:

```rust
let google = OidcLogin::discovery("https://accounts.google.com")
    .client_id("client-id")
    .client_secret(KeySource::env("GOOGLE_CLIENT_SECRET"))
    .redirect_uri("https://example.com/auth/google/callback")
    .scopes(["email", "profile"])
    .mapper(GoogleAccountMapper);
```

The start route calls `via(GOOGLE)?.begin(OidcStart::new(), &[API])`; the
callback calls `via(GOOGLE)?.complete(callback)`. Vyuh validates discovery,
Authorization Code, PKCE-S256, state, nonce, ID-token signature, issuer,
audience, expiry, and access-token hash before invoking `OidcUserMapper`.

MFA and OIDC continuation state is AES-256-GCM sealed with a domain-separated
site-secret key. New state uses the active secret and in-flight state accepts
configured fallback secrets during rotation. The state binds login method,
credential provider, audiences, and expiry.

## Default JWT login

`AuthConf::default()` registers one bearer JWT provider using HS256 and
`SiteConf.secret_key`. It issues a one-hour access token and a seven-day refresh
token:

```rust
use vyuh::auth::{AuthConf, AuthUser, LoginResponse};
use vyuh::prelude::*;

async fn login(site: Site) -> Result<LoginResponse, Error> {
    let user = AuthUser::new("user-123");
    Ok(site.auth().login(user, &[API]).await?)
}

let conf = SiteConf::default()
    .secret_key("replace-with-at-least-32-random-characters")
    .auth(AuthConf::default());
```

`login`, rather than a framework login route, remains the credential-creation
API. Applications can call it directly with an already verified `AuthUser`, or
select a configured identity-proof method through `.via(...)`.

For multiple API surfaces, pass one slice:

```rust
site.auth().login(user, &[API, REPORTS]).await?;
```

There is no `issue_many` operation and no separate refresh provider.
`TokenKind::Access` and `TokenKind::Refresh` distinguish the two credentials
inside one provider.

## Refresh

Vyuh supplies refresh behavior, while the application chooses and registers the
route:

```rust
use axum::extract::Request;

async fn refresh(site: Site, request: Request) -> Result<LoginResponse, Error> {
    let (parts, _) = request.into_parts();
    Ok(site.auth().refresh(&parts, &[API]).await?)
}
```

Without `using`, refresh always selects `DEFAULT_AUTH_PROVIDER`. A route for a
different complete provider selects it explicitly:

```rust
site.auth()
    .using(APP_AUTH)
    .refresh(&parts, &[API])
    .await?;
```

Refresh extracts only the selected provider's refresh credential, verifies its format and
`TokenKind`, validates time, provider, binding, and lifecycle state, then runs
the provider's `TokenVerifier`. Requested audiences must already be present in
the refresh token, so a refresh may preserve or narrow authority but cannot add
it. The result contains a new access token and a rotated refresh token with the
same family identifier.

Stateless rotation is the default: an old refresh token remains valid until it
expires. Add a `TokenLifecycle` to atomically consume token IDs, detect replay,
and revoke a refresh family when the application needs stateful rotation.

## `LoginResponse`

`LoginResponse` implements `IntoResponse` and keeps body data separate from
credential delivery:

```rust
let login = site.auth().login(user, &[API]).await?;

let access = login.credentials().access();
let refresh = login.credentials().refresh();

let login = login.data(serde_json::json!({ "user": "user-123" }));
assert_eq!(login.data_ref()["user"], "user-123");
login.write(&mut response);
```

For body-delivered tokens, the default JSON contains `access_token`, optional
`refresh_token`, `token_type`, and `expires_in`. Cookie-delivered values are
omitted. When every credential is delivered by cookie, the body is `{ "ok":
true }`. Replacing the body with `.data(value)` does not discard cookies or
response headers.

`Credentials` deliberately exposes values only through `access()` and
`refresh()`. It does not implement `Debug`, `Display`, or `Serialize`.

## Complete token providers

Use `AuthConf::empty()` when no implicit provider is wanted. An `AuthProvider`
is a stable name for one complete configured authentication system:

```rust
use chrono::Duration;
use vyuh::auth::{
    AuthConf, AuthProvider, CookieConf, Jwt, TokenConf, TokenProvider,
};

const APP_AUTH: AuthProvider = AuthProvider::new("app-auth");

let auth = AuthConf::empty().provider(
    APP_AUTH,
    TokenProvider::new(Jwt::hs256_site_secret())
        .access(TokenConf::bearer().ttl(Duration::minutes(15)))
        .refresh(
            TokenConf::cookie(CookieConf::new("refresh_token"))
                .ttl(Duration::days(30)),
        ),
);
```

Select it for login or refresh without changing the operation shape:

```rust
site.auth()
    .using(APP_AUTH)
    .login(user, &[API])
    .await?;

site.auth()
    .using(APP_AUTH)
    .refresh(&parts, &[API])
    .await?;
```

The unselected `login`, `refresh`, and `logout` conveniences all mean the
default provider. Refresh and logout never probe or mutate another provider;
multi-provider routes must select their provider with `using`.

Call `.without_refresh()` for access-only providers. Access and refresh may use
different formats while remaining one provider:

```rust
use vyuh::auth::DjangoSigning;

let provider = TokenProvider::new(Jwt::hs256_site_secret())
    .access(TokenConf::bearer().ttl(Duration::minutes(15)))
    .refresh(
        TokenConf::cookie("refresh_token")
            .ttl(Duration::days(30))
            .codec(DjangoSigning::site_secret()),
    );
```

`TokenConf` owns location, delivery, lifetime, optional codec override, CSRF
policy, and a bounded pre-decode credential-size limit.

Construct only valid source/delivery combinations directly:

```rust
TokenConf::bearer();
TokenConf::header("x-auth-token");
TokenConf::header_with_scheme("authorization", "Token");
TokenConf::cookie(CookieConf::new("auth_token"));
TokenConf::query("token", UnsafeQueryCredentials::allow());

// Extraction and response delivery are independent when requested explicitly.
TokenConf::header("x-auth-token")
    .response_header("x-new-auth-token");
```

Bearer, custom-header, and query sources return new credentials in the response
body by default. Cookie sources write the same cookie. Query credentials are
never emitted into URLs. Duplicate matching headers, cookies, or query values
are rejected instead of choosing one.

## Token formats

Every parseable format authenticates the same private `AuthToken` envelope:
provider, `TokenKind`, subject, roles, audiences, timestamps, token and family
IDs, optional issuer and binding, and optional authenticated payload data.
Normal handlers never extract `AuthToken`; they extract `AuthUser`.

Built-in codecs are:

- `Jwt`: HS256 from the site secret or explicit asymmetric RS256 keys, exact
  algorithm pinning, duplicate-claim rejection, and key-ID rotation.
- `DjangoSigning`: Django 5.2 `TimestampSigner` and
  `django.core.signing.dumps/loads` compatible salted HMAC, timestamps,
  compressed payloads, and fallback secrets. This does not mean SimpleJWT or
  Django session-storage compatibility.
- `Paseto`: PASETO v4.public and v4.local behind the `paseto` feature.
- `Branca`: authenticated encrypted BRANCA tokens behind the `branca` feature.

For example:

```rust
use vyuh::auth::{DjangoSigning, KeySource, Jwt, TokenProvider};

let django = TokenProvider::new(DjangoSigning::site_secret());

let asymmetric = TokenProvider::new(
    Jwt::rs256(
        KeySource::file("keys/jwt-private.pem"),
        KeySource::file("keys/jwt-public.pem"),
    )
    .key_id("2026-07")
    .verification_key(
        "2026-04",
        KeySource::file("keys/jwt-public-2026-04.pem"),
    ),
);
```

`KeySource` is opaque and is constructed only through `site_secret`, `file`,
`env`, or `inline`. Key material and source values remain redacted from
diagnostics and are resolved during site construction.

Issuer, temporal, audience, binding, and lifecycle policy belongs to
`TokenProvider` and runs exactly once after format authentication. For example,
`.issuer("https://api.example.com")` and `.leeway(Duration::seconds(30))` apply
equally to JWT, PASETO, BRANCA, Django signing, and custom codecs.

`TokenProvider::custom(codec, format)` accepts an application codec that
implements both `TokenEncoder` and `TokenDecoder`. `verify_only(decoder,
format)` integrates externally issued self-contained tokens; login and refresh
then fail with `UnsupportedProviderCapability`. Framework validation always
runs after decoding and cannot be bypassed by a `TokenVerifier`.

An external decoder constructs the normalized value without invented claims:

```rust
let token = AuthToken::builder(
    PARTNER_AUTH,
    TokenKind::Access,
    claims.subject,
    claims.issued_at,
    claims.expires_at,
)
.roles(claims.roles)
.audiences(claims.audiences)
.issuer(claims.issuer)
.token_id(claims.token_id)
.authentication(claims.auth_time, claims.amr, claims.acr)
.payload(claims.application_data)
.build()?;
```

Provider, kind, subject, issuance, and expiry are mandatory. Audience omission
is preserved for default-audience compatibility, while an explicitly empty
audience is rejected. Runtime extras belong on `AuthUser`, never `AuthToken`.

## Secret rotation and Django compatibility

The simple path remains one Django-style site secret:

```rust
let conf = SiteConf::default().secret_key(current_secret);
```

During rotation, keep previous values as verify-only fallbacks:

```rust
let conf = SiteConf::default()
    .secret_key(current_secret)
    .secret_key_fallbacks([previous_secret, older_secret]);
```

The active secret creates new credentials. Up to seven fallback values are
tried only during verification. JWT HMAC and Django signing use the ring
directly; PASETO local and BRANCA derive fixed-size, domain-separated keys with
HKDF-SHA256. Production validation rejects the framework development secret,
weak secrets, duplicate fallbacks, and oversized key rings.

`SECRET_KEY_FALLBACKS` accepts either a JSON string array or a comma-separated
environment value, matching the simple deployment style of `SECRET_KEY`.

Formats with native key IDs use one active signing key and named retired
verification keys. Unknown or duplicate IDs fail; rotation never enables a
second algorithm.

## Application verification and lifecycle

A `TokenVerifier` runs only after extraction, size limits, cryptographic
authentication, provider and kind checks, temporal validation, requested
audiences, CSRF, and binding. It may reject a user, replace stale roles, or add
runtime extras:

```rust
use vyuh::auth::{AuthError, AuthToken, AuthUser, TokenVerifier};

struct ActiveAccount;

impl TokenVerifier for ActiveAccount {
    async fn verify(&self, token: &AuthToken) -> Result<AuthUser, AuthError> {
        let account = load_account(token.subject()).await?;
        if !account.active {
            return Err(AuthError::InvalidCredential);
        }
        Ok(AuthUser::new(account.id.to_string())
            .with_role_mask(account.roles)
            .with_extra(account))
    }
}
```

Use `TokenLifecycle` for refresh replay protection, token-ID revocation,
security-version invalidation, or family logout. Lifecycle rotation completes
before a refreshed `LoginResponse` is returned.

## Opaque API keys

`AuthKey` is separate from parseable `AuthToken` formats. Its verifier owns the
lookup, expiry, revocation, tenant checks, and role resolution:

```rust
use vyuh::auth::{
    AuthError, AuthKey, AuthProvider, AuthUser, KeyRequest, KeyVerifier,
};

const API_KEY: AuthProvider = AuthProvider::new("api-key");

struct ApiKeyVerifier;

impl KeyVerifier for ApiKeyVerifier {
    async fn verify(
        &self,
        credential: &vyuh::auth::PresentedCredential<'_>,
        request: KeyRequest<'_>,
    ) -> Result<AuthUser, AuthError> {
        let record = load_api_key(credential.expose(), request.audience()).await?;
        Ok(AuthUser::new(record.subject)
            .with_role_mask(record.roles)
            .with_extra(record))
    }
}

let auth = AuthConf::default().provider(
    API_KEY,
    AuthKey::header("x-api-key", ApiKeyVerifier),
);
```

Headers and cookies are supported. Query extraction requires an explicit
`UnsafeQueryCredentials::allow()` acknowledgement because URLs leak into logs,
history, referrers, and monitoring systems.

## Cookies, CSRF, and logout

Token cookies are opt-in and are not server-side sessions. `CookieConf`
defaults to `Secure`, `HttpOnly`, `SameSite=Lax`, and path `/`. A cookie
credential automatically gets a readable double-submit CSRF cookie; unsafe
requests must copy its value into `X-CSRF-Token`. The policy can be renamed with
`CsrfConf`. Explicitly disabling it is rejected in production.

Logout is another authenticator helper owned by an application route:

```rust
use axum::{extract::Request, response::Response};

async fn logout(site: Site, request: Request) -> Result<Response, Error> {
    let (parts, _) = request.into_parts();
    let mut response = Json(serde_json::json!({ "ok": true })).into_response();
    let logout = site.auth().logout(&parts).await?;
    logout.write(&mut response);
    Ok(response)
}
```

For the default `{ "ok": true }` body, return `LogoutResponse` directly:

```rust
async fn logout(site: Site, request: Request) -> Result<LogoutResponse, Error> {
    let (parts, _) = request.into_parts();
    Ok(site.auth().logout(&parts).await?)
}
```

Cookie providers clear access, refresh, and CSRF cookies. Stateless bearer
providers need no server mutation. A token or key lifecycle can revoke the
presented credential or refresh family. Logout is therefore provider-managed
sign-out: without a lifecycle it clears applicable client state but does not
claim server-side revocation.

## Security and operations

Vyuh bounds credential input before decoding, pins algorithms, binds locally
issued tokens to their provider, validates expiry and `not_before`, rejects
audience escalation, short-circuits malformed credentials, and renders generic
`401`, `403`, `500`, and provider-unavailable `503` bodies. Raw keys, tokens,
bindings, and key material are redacted from formatting and serialization.

Prometheus output includes `vyuh_auth_attempts_total` with configured provider
names, one fixed `<unknown>` selector bucket, and safe outcome classes, plus
`vyuh_login_attempts_total` with the same bounded method-label policy. OpenAPI
emits one security alternative per provider, `x-vyuh-audience`, token
`bearerFormat`, API-key locations, and CSRF-header metadata for unsafe
cookie-authenticated operations.

Rate limiting remains middleware policy. Treat password login, refresh, and
expensive API-key verification endpoints as explicit rate-limit boundaries.

## Future session providers

Traditional server-side sessions are not implemented. The internal provider
runtime already has asynchronous authenticate, login, refresh, logout, response
attachment, and capability boundaries, so a future stateful provider can fit
the same handler and `Authenticator` APIs without introducing session-specific
routes or extractors.

Vyuh's Django-compatible `make_password`, `check_password`, and
`unusable_password` helpers remain available independently of token format.

## Migration from the halted auth redesign

The earlier uncommitted API has no compatibility aliases. Update applications
as follows:

- register identity proof with `AuthConf::method`, reserving `login` for the
  runtime operation;
- configure the default credential provider through
  `AuthConf::empty().provider(DEFAULT_AUTH_PROVIDER, provider)` instead of
  default-provider convenience fields;
- construct sources with `TokenConf::{bearer,header,cookie,query}` or the
  parallel `AuthKey` constructors instead of `CredentialLocation`;
- compose password or Basic proof with MFA through `.then(...)`;
- construct externally decoded values through `AuthToken::builder(...)`;
- call infallible `LoginResponse::write`; obtain assurance through
  `AuthUser::authentication()` and handle its `auth_time()` as optional;
- remove `?` after `using(...)` and `via(...)`; selection errors are returned
  by the following terminal operation;
- replace `logout(parts, response).await?` with
  `let logout = logout(parts).await?; logout.write(response);`.

Access and refresh remain `TokenKind` values inside one provider, and all
`login` and `refresh` calls continue to take `&[Audience]`.
