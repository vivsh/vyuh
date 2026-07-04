# Auth

Vyuh auth is opt-in and handler-signature driven. If a route does not extract
`AuthUser`, `permit!(...)`, or `ApiKey`, Vyuh does no authentication work for
that route.

Vyuh does not provide user models, registration, password reset, API-key tables,
session tables, or account management flows. Applications own identity storage
and decide how users or API keys are verified.

Vyuh auth deliberately stops at verified principals. Applications own user
records, account lifecycle, and domain permissions.

## Mental Model

| Need | Use |
| --- | --- |
| Public route | no auth type |
| Authenticated JWT user | `AuthUser` |
| Static role mask | `permit!(Role, Admin)` |
| Dynamic permission | handler or service logic |
| Machine-to-machine auth | `ApiKey` |
| Issue or refresh JWTs | `site.auth()` |
| Django password hashes | `make_password`, `check_password` |

Roles are optional. The `permit!` macro is a static role-mask convenience, not a
general authorization framework. Omit roles entirely when an application does
not need them.

Console roles live under `vyuh::console` and are separate from application
roles. They only control access to Vyuh's optional read-only console APIs.

## Configuration

Auth configuration lives on `SiteConf`. Cookies are disabled by default:

```rust
use vyuh::prelude::*;
use vyuh::auth::{AuthConf, CookieConf};

let conf = SiteConf::default()
    .secret_key("replace-with-a-long-random-secret")
    .auth(
        AuthConf::default()
            .access_ttl(3600)
            .refresh_ttl(604800)
            .access_cookie(CookieConf::new("access_token"))
            .refresh_cookie(CookieConf::new("refresh_token")),
    );
```

By default, JWTs are signed with `HS256` using `SiteConf.secret_key`.
`SiteConf::validate()` checks the configured minimum length for HMAC signing
keys.

Useful `AuthConf` options:

- `access_ttl(seconds)` and `refresh_ttl(seconds)`.
- `issuer(value)` to require and emit an issuer claim.
- `audience(policy)` for optional, required, or disabled audience checks.
- `leeway_seconds(seconds)` for clock skew.
- `min_secret_len(len)` for signing-secret validation.
- `jwt(...)` for algorithm and key configuration.
- `access_cookie(...)` and `refresh_cookie(...)` for opt-in cookies.
- `api_keys(...)` for API-key verification.

## JWT Users

Use `site.auth()` to issue tokens:

```rust
use vyuh::prelude::*;
use vyuh::auth::{AuthError, AuthUser, TokenPair};

async fn login(site: Site) -> Result<Json<TokenPair>, AuthError> {
    let user = AuthUser::new("user-123", 0);
    let tokens = site.auth().create_token_pair(user, &["web"])?;
    Ok(Json(tokens))
}
```

Extract `AuthUser` to require an access token:

```rust
use vyuh::prelude::*;
use vyuh::auth::AuthUser;

async fn me(user: AuthUser) -> Json<String> {
    Json(user.key.to_string())
}
```

`AuthUser` contains:

- `key`: the authenticated subject, stored as JWT `sub`.
- `roles`: a `u64` static role mask.

Access and refresh tokens are distinct. `AuthUser` accepts access tokens only,
and `site.auth().refresh(...)` accepts refresh tokens only.

Access tokens can be sent with:

```text
Authorization: Bearer <jwt>
```

The older `Authorization: JWT <jwt>` form is also accepted.

## JWT Algorithms

Vyuh defaults to `HS256` with `SiteConf.secret_key`. That matches the common
Django Simple JWT default of HS256 with Django's `SECRET_KEY`. Django core
itself does not use JWT for its built-in sessions; its signing utilities use
SHA-256 signed values and cookies.

Use `JwtConf` when deployments need a different symmetric algorithm or an
asymmetric key pair:

```rust
use vyuh::prelude::*;
use vyuh::auth::{AuthConf, JwtConf, JwtKeySource};

let auth = AuthConf::default().jwt(JwtConf::hs512(JwtKeySource::Env(
    "JWT_SECRET".into(),
)));
```

Asymmetric deployments can sign with a private PEM and verify with a public
PEM. Relative file paths are resolved from `SiteConf.project_dir`:

```rust
use vyuh::prelude::*;
use vyuh::auth::{AuthConf, JwtConf, JwtKeySource};

let auth = AuthConf::default().jwt(JwtConf::rs256(
    JwtKeySource::File("secrets/jwt-private.pem".into()),
    JwtKeySource::File("secrets/jwt-public.pem".into()),
));
```

Supported algorithms are `HS256`, `HS384`, `HS512`, `RS256`, `RS384`,
`RS512`, `ES256`, `ES384`, and `EdDSA`. HMAC algorithms use one symmetric key
and reject a separate verifying key. RSA, ECDSA, and EdDSA configurations
require both signing and verifying keys. Token decoding accepts only the
configured algorithm.

## Cookies

Cookies are opt-in. When configured, `login_user(...)` writes access and refresh
cookies, and `logout(...)` clears them:

```rust
use vyuh::prelude::*;
use vyuh::auth::AuthConf;

let conf = SiteConf::default().auth(
    AuthConf::cookie_pair("access_token", "refresh_token")
);
```

`CookieConf` uses typed `CookieSameSite` values. Invalid SameSite strings are
not silently accepted.

## Static Roles

Static role checks are useful for simple route gates:

```rust
use vyuh::prelude::*;
use vyuh::auth::{AuthUser, BitRole, permit};

#[derive(BitRole)]
enum AppRole {
    Manager,
    Editor,
    Viewer,
}

async fn managers_only(_permit: permit!(AppRole, Manager)) {}
```

Role expressions support:

- `permit!(AppRole, Manager)` - requires one role.
- `permit!(AppRole, Manager | Editor)` - requires any listed role.
- `permit!(AppRole, Manager & Editor)` - requires all listed roles.

`permit!` returns `401` for missing or invalid tokens and `403` when the token
is valid but lacks the required role mask. It also contributes role metadata to
OpenAPI.

Use handler or service logic for dynamic authorization:

```rust
use vyuh::prelude::*;
use vyuh::auth::AuthUser;

async fn edit_post(
    site: Site,
    user: AuthUser,
    Path(id): Path<i64>,
) -> Result<Json<PostOut>, Error> {
    let post = load_post(site.db(), id).await?;
    if post.owner_id != user.key.as_ref() {
        return Err(Error::new(vyuh::ErrorKind::Forbidden).with_context("not allowed"));
    }
    Ok(Json(post.into()))
}
```

## API Keys

API keys are for machine-to-machine authentication. Vyuh extracts keys, but the
application verifies them through a hook. Vyuh does not store plaintext keys or
own API-key tables.

```rust
use vyuh::prelude::*;
use vyuh::auth::{ApiKey, ApiKeyPrincipal, ApiKeyVerifier, AuthError};

struct MyVerifier;

impl ApiKeyVerifier for MyVerifier {
    async fn verify(&self, presented: &str) -> Result<ApiKeyPrincipal, AuthError> {
        if presented == "secret" {
            Ok(ApiKeyPrincipal::new("key-1").subject("service-1"))
        } else {
            Err(AuthError::InvalidApiKey)
        }
    }
}

async fn ingest(key: ApiKey) {
    let key_id = key.key_id.to_string();
}
```

Configure the verifier:

```rust
use vyuh::prelude::*;
use vyuh::auth::{ApiKeyConf, AuthConf};

let auth = AuthConf::default().api_keys(
    ApiKeyConf::default().verifier(MyVerifier)
);
```

By default, API keys are read from `X-API-Key` and
`Authorization: ApiKey <key>`. Query-string API keys are disabled by default
because URLs are commonly logged and copied. Enable them only when the protocol
requires it.

## OpenAPI

Auth is reflected in generated OpenAPI specs from handler arguments:

- `AuthUser` adds JWT security.
- `permit!(Role, ...)` adds `bearerAuth` with role scopes.
- `ApiKey` adds `apiKeyAuth`.

When cookie-pair auth is configured, Vyuh also emits a cookie security scheme
using the configured access-cookie name. Public endpoints need no annotation:
absence of auth extractors means no security requirement.

Vyuh emits standard security schemes:

```yaml
components:
  securitySchemes:
    bearerAuth:
      type: http
      scheme: bearer
      bearerFormat: JWT
    apiKeyAuth:
      type: apiKey
      in: header
      name: X-API-Key
    cookieAuth:
      type: apiKey
      in: cookie
      name: access_token
```

OpenAPI spec endpoints can be protected independently with
`OpenApiConf::auth(...)`, which receives the extracted `AuthUser`.

## Password Utilities

Vyuh includes small PBKDF2 password helpers:

- `make_password(password, salt, algorithm)`
- `check_password(password, encoded)`
- `unusable_password()`

The encoded hash format is compatible with Django PBKDF2 hashes. Django
compatibility means password-hash compatibility and migration friendliness, not
Django sessions, permissions, groups, or user tables.

## Examples

The snippets in this chapter cover JWT issuance, route protection with
`AuthUser`, optional cookies, static roles, dynamic authorization, API-key
verification, and OpenAPI security metadata.

## Failure Modes

- Missing JWTs return `AuthError::MissingToken` and HTTP `401`.
- Invalid JWTs return `AuthError::InvalidToken` and HTTP `401`.
- Expired JWTs return `AuthError::ExpiredToken` and HTTP `401`.
- Access/refresh token-kind mismatch returns `AuthError::WrongTokenKind` and
  HTTP `401`.
- Missing API keys return `AuthError::MissingApiKey` and HTTP `401`.
- Invalid API keys return `AuthError::InvalidApiKey` and HTTP `401`.
- Failed audience, role, or permission checks return `AuthError::Forbidden` and
  HTTP `403`.

## Current Limitations

- Auth is stateless unless the application stores extra session or token data.
- API-key storage and revocation are application responsibilities.
- JWK and JWKS fetching are not included in this pass.
- Vyuh does not ship user models, registration, password reset, or account
  management flows.
- Role masks are limited to 64 bit positions.
