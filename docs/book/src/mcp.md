# Model Context Protocol

Vyuh can expose explicitly registered semantic JSON operations as remote MCP
tools behind the optional `mcp` feature. MCP owns protocol framing, tool
discovery, and dispatch; Vyuh authentication owns every credential decision.

Each service uses the owning bundle's effective audience and therefore the same
central authentication registry as ordinary HTTP routes:

```rust,ignore
const NOTES_MCP: Audience =
    Audience::new("https://api.example.com/mcp/notes");
let notes = bundles::bundle! { search_notes }.with_conf(
    bundles::conf()
        .audience(NOTES_MCP)
        .mcp(McpConf::new("/mcp/notes")),
);
```

The engine receives the ordinary `AuthUser`, filters tools by
`Permit<ScopeRule>`, and never forwards the original credential to a tool.
Use `.public()` for deliberate anonymous exposure or `.auth(predicate)` for an
additional `AuthUser` check. Provider selection remains central and audience
based; a provider eligible for the MCP audience can authenticate it just as it
can any other route with that audience.

Anonymous exposure is deliberate and cannot claim a tool requiring `AuthUser`
or `Permit<ScopeRule>`.

## External OAuth

Enable `oauth-resource` when an external authorization server issues access
tokens. In this mode Vyuh is the resource server:
Hydra, Auth0, Keycloak, or another provider owns login, consent, PKCE, client
registration, token issuance, and refresh.

```rust,ignore
let auth = AuthConf::default().provider(
    HYDRA,
    OAuthResourceServer::discovery("https://auth.example.com")
        .resource(
            NOTES_MCP,
            OAuthResource::new("https://api.example.com/notes")
                .advertise_scopes(["mcp.notes", "mcp.notes.read"])
                .require_scopes(["mcp.notes"]),
        )
        .mapper(AppIdentityMapper::new(users)),
);
```

By default, generic Vyuh auth verifies access JWTs locally with discovered keys.
For opaque tokens, configure [explicit introspection](auth.md#opaque-tokens-and-introspection)
on the same provider. Both modes enforce the external audience and required
upstream scopes before producing `AuthUser`. By default the provider maps `sub` to
`AuthUser::subject()` with no application scopes. An explicit identity mapper resolves
trusted application scopes. Upstream OAuth scopes are not copied into Vyuh
application scopes automatically; `Permit<R>` evaluates only the mapped grants.

MCP publishes RFC 9728 Protected Resource Metadata only when exactly one
central provider eligible for its audience exposes compatible metadata. Native
token and key services do not advertise an OAuth flow. The discovery metadata
and Bearer challenges do not change when switching validation modes.

OAuth providers initialize during `Site::build`. Both modes require working
discovery; JWT mode additionally loads JWKS, maintains keys per site, and
refreshes them on rotation. Introspection makes an uncached request per
presented credential. An upstream validation outage is reported as a provider
failure without exposing credentials or verifier details to MCP clients.

## Issue OAuth credentials with your existing login

Enable `oauth-issue` and one database feature when the same application should
authorize access to its own MCP tools. Clients must support authorization code
with PKCE S256, protected-resource and authorization-server discovery, and the
`resource` parameter on authorization and token requests. The application
provides the sign-in and consent pages for every client mode.

Use a pre-registered client with the exact callback shown by that client.
For a confidential static client, configure a dedicated secret with
`OAuthClient::secret(KeySource::env("MCP_CLIENT_SECRET"))`.

```rust,ignore
const BROWSER: Audience = Audience::new("browser");
const NOTES_MCP: Audience = Audience::new("https://app.example.com/mcp/notes");
const NOTES_READ: Scope = Scope::of("notes:read");

let auth = AuthConf::default()
    .provider(
        WEB_AUTH,
        TokenProvider::new(Jwt::hs256_site_secret())
            .audiences([BROWSER])
            .access(TokenConf::cookie("browser_session")),
    )
    .oauth_issuer(
        OAuthIssuer::new("https://app.example.com")
            .authorize_route("oauth_authorize")
            .browser_audience(BROWSER)
            .client(OAuthClient::new(client_id, "Notes client").redirect_uri(callback_uri))
            .resource(NOTES_MCP, [NOTES_READ])
            .verifier(ActiveAccount),
    );
```

Use the configured resource URL as the protected MCP bundle's audience. Its
origin must equal the HTTPS issuer and its path must match the endpoint. The
audience is exclusive to the issuer; explicitly restrict all other providers
to their own audiences. Put your named, static `oauth_authorize` GET route where
your existing access cookie is sent. The site must have exactly one
CSRF-protected cookie token provider eligible for `BROWSER`.

Your named GET handler calls `site.oauth()?.request(&parts).await?`.
On `OAuthConsent::Redirect(response)`, return the protocol redirect unchanged.
On `OAuthConsent::SignIn { return_to }`, use your existing login endpoint and
pass that local return URL through your usual sign-in flow. On
`OAuthConsent::Ready`, render your own consent screen using the client name,
resource and requested scopes. Keep the opaque continuation out of URLs and
logs. Set `Cache-Control: no-store` and `Referrer-Policy: no-referrer` on your
browser pages.

Your POST handler calls `site.oauth()?.approve(&parts, &continuation, &selected_scopes).await`
or `site.oauth()?.deny(&parts, &continuation).await` and returns the resulting
redirect. Supply the usual CSRF header. If you use a native HTML form, you can
read the CSRF value from a hidden field and pass it in the configured request
header before calling the helper; **never** populate the header from the
incoming cookie on the server. The helpers use the exact accepted access
cookie; if it rotated during consent, start again. A denied or approved
continuation cannot be used a second time.

`OAuthGrantVerifier` borrows the invoking `&Site` and resolves the original
subject to its current account and scopes. It can reject disabled or missing
accounts. The browser user's current scopes limit consent; MCP receives only
the intersection of the approved scopes, the configured resource scopes, and
the current account scopes. The usual MCP predicate and tool permits still
apply. The verifier runs for resource access alongside the issuer's indexed
credential and grant read. Dynamically registered clients also require an
indexed registration lookup. CIMD bearer access uses the local trust rule.

Vyuh serves `/.well-known/oauth-authorization-server`, `/oauth/token`,
`/oauth/revoke`, and the existing MCP protected-resource metadata. Codes live
for two minutes, access tokens for up to ten minutes, and grants for up to 30
days. Refresh rotates both credentials; a validated replay revokes the grant,
including any replacement from a competing refresh. If a refresh response is
lost after commit, the client may need to authorize again. Do not retry refresh
concurrently.

Generate and apply the [application migration](migrations.md#oauth-issuance)
before serving OAuth traffic. Set the same issuer configuration and secret ring
on every replica, synchronize clocks, and use a shared database. Rate-limit
public protocol endpoints at the edge and redact request bodies and query
strings in proxies and application logs. Call `site.oauth()?.prune(limit).await`
from trusted scheduled maintenance to delete expired grants and their credential
history; there is no automatic cleanup task. Verify an actual connection with
each target client before rollout.

### Allow a client metadata document

Add one `OAuthCimdClient` rule for each exact HTTPS client ID that the
application trusts. Give the rule an operator-controlled label and exact
callbacks. Vyuh displays the document hostname alongside that label. Before
accepting a callback, Vyuh requires an exact match in both the document and
your rule.

```rust,ignore
let issuer = issuer.cimd_client(
    OAuthCimdClient::new(
        "https://client.example.com/oauth/client.json",
        "Notes integration",
        OAuthCimdAuth::PrivateKeyJwt,
    )
    .redirect_uri("https://client.example.com/oauth/callback"),
);
```

Use `OAuthCimdAuth::Public` for a PKCE-only client you trust to use that
method. `PrivateKeyJwt` requires a signed assertion on code exchange, refresh,
and revocation, even if the document also offers `none`. Document and key
resolution uses bounded HTTPS requests to public addresses. Supply a public
JWKS inline or at a same-origin HTTPS URL. Every signed attempt needs a fresh
`jti` and an assertion whose sole audience is the issuer URL. The supported
signing algorithms are RS256 and ES256. Publish overlapping keys during
rotation so pending assertions signed with the previous key can still verify.
Vyuh reads the current key set on the next signed request.

Keep replica clocks synchronized to within 30 seconds and use the same shared
issuer database. Schedule `site.oauth()?.prune_assertions(limit).await` from
trusted maintenance to remove expired replay entries. Metadata or JWKS
unavailability prevents dependent authorization and signed protocol operations.
An already-issued bearer token can continue until expiry while the local trust
rule and the grant remain valid.

### Enable dynamic client registration

Add `OAuthDcr` policy to enable `POST /oauth/register` and advertise it in
discovery. DCR and CIMD can be enabled independently or together. Configure
exact callback URLs, the permitted methods, and the maximum scopes that a
registration can request. Authorization still checks the requested scopes
against the exact resource and the current user account.

```rust,ignore
let issuer = issuer.dynamic_registration(
    OAuthDcr::new(
        "Notes integration",
        OAuthDcrAdmission::InitialAccessToken(KeySource::env("MCP_DCR_INITIAL_TOKEN")),
    )
    .redirect_uri("https://client.example.com/oauth/callback")
    .method(OAuthDcrMethod::None)
    .method(OAuthDcrMethod::ClientSecretBasic)
    .scopes([NOTES_READ])
    .allow_refresh(),
);
```

The registration client sends the initial token as an HTTP Bearer credential.
`OAuthDcrAdmission::Unauthenticated` explicitly enables admission without
caller authentication. Apply shared ingress limits to the registration route
and treat its requester as unidentified. Each issuer has a shared limit of
1,024 registrations. Omitted authentication defaults to `client_secret_basic`
and is accepted only if policy permits it. Omitted scope uses the configured
scope ceiling. A client can register for refresh by requesting `refresh_token`
alongside `authorization_code` when the policy permits it.

Registration binds the selected authentication method to the new client ID.
For either supported secret method, the registration response is the only time
the generated secret is returned. Keep the ID and secret for the life of the
connection. Trusted operator code can call
`site.oauth()?.disable_registration(client_id).await` to delete an abandoned or
compromised registration and free its slot. Later requests using that client's
grants fail, and the grant history expires through ordinary maintenance.

### Check a deployed issuer and run the smoke tests

Add the optional command to the application bundle passed to `Site::run`.
Run it with the same issuer configuration as the HTTPS deployment being checked.

```rust,ignore
let bundle = bundle.merge(vyuh::bundles::bundle([vyuh::auth::mcp_preflight()]));
```

```sh
./app mcp:preflight --resource https://app.example.com/mcp/notes \
  --mode signed-cimd --client-id https://client.example.com/oauth/client.json \
  --redirect-uri https://client.example.com/oauth/callback --scopes notes:read \
  --require-refresh

./app mcp:preflight --resource https://app.example.com/mcp/notes \
  --mode dcr --redirect-uri https://client.example.com/oauth/callback \
  --token-auth-method none --scopes notes:read --require-refresh --json
```

The preflight checks local policy, the deployed bearer challenge, and the
discovery documents. Signed CIMD also checks the trusted metadata and published
key shape. DCR checks the registration advertisement and never creates a client.
The command exits with an error if a required check fails. `--json` emits the
resource, mode, and ordered checks with `pass`, `fail`, or `skipped` status.
The command sends an unauthenticated probe, so a gateway that requires credentials
before discovery responds will appear as a reachability or challenge failure.

Run the two smoke tests separately from the Vyuh workspace. Each uses a
temporary SQLite database and in-process HTTP handlers. The tests exercise
login, CSRF-protected consent, authorization code, scoped tool access,
refresh, and revocation. Signed CIMD uses a test document and signing keys
with DCR disabled. The DCR test registers public and confidential clients
with CIMD disabled.

```sh
cargo test -p vyuh --no-default-features --features sqlite,oauth-issue \
  --lib auth::oauth_issue::tests::smoke::signed_cimd -- --exact

cargo test -p vyuh --no-default-features --features sqlite,oauth-issue \
  --lib auth::oauth_issue::tests::smoke::dcr -- --exact
```

A minimal ChatGPT plugin `mcp.json` can select CIMD and let discovery supply
the OAuth endpoints. Set the exact client document and callback in the
application's trusted policy.

```json
{
  "mcpServers": {
    "notes": {
      "type": "streamable-http",
      "url": "https://app.example.com/mcp/notes",
      "extensions": {
        "com.openai": {
          "auth": {
            "type": "oauth",
            "client": { "mode": "cimd" }
          }
        }
      }
    }
  }
}
```

## Tools

Each `mcp_tool` callable exposes its one object payload as the tool input
schema. The model never sees paths, headers, cookies, credentials, `AuthUser`,
or permits. Tools run directly through `McpToolContext`; MCP never reconstructs
or dispatches an HTTP route.

## Static resources and MCP Apps UI

An MCP service can also expose immutable registered resources. Resources are
not Vyuh routes: they have no handler, database lookup, template rendering, or
runtime filesystem access. They are advertised through `resources/list` and
read through `resources/read` behind the same MCP endpoint authentication
boundary as tools.

Use `#[bundles::mcp_resource]` for concise factory registration. Its function
name becomes the standard MCP resource name. The direct equivalent is
`bundles::mcp_resource("member_card", member_card())`.

```rust,ignore
#[bundles::mcp_resource]
fn member_card() -> McpResource {
    McpResource::text(
        "ui://widget/member-card.html",
        "text/html;profile=mcp-app",
        include_str!("member-card.html"),
    )
    .ui(McpUiResourceMeta::default().prefers_border(true))
}

#[bundles::mcp_tool(ui_resource_uri = "ui://widget/member-card.html")]
async fn render_member(input: Data<MemberCard>) -> Data<MemberCard> {
    input
}
```

`ui_resource_uri` is optional. When present, Vyuh emits the standard MCP Apps
tool metadata in `tools/list`:

```json
{
  "_meta": {
    "ui": { "resourceUri": "ui://widget/member-card.html" }
  }
}
```

The referenced resource must belong to the same MCP service, use a strict
`ui://authority/path` URI, and have exactly
`text/html;profile=mcp-app`. Other resources may use any valid MIME type, but
cannot be attached as MCP Apps UI. Duplicate resource URIs, unclaimed
resources, malformed metadata, and cross-service attachments fail site
construction.

Use a data-tool/render-tool split when a host should display an app: a data
tool returns structured content with no UI attachment; the render tool accepts
the final model-checked object and declares `ui_resource_uri`. The attachment
does not change tool input, result content, output schemas, permissions, or
ordinary non-UI clients.

The HTML receives initialization, tool input, and tool result notifications via
the MCP Apps JSON-RPC `postMessage` bridge. A minimal portable listener is:

```html
<script>
  window.addEventListener("message", (event) => {
    const message = event.data;
    if (message?.method !== "ui/notifications/tool-result") return;
    const result = message.params?.structuredContent;
    document.body.textContent = result?.name ?? "No member selected";
  });
</script>
```

Applications should feature-detect host-specific extensions rather than rely
on them. MCP Apps resource fields and bridge methods are the portability
baseline; OpenAI-specific aliases such as `openai/outputTemplate` are not used
by Vyuh.
