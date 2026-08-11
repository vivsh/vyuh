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

Enable the separate `oauth` auth feature when an external authorization server
issues JWT access tokens. Vyuh is a resource server, not an OAuth server:
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

OAuth discovery and JWKS validation are part of generic Vyuh auth. The provider
validates issuer, signature, algorithm, audience, time claims, and all required
upstream scopes before producing `AuthUser`. By default it maps `sub` to
`AuthUser::subject()` with no application scopes. An explicit identity mapper resolves
trusted application scopes. Upstream OAuth scopes are not copied into Vyuh
application scopes automatically; `Permit<R>` evaluates only the mapped grants.

MCP publishes RFC 9728 Protected Resource Metadata only when exactly one
central provider eligible for its audience exposes compatible metadata. Native
token and key services do not advertise an OAuth flow. OAuth v1 accepts JWT access tokens only; opaque-token
introspection is intentionally unsupported.

OAuth providers initialize during `Site::build`. Their private per-site Huskarl
verifier owns bounded in-memory JWKS reuse, key rotation, single-flight refresh,
and unknown-key refresh throttling. OAuth metadata and JWKS do not consume a
named Vyuh cache provider. Startup therefore requires working discovery and an
initial JWKS load; runtime refresh outages are classified as provider failures
without exposing upstream responses or verifier details to MCP clients.

## Tools

Each `mcp_tool` callable exposes its one object payload as the tool input
schema. The model never sees paths, headers, cookies, credentials, `AuthUser`,
or permits. Tools run directly through `McpToolContext`; MCP never reconstructs
or dispatches an HTTP route.
