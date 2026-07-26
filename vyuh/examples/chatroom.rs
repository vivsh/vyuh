//! Minimal channels-powered chatroom demo.
//!
//! Run with:
//!   cargo run -p vyuh --example chatroom
//!
//! Then open:
//!   http://127.0.0.1:8080

use schemars::JsonSchema;
use vyuh::prelude::*;

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, Validate)]
struct SendMessage {
    #[validate(min_length = 1, max_length = 24)]
    room: String,
    #[validate(min_length = 1, max_length = 24)]
    user: String,
    #[validate(min_length = 1, max_length = 280)]
    text: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
struct ChatMessage {
    room: String,
    user: String,
    text: String,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
struct StreamQuery {
    room: String,
    user: String,
}

#[bundles::route(path = "/")]
async fn index() -> Html<String> {
    Html(CHATROOM_HTML.to_string())
}

#[bundles::route(path = "/api/messages", method = "POST")]
async fn send_message(site: Site, Json(input): Json<SendMessage>) -> Result<StatusCode, Error> {
    input
        .validate()
        .map_err(|report| Error::bad_request(report.to_string()))?;
    site.signals()
        .emit(ChatMessage {
            room: input.room.clone(),
            user: input.user.clone(),
            text: input.text.clone(),
        })
        .map_err(Error::other)?;
    Ok(StatusCode::ACCEPTED)
}

#[bundles::route(path = "/api/events")]
async fn subscribe(
    Query(query): Query<StreamQuery>,
    sub: Subscriber,
    channels: Channels,
) -> Result<ChannelResponse, Error> {
    let room = query.room.clone();
    let stream = channels
        .user(UserKey::new(query.room)?)
        .deliver_if::<ChatMessage, _>(move |msg| msg.room == room);
    sub.attach(stream).allow(WS | SSE | POLL).await
}

#[bundles::signal]
async fn audit_message(Data(message): Data<ChatMessage>) {
    println!("[room:{}] {}: {}", message.room, message.user, message.text);
}

fn app_bundle() -> vyuh::bundles::Bundle {
    bundles::bundle! {
        index,
        send_message,
        subscribe,
        audit_message,
    }
}

#[tokio::main]
async fn main() -> Result<(), SiteError> {
    // Bind to IPv4 explicitly so the example is reachable at 127.0.0.1.
    Site::serve(SiteConf::default().host("127.0.0.1"), app_bundle()).await
}

const CHATROOM_HTML: &str = r#"<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>Vyuh Chatroom</title>
    <style>
      :root {
        color-scheme: light;
        --bg: #f5efe6;
        --panel: #fffaf4;
        --line: #d8ccb9;
        --text: #1f1b16;
        --muted: #76695b;
        --accent: #ec5b1a;
      }
      * { box-sizing: border-box; }
      body {
        margin: 0;
        background:
          radial-gradient(circle at top left, #fff8ef 0, transparent 26rem),
          linear-gradient(180deg, #efe3d4 0%, var(--bg) 40%, #f8f4ee 100%);
        color: var(--text);
        font: 16px/1.4 Georgia, "Iowan Old Style", "Palatino Linotype", serif;
      }
      main {
        width: min(64rem, calc(100vw - 2rem));
        margin: 2rem auto;
        display: grid;
        gap: 1rem;
      }
      .hero, .panel {
        background: var(--panel);
        border: 1px solid var(--line);
        border-radius: 8px;
      }
      .hero { padding: 1.5rem; }
      .hero h1 {
        margin: 0 0 0.35rem;
        font-size: clamp(2rem, 4vw, 3.25rem);
        line-height: 0.95;
        font-weight: 600;
      }
      .hero p, .muted { margin: 0; color: var(--muted); }
      .shell {
        display: grid;
        grid-template-columns: 18rem minmax(0, 1fr);
        gap: 1rem;
      }
      .panel { padding: 1rem; }
      .controls, form { display: grid; gap: 0.75rem; }
      label { display: grid; gap: 0.35rem; font-size: 0.95rem; }
      input, button {
        width: 100%;
        font: inherit;
        border-radius: 6px;
        border: 1px solid var(--line);
        padding: 0.7rem 0.8rem;
        background: white;
        color: inherit;
      }
      button {
        background: var(--accent);
        border-color: var(--accent);
        color: white;
        cursor: pointer;
      }
      .status {
        display: flex;
        align-items: center;
        gap: 0.5rem;
        font-size: 0.95rem;
      }
      .dot {
        width: 0.7rem;
        height: 0.7rem;
        border-radius: 999px;
        background: #b94a48;
      }
      .dot.online { background: var(--accent); }
      .chat {
        display: grid;
        grid-template-rows: minmax(18rem, 1fr) auto;
        min-height: 28rem;
        gap: 1rem;
      }
      .messages {
        margin: 0;
        padding: 0;
        list-style: none;
        display: grid;
        gap: 0.75rem;
        overflow: auto;
      }
      .message {
        padding: 0.75rem 0.85rem;
        border: 1px solid var(--line);
        border-radius: 6px;
        background: white;
      }
      .meta {
        display: flex;
        justify-content: space-between;
        gap: 1rem;
        font-size: 0.85rem;
        color: var(--muted);
        margin-bottom: 0.35rem;
      }
      .message strong { color: var(--accent); }
      .message-form {
        display: grid;
        grid-template-columns: minmax(0, 1fr) 8rem;
        gap: 0.75rem;
      }
      @media (max-width: 760px) {
        .shell { grid-template-columns: 1fr; }
        .message-form { grid-template-columns: 1fr; }
      }
    </style>
  </head>
  <body>
    <main>
      <section class="hero">
        <p class="muted">Channels + signals + one publish route</p>
        <h1>Chatroom</h1>
        <p>Open this page in two browsers with different names and the same room.</p>
      </section>

      <section class="shell">
        <aside class="panel controls">
          <label>
            Name
            <input id="user" value="alice" maxlength="24" />
          </label>
          <label>
            Room
            <input id="room" value="lobby" maxlength="24" />
          </label>
          <div class="status">
            <span id="dot" class="dot"></span>
            <span id="status">Disconnected</span>
          </div>
          <button id="connect" type="button">Connect</button>
        </aside>

        <section class="panel chat">
          <ol id="messages" class="messages"></ol>
          <form id="message-form" class="message-form">
            <input id="message" placeholder="Say something to the room" maxlength="280" />
            <button type="submit">Send</button>
          </form>
        </section>
      </section>
    </main>

    <script>
      const messages = document.getElementById("messages");
      const status = document.getElementById("status");
      const dot = document.getElementById("dot");
      const connectBtn = document.getElementById("connect");
      const form = document.getElementById("message-form");
      const roomInput = document.getElementById("room");
      const userInput = document.getElementById("user");
      const messageInput = document.getElementById("message");
      let events = null;

      function escapeHtml(value) {
        return value
          .replaceAll("&", "&amp;")
          .replaceAll("<", "&lt;")
          .replaceAll(">", "&gt;")
          .replaceAll('"', "&quot;");
      }

      function setStatus(text, online) {
        status.textContent = text;
        dot.classList.toggle("online", online);
      }

      function appendMessage(event) {
        const item = document.createElement("li");
        item.className = "message";
        const createdAt = new Date(event.created_at * 1000).toLocaleTimeString();
        item.innerHTML = `
          <div class="meta">
            <span><strong>${escapeHtml(event.data.user)}</strong> in #${escapeHtml(event.data.room)}</span>
            <span>${createdAt}</span>
          </div>
          <div>${escapeHtml(event.data.text)}</div>
        `;
        messages.appendChild(item);
        messages.scrollTop = messages.scrollHeight;
      }

      function connect() {
        if (events) {
          events.close();
        }
        messages.innerHTML = "";
        const room = roomInput.value.trim() || "lobby";
        const user = userInput.value.trim() || "guest";
        const url = new URL("/api/events", window.location.origin);
        url.searchParams.set("transport", "sse");
        url.searchParams.set("room", room);
        url.searchParams.set("user", user);

        events = new EventSource(url);
        setStatus("Connected with SSE", true);
        events.onmessage = (message) => appendMessage(JSON.parse(message.data));
        events.addEventListener("ChatMessage", (message) => {
          appendMessage(JSON.parse(message.data));
        });
        events.onerror = () => {
          setStatus("Reconnecting", false);
          events.close();
          setTimeout(connect, 1000);
        };
      }

      connectBtn.addEventListener("click", connect);

      form.addEventListener("submit", async (event) => {
        event.preventDefault();
        const text = messageInput.value.trim();
        if (!text) return;
        await fetch("/api/messages", {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({
            room: roomInput.value.trim() || "lobby",
            user: userInput.value.trim() || "guest",
            text
          })
        });
        messageInput.value = "";
        messageInput.focus();
      });

      connect();
    </script>
  </body>
</html>
"#;
