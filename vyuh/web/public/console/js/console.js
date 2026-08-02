document.addEventListener("htmx:configRequest", (event) => {
  const csrf = csrfToken();
  if (csrf) {
    event.detail.headers["x-csrf-token"] = csrf;
  }
});

function csrfToken() {
  const meta = document.querySelector("meta[name='csrf-token']");
  if (meta) {
    return meta.content;
  }
  const cookie = document.cookie
    .split(";")
    .map((value) => value.trim())
    .find((value) => value.slice(0, value.indexOf("=")).endsWith("_csrf"));
  return cookie ? decodeURIComponent(cookie.slice(cookie.indexOf("=") + 1)) : null;
}

function activateTab(tab) {
  const tablist = tab.closest("[data-console-tabs]");
  if (!tablist) {
    return;
  }

  const panelId = tab.getAttribute("aria-controls");
  if (!panelId) {
    return;
  }

  const root = tablist.parentElement;
  if (!root) {
    return;
  }

  for (const item of tablist.querySelectorAll("[role='tab']")) {
    item.setAttribute("aria-selected", String(item === tab));
    item.tabIndex = item === tab ? 0 : -1;
  }

  for (const panel of root.querySelectorAll("[role='tabpanel']")) {
    panel.hidden = panel.id !== panelId;
  }
}

function moveTab(tablist, direction) {
  const tabs = Array.from(tablist.querySelectorAll("[role='tab']"));
  const current = tabs.findIndex((tab) => tab.getAttribute("aria-selected") === "true");
  if (current < 0 || tabs.length === 0) {
    return;
  }

  const next = (current + direction + tabs.length) % tabs.length;
  const tab = tabs[next];
  if (tab) {
    tab.focus();
    activateTab(tab);
  }
}

document.addEventListener("click", (event) => {
  const tab = event.target.closest("[data-console-tabs] [role='tab']");
  if (!tab) {
    return;
  }

  event.preventDefault();
  activateTab(tab);
});

document.addEventListener("keydown", (event) => {
  const tablist = event.target.closest("[data-console-tabs]");
  if (!tablist) {
    return;
  }

  if (event.key === "ArrowRight") {
    event.preventDefault();
    moveTab(tablist, 1);
  } else if (event.key === "ArrowLeft") {
    event.preventDefault();
    moveTab(tablist, -1);
  }
});
