"""Generate and verify Vyuh auth-token fixtures with Django 5.2 itself."""

import json
from pathlib import Path
from unittest.mock import patch

import django
from django.conf import settings
from django.core import signing

FIXTURE = Path(__file__).with_name("django_signing.json")
SALT = "django.core.signing"


def django_token(payload: dict, secret: str) -> str:
    """Create a deterministic token through Django's public signing API."""
    with patch("django.core.signing.time.time", return_value=payload["iat"]):
        return signing.dumps(payload, key=secret, salt=SALT, compress=False)


def verified_payload(token: str, secret: str) -> dict:
    """Verify and decode a token through Django's public signing API."""
    value = signing.loads(token, key=secret, salt=SALT)
    if not isinstance(value, dict):
        raise SystemExit("Django decoded a non-object authentication envelope")
    return value


def main() -> None:
    """Check Django-to-Vyuh generation and Vyuh-to-Django verification."""
    if not settings.configured:
        settings.configure(SECRET_KEY="fixture-only", SECRET_KEY_FALLBACKS=[])
    if django.VERSION[:2] != (5, 2):
        raise SystemExit(f"Django 5.2 is required, found {django.get_version()}")
    fixture = json.loads(FIXTURE.read_text())
    generated = django_token(fixture["payload"], fixture["secret"])
    if generated != fixture["django_token"]:
        raise SystemExit("Django-generated token differs from the committed fixture")
    legacy_payload = dict(fixture["payload"])
    legacy_payload.pop("aud")
    if django_token(legacy_payload, fixture["secret"]) != fixture["legacy_token"]:
        raise SystemExit("Django-generated legacy token differs from the committed fixture")
    for name in ("django_token", "vyuh_token"):
        if verified_payload(fixture[name], fixture["secret"]) != fixture["payload"]:
            raise SystemExit(f"Django rejected the {name} payload")
    if verified_payload(fixture["legacy_token"], fixture["secret"]) != legacy_payload:
        raise SystemExit("Django rejected the legacy payload")


if __name__ == "__main__":
    main()
