"""Process-wide TLS configuration for application HTTP clients."""

from __future__ import annotations

from threading import Lock

_configure_lock = Lock()
_configured = False


def configure_native_trust_store() -> None:
    """Use the operating system certificate store without weakening validation."""
    global _configured
    if _configured:
        return
    with _configure_lock:
        if _configured:
            return
        try:
            import truststore
        except ImportError as exc:
            raise RuntimeError("truststore is required for verified HTTPS connections") from exc
        truststore.inject_into_ssl()
        _configured = True
