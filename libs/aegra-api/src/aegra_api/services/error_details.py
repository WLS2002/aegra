"""Bounded diagnostic fingerprints without arbitrary exception text or payloads."""

import hashlib
import re
from typing import Any

_CODES = (
    (r"(unauthori[sz]ed|forbidden|permission denied|access denied)", "access_denied"),
    (r"(quota|insufficient.*(credit|balance)|payment required)", "quota_exhausted"),
    (r"(rate.?limit|too many requests)", "rate_limited"),
    (r"(timed? ?out|timeout)", "timeout"),
    (r"(connection|connecterror|network)", "connection_error"),
    (r"(invalid|validation)", "validation_error"),
)


def describe_error(exc: BaseException) -> dict[str, Any]:
    """Traverse groups/causes, storing at most 16 safe leaves and five levels."""
    leaves: list[dict[str, Any]] = []
    seen: set[int] = set()
    truncated = False

    def visit(error: BaseException, depth: int) -> None:
        nonlocal truncated
        if id(error) in seen:
            return
        seen.add(id(error))
        if len(leaves) >= 16 or depth > 5:
            truncated = True
            return
        if isinstance(error, BaseExceptionGroup):
            for child in error.exceptions:
                visit(child, depth + 1)
            return
        if error.__cause__ is not None:
            visit(error.__cause__, depth + 1)
        kind = type(error).__name__[:128]
        # Inspect text only for a fixed classification. Never persist the text.
        try:
            message = str(error)[:8192]
        except Exception:
            message = ""
        code = next((value for pattern, value in _CODES if re.search(pattern, message, re.I)), "execution_error")
        leaf: dict[str, Any] = {"type": kind, "code": code}
        try:
            status = getattr(error, "status_code", None)
        except Exception:
            status = None
        if isinstance(status, int) and 100 <= status <= 599:
            leaf["status_code"] = status
        for note in getattr(error, "__notes__", ()):
            match = re.search(r"During task with name '([\w.:-]{1,128})'", str(note))
            if match:
                leaf["node"] = match[1]
        frame = error.__traceback__
        sites: list[str] = []
        while frame:
            sites.append(f"{frame.tb_frame.f_code.co_name}:{frame.tb_lineno}")
            frame = frame.tb_next
        leaf["fingerprint"] = hashlib.sha256((kind + code + "|".join(sites[-8:])).encode()).hexdigest()[:20]
        if len(leaves) < 16:
            leaves.append(leaf)
        else:
            truncated = True

    visit(exc, 0)
    return {"version": 1, "leaves": leaves, "truncated": truncated}


def error_summary(details: dict[str, Any]) -> str:
    """A safe, useful legacy error_message, preserving the old string field."""
    return (
        "; ".join(dict.fromkeys(f"{leaf['type']}: {leaf['code']}" for leaf in details["leaves"]))[:2048]
        or "ExecutionError"
    )
