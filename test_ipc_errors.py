"""Unit tests for HugrIPCResponse error-part handling.

Regression for the silent-drop bug: the server emits GraphQL errors as a
multipart part with `X-Hugr-Part-Type: errors` (plural, a JSON array);
the client must surface them, not skip them.
"""
from hugr.client import HugrIPCResponse

BOUNDARY = "testboundary"


class _FakeResp:
    def __init__(self, body: bytes, content_type: str):
        self.content = body
        self.encoding = "utf-8"
        self.headers = {"content-type": content_type}


def _multipart(*parts: bytes) -> _FakeResp:
    body = b""
    for p in parts:
        body += b"--" + BOUNDARY.encode() + b"\r\n" + p + b"\r\n"
    body += b"--" + BOUNDARY.encode() + b"--\r\n"
    return _FakeResp(body, f"multipart/mixed; boundary={BOUNDARY}")


ERRORS_PART = (
    b"Content-Type: application/json\r\n"
    b"X-Hugr-Part-Type: errors\r\n"
    b"X-Hugr-Format: object\r\n"
    b"\r\n"
    b'[{"message": "syntax error: unexpected EOF"}, {"message": "second"}]'
)

DATA_PART = (
    b"Content-Type: application/json\r\n"
    b"X-Hugr-Path: data.foo\r\n"
    b"X-Hugr-Part-Type: data\r\n"
    b"X-Hugr-Format: object\r\n"
    b"\r\n"
    b'{"foo": 1}'
)


def test_errors_only_raises():
    try:
        HugrIPCResponse(_multipart(ERRORS_PART))
    except ValueError as e:
        assert "syntax error" in str(e), str(e)
        return
    raise AssertionError("errors-only response did not raise")


def test_partial_keeps_data_and_exposes_errors():
    r = HugrIPCResponse(_multipart(DATA_PART, ERRORS_PART))
    assert isinstance(r.errors, list), type(r.errors)
    assert len(r.errors) == 2, r.errors          # the JSON array was flattened
    assert r.errors[0]["message"].startswith("syntax error")
    assert "data.foo" in r.parts                 # data survived
    raised = False
    try:
        r.raise_for_errors()
    except ValueError:
        raised = True
    assert raised, "raise_for_errors did not raise on a partial-error response"


def test_clean_response_has_empty_errors():
    r = HugrIPCResponse(_multipart(DATA_PART))
    assert r.errors == []
    assert "data.foo" in r.parts


if __name__ == "__main__":
    test_errors_only_raises()
    test_partial_keeps_data_and_exposes_errors()
    test_clean_response_has_empty_errors()
    print("OK: all error-part tests passed")
