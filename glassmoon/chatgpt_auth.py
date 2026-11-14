"""Local OAuth credentials for OpenAI's Sign in with ChatGPT plan usage."""

import base64
import fcntl
import hashlib
import json
import os
import secrets
import tempfile
import time
import uuid
import webbrowser
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlencode, urlsplit

import httpx
import jwt

ISSUER = "https://auth.openai.com"
RESOURCE = "https://api.openai.com/v1"
TOKEN_URL = f"{ISSUER}/api/accounts/oauth/token"
SCOPES = "openid profile email offline_access resource.invoke chatgpt.tokens.use.direct"
PLAN_SCOPE = "chatgpt.tokens.use.direct"
CALLBACK = "/auth/callback"
DEFAULT_PROFILE = Path.home() / ".config" / "glassmoon" / "chatgpt.json"


class AuthError(Exception):
    pass


class Profile:
    def __init__(self, path):
        self.path = Path(path).expanduser()

    @contextmanager
    def locked(self):
        self.path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        with os.fdopen(os.open(f"{self.path}.lock", os.O_CREAT | os.O_RDWR, 0o600), "r+") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            yield

    def read(self):
        try:
            with self.path.open() as file:
                if os.fstat(file.fileno()).st_mode & 0o077:
                    raise AuthError("ChatGPT profile permissions must be 0600")
                return json.load(file)
        except FileNotFoundError:
            return {}

    def write(self, profile):
        fd, temporary = tempfile.mkstemp(dir=self.path.parent)
        try:
            with os.fdopen(fd, "w") as file:
                json.dump(profile, file)
                file.flush()
                os.fsync(file.fileno())
            os.replace(temporary, self.path)
        finally:
            Path(temporary).unlink(missing_ok=True)

    def access_token(self):
        with self.locked():
            current = self.read()
            if not current.get("access_token"):
                raise AuthError("Run python -m glassmoon.openai_service login first")
            if time.time() + 60 >= current["expires_at"]:
                tokens = token_request(
                    grant_type="refresh_token",
                    client_id=current["client_id"],
                    refresh_token=current["refresh_token"],
                )
                current = update_tokens(current, tokens)
                self.write(current)
            return current["access_token"]


def token_request(**parameters):
    try:
        response = httpx.post(TOKEN_URL, data={"resource": RESOURCE, **parameters}, timeout=15)
        response.raise_for_status()
        return response.json()
    except httpx.HTTPStatusError as error:
        if error.response.status_code in (400, 401, 403):
            raise AuthError("ChatGPT authorization expired or was rejected. Run login again.") from None
        raise AuthError(f"OpenAI token endpoint returned HTTP {error.response.status_code}") from None
    except (httpx.RequestError, ValueError):
        raise AuthError("Could not read the OpenAI token response") from None


def update_tokens(profile, tokens):
    scopes = tokens.get("scope", profile.get("scope", ""))
    if PLAN_SCOPE not in scopes.split():
        raise AuthError("ChatGPT plan usage permission was not granted")
    if not tokens.get("access_token") or not tokens.get("expires_in") or not (
        tokens.get("refresh_token") or profile.get("refresh_token")
    ):
        raise AuthError("OpenAI returned an incomplete token set")
    return profile | {
        "scope": scopes,
        "access_token": tokens["access_token"],
        "refresh_token": tokens.get("refresh_token") or profile["refresh_token"],
        "expires_at": time.time() + int(tokens["expires_in"]),
    }


def browser_authorization(host, client, verifier, nonce):
    state = secrets.token_urlsafe(32)
    challenge = base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest()).rstrip(b"=")
    answer = {}

    class CallbackHandler(BaseHTTPRequestHandler):
        def log_message(self, *_args):
            pass

        def do_GET(self):
            callback = urlsplit(self.path)
            values = parse_qs(callback.query)
            if callback.path != CALLBACK or not secrets.compare_digest(
                values.get("state", [""])[0].encode(), state.encode()
            ):
                self.send_error(400, "Invalid callback")
                return
            answer.update({key: value[0] for key, value in values.items()})
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(b"Return to the GlassMoon terminal for the sign-in result.")

    # Threads prevent a browser's speculative connection from blocking the callback.
    with ThreadingHTTPServer(("127.0.0.1", 0), CallbackHandler) as server:
        server.timeout = 0.25
        redirect = f"http://127.0.0.1:{server.server_port}{CALLBACK}"
        params = {
            "client_id": client or "dynamic_agent_client",
            "ext_agent_host_id": host, "response_type": "code",
            "redirect_uri": redirect, "scope": SCOPES, "resource": RESOURCE,
            "state": state, "nonce": nonce, "code_challenge_method": "S256",
            "code_challenge": challenge.decode(),
        }
        if not client:
            params["agent_name_hint"] = "GlassMoon"
        url = f"{ISSUER}/api/accounts/authorize?{urlencode(params)}"
        print("Continue with ChatGPT in your browser.", flush=True)
        if not webbrowser.open(url):
            print(url, flush=True)
        deadline = time.monotonic() + 180
        while not answer and time.monotonic() < deadline:
            server.handle_request()
    if answer.get("error") or not answer.get("code"):
        raise AuthError("ChatGPT sign-in was declined or did not complete")
    issued_client = answer.get("client_id", client)
    if not issued_client or issued_client == "dynamic_agent_client" or (client and client != issued_client):
        raise AuthError("OpenAI returned an unexpected OAuth client")
    return issued_client, redirect, answer["code"]


def validate_identity(token, client, nonce):
    try:
        keys = jwt.PyJWKClient(f"{ISSUER}/.well-known/jwks.json", timeout=15)
        key = keys.get_signing_key_from_jwt(token)
        identity = jwt.decode(
            token, key, algorithms=["RS256"], audience=client, issuer=ISSUER, leeway=5,
            options={"require": ["sub", "exp", "iat", "nonce"]},
        )
    except jwt.PyJWTError:
        raise AuthError("Could not verify the ChatGPT identity") from None
    if identity["nonce"] != nonce:
        raise AuthError("ChatGPT returned an unexpected sign-in nonce")
    return identity


def login(profile):
    with profile.locked():
        previous = profile.read()
        if not previous.get("host"):
            previous["host"] = f"urn:uuid:{uuid.uuid4()}"
            profile.write(previous)
    verifier, nonce = secrets.token_urlsafe(48), secrets.token_urlsafe(32)
    client, redirect, code = browser_authorization(previous["host"], previous.get("client_id"), verifier, nonce)
    tokens = token_request(
        grant_type="authorization_code", client_id=client, code=code,
        code_verifier=verifier, redirect_uri=redirect,
    )
    if not tokens.get("id_token"):
        raise AuthError("OpenAI returned no identity token")
    identity = validate_identity(tokens["id_token"], client, nonce)
    if previous.get("subject", identity["sub"]) != identity["sub"]:
        raise AuthError("Sign-in returned a different account. Use a separate --profile.")
    with profile.locked():
        current = profile.read()
        if current.get("client_id") not in (None, client):
            raise AuthError("The saved account changed during sign-in")
        profile.write(update_tokens({
            "host": previous["host"], "client_id": client, "subject": identity["sub"],
            "email": identity.get("email"),
        }, tokens))
    print(f"ChatGPT plan connected for {identity.get('email') or 'your account'}.")
