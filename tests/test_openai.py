import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import httpx

from glassmoon.chatgpt_auth import PLAN_SCOPE, Profile, RESOURCE
from glassmoon.ipc import RequestError
from glassmoon.models import Transcripts
from glassmoon.openai_service import OpenAI

MODEL = {
    "id": "example",
    "input_modalities": ["text", "image"],
    "output_modalities": ["text"],
    "context_tokens": 100_000,
    "streaming": True,
}


def sse(*events):
    return "".join(f"data: {json.dumps(event)}\n\n" for event in events).encode()


class OpenAITests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.profile = Profile(Path(directory.name) / "profile.json")
        with self.profile.locked():
            self.profile.write({"access_token": "fake", "expires_at": 9_999_999_999})
        self.transcripts = Transcripts(Path(directory.name) / "state.db")
        self.addCleanup(self.transcripts.close)
        self.requests = []
        self.terminal = {"type": "response.completed", "response": {"usage": {"input_tokens": 5}}}
        self.chunks = []
        client = httpx.AsyncClient(transport=httpx.MockTransport(self.upstream))
        self.addAsyncCleanup(client.aclose)
        self.service = OpenAI(self.profile, client, [MODEL], self.transcripts)

    def upstream(self, request):
        self.assertEqual(str(request.url), f"{RESOURCE}/responses")
        self.assertEqual(request.headers["Authorization"], "Bearer fake")
        self.requests.append(json.loads(request.content))
        delta = {"type": "response.output_text.delta", "delta": "Hello"}
        return httpx.Response(200, content=sse(delta, self.terminal))

    async def emit(self, chunk):
        self.chunks.append(chunk)

    async def call(self, name, params, stream=False):
        return await self.service.handle(name, params, self.emit, stream)

    async def test_conversation_streaming_and_buffered(self):
        self.assertEqual((await self.call("models", {}))["models"], [MODEL])
        identifier = (await self.call("start", {"model": "example"}))["conversation"]
        await self.call("add", {"conversation": identifier, "message": {
            "role": "user", "content": [{"type": "image", "url": "https://example.org/image.png"}],
        }})
        first = await self.call("respond", {"conversation": identifier}, stream=True)
        self.assertEqual(self.chunks, [{"type": "text", "text": "Hello"}])
        self.assertEqual(first["message"]["content"], self.chunks)
        second = await self.call("respond", {"conversation": identifier})
        self.assertEqual(second["revision"], 3)
        self.assertEqual(len(self.chunks), 1)
        self.assertEqual(self.requests[1]["input"][0]["content"][0]["type"], "input_image")
        self.assertEqual(self.requests[1]["input"][1]["content"][0]["type"], "output_text")
        self.assertTrue(self.requests[1]["stream"])
        self.assertFalse(self.requests[1]["store"])

    async def test_failed_stream_keeps_only_committed_messages(self):
        identifier = (await self.call("start", {"model": "example"}))["conversation"]
        await self.call("add", {"conversation": identifier, "message": {"role": "user", "content": "Hi"}})
        self.terminal = {"type": "response.failed", "response": {
            "error": {"code": "subscription_sharing_usage_limit_exceeded"},
        }}
        with self.assertRaises(RequestError) as caught:
            await self.call("respond", {"conversation": identifier}, stream=True)
        self.assertEqual(caught.exception.code, "subscription_sharing_usage_limit_exceeded")
        self.assertEqual(self.transcripts.history(identifier)["revision"], 1)

    async def test_refresh_replaces_rotating_tokens(self):
        with self.profile.locked():
            self.profile.write({
                "access_token": "old", "expires_at": 0, "scope": PLAN_SCOPE,
                "client_id": "issued", "refresh_token": "old-refresh",
            })
        with patch("glassmoon.chatgpt_auth.token_request", return_value={
            "access_token": "new", "refresh_token": "new-refresh", "expires_in": 3600,
        }) as refresh:
            self.assertEqual(self.profile.access_token(), "new")
            self.assertEqual(self.profile.access_token(), "new")
        refresh.assert_called_once_with(
            grant_type="refresh_token", client_id="issued", refresh_token="old-refresh",
        )
        self.assertEqual(self.profile.read()["refresh_token"], "new-refresh")
