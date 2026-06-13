# Feishu Dispatcher Local Callback Implementation Detail

This document summarizes the technical changes, architecture, and verification results of integrating a local HTTP callback path to route Feishu message events from a WebSocket dispatcher directly to the internal Glean API.

---

## 1. Core Architecture

The inbound message callback architecture runs as follows:

```text
Feishu Open Platform (Message Event)
   ──[WebSocket Long Connection]──>
feishu-dispatcher (Running on host / in background)
   ──[POST /api/internal/feishu/messages (HTTP Bearer Authorized)]──>
Glean Backend (FastAPI API Container)
   ──[FeishuDigestEventService processing & database resolution]──>
daily_news_reply_*.txt (Atomic file written to shared outbox volume)
   ──[File System Scan]──>
feishu-dispatcher (outbox scanning scan loop)
   ──[Feishu Send Message API Call]──>
#news Group Chat
```

---

## 2. Code Modifications

All changes have been implemented and verified. Below is the list of modifications grouped by project.

### A. Glean Project (`glean`)

1. **Pydantic Schemas**:
   Created [feishu.py](file:///workspace/glean/backend/packages/core/glean_core/schemas/feishu.py) defining:
   - `FeishuDigestSender`: Wrapper for `open_id` and `user_id`.
   - `FeishuDigestMessage`: Normalized payload mapping incoming dispatcher message fields. Includes `@property` getter helpers for open/user IDs.
   - `FeishuDigestProcessResult`: Execution output wrapper containing resolution status, short code, entry UUID, output text, and outbox file path.
   - Exported these schemas in [schemas/\_\_init\_\_.py](file:///workspace/glean/backend/packages/core/glean_core/schemas/__init__.py).

2. **Core Service Extraction**:
   Implemented `FeishuDigestEventService` in [feishu_digest_event_service.py](file:///workspace/glean/backend/packages/core/glean_core/services/feishu_digest_event_service.py):
   - Handles Redis-based message ID deduplication (key: `feishu:processed_msg:{message_id}`, TTL: 10 minutes).
   - Validates that incoming events belong to the configured `news_chat_id`.
   - Checks the sender against allowed user ID constraints (if any).
   - Verifies whether the bot is mentioned when `require_mention` is active.
   - Normalizes and parses current code text (e.g. `n01`, case-insensitive).
   - Supports date-prefixed historical lookup with `MMDD+nXX` forms, including `0612n01`, `0612 n01`, `0612-n01`, and `0612_n01`.
   - Finds the latest `DigestRun` in `sent` or `partial_failed` status for bare codes, or the matching local digest date for date-prefixed codes.
   - Retrieves the matching `DigestItem` and lazily translates fulltext via `ArticleLanguageService`.
   - Generates the reply files atomically using a temporary file prefix inside the same folder and renaming it to the final target to avoid dispatcher file scanner race conditions.
   - Marks successfully fetched Feishu articles as liked in Glean and enqueues the normal `update_user_preference` job when the like state changes.
   - Exported this service in [services/\_\_init\_\_.py](file:///workspace/glean/backend/packages/core/glean_core/services/__init__.py).

3. **FastAPI Endpoints**:
   Modified [routers/feishu.py](file:///workspace/glean/backend/apps/api/glean_api/routers/feishu.py):
   - Refactored `handle_feishu_events` (`POST /api/integrations/feishu/events`) to parse webhook callbacks, construct a `FeishuDigestMessage`, and delegate processing to the service layer with direct HTTP response replies.
   - Created a separate `internal_router` exposing `POST /api/internal/feishu/messages`. This route performs secure HTTP Bearer token validation and calls the service layer with `write_to_outbox=True`.
   - Registered the `internal_router` under prefix `/api/internal/feishu` in [main.py](file:///workspace/glean/backend/apps/api/glean_api/main.py).

4. **Environment Settings & Volume Mounts**:
   - Added configuration fields `feishu_dispatcher_outbox_dir` and `feishu_dispatcher_callback_token` in [config.py](file:///workspace/glean/backend/apps/api/glean_api/config.py).
   - Defined default overrides in [.env](file:///workspace/glean/.env).
   - Mounted the host directory path into the container volumes block inside [docker-compose.yml](file:///workspace/glean/docker-compose.yml):
     ```yaml
     - ${FEISHU_DISPATCHER_HOST_OUTBOX_DIR:-/mnt/workspace/ocworkspace/feishu_outbox}:/workspace/ocworkspace/feishu_outbox
     ```
   - Passed `FEISHU_DISPATCHER_OUTBOX_DIR` and `FEISHU_DISPATCHER_CALLBACK_TOKEN` into the backend container environment.

5. **Review Fixes Applied**:
   - Corrected the compose bind-mount host path so the container writes into the actual dispatcher outbox on this host.
   - Added `.env.example` entries for the dispatcher host outbox path, container outbox path, and callback token.
   - Sanitized dynamic `message_id` and code filename segments before writing `daily_news_reply_*.txt`.
   - Preserved original exception tracebacks when atomic outbox writes fail.
   - Added historical code lookup so today's articles can still use `n01`, while older digests can use an `MMDD` prefix.
   - Added liked-state persistence for articles retrieved through Feishu.

### B. Feishu Dispatcher Project (`ocworkspace`)

1. **Dependency & Callback Config**:
   - Created [requirements.txt](file:///workspace/ocworkspace/feishu-dispatcher/requirements.txt) referencing `lark-oapi` and `requests`.
   - Created callback configuration in [feishu_callbacks.json](file:///workspace/ocworkspace/feishu-dispatcher/feishu_callbacks.json) detailing target callback URLs, matching criteria, and token keys.

2. **WebSocket Listener**:
   Modified [feishu_dispatcher.py](file:///workspace/ocworkspace/feishu-dispatcher/feishu_dispatcher.py):
   - Added a command-line check for `--listen`.
   - Implemented `start_websocket_listener()` to load the app credentials and start a persistent Lark OAPI WebSocket long connection.
   - Parses the event and checks rules against configured paths in `feishu_callbacks.json`.
   - Sends authorized POST requests containing normalized payloads to the local Glean endpoint.

---

## 3. Verification Details

### Automated Test Cases Added
New integration tests were added in [test_feishu_api.py](file:///workspace/glean/backend/tests/integration/test_feishu_api.py):
- `test_internal_feishu_callback_unauthorized`: Verifies that endpoints reject unauthorized or missing bearer token headers with `401` or `403`.
- `test_internal_feishu_callback_success`: Verifies successful processing of internal callback events, ensuring the lazy translation is committed and the outbox file is created atomically containing correct article body text.
- `test_internal_feishu_callback_deduplicated`: Verifies that duplicates return `"deduplicated"` status immediately and do not generate extra outbox files.
- `test_internal_feishu_callback_date_prefixed_code_selects_historical_run`: Verifies that `MMDD+nXX` resolves a previous digest while bare `nXX` still resolves the latest digest.

### Test Verification
The full backend test suite was run and passed completely:
```bash
TEST_DATABASE_URL="postgresql+asyncpg://glean:devpassword@localhost:5433/glean_test" uv run pytest
...
========================= 339 passed, 2 skipped, 1 warning in 74.28s (0:01:14) =============
```

---

## 4. Run Instructions

### Start Glean Dev Stack
Start the containers normally:
```bash
docker compose up -d
```

### Start Dispatcher WebSocket Listener
Export your token and launch the listener process using the virtual environment:
```bash
export GLEANDISPATCH_CALLBACK_TOKEN=a_very_long_secure_callback_token_here
./venv/bin/python feishu-dispatcher/feishu_dispatcher.py --listen
```
Once connected, any `@Glean nXX` message in the news group will trigger an immediate backend resolution callback. Running `python feishu_dispatcher.py --no-wait` afterwards will deliver the generated reply text file to the Feishu news chat.
