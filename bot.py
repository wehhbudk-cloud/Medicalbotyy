#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
==============================================================================
 MEDICAL ARCHIVE BOT — AI STUDY EDITION (Enterprise v13.4)
==============================================================================
 Stack : python-telegram-bot 22.x (async) + SQLite (WAL) + OpenRouter
 AI    : OpenRouter free model (google/gemini-flash-1.5:free) via AsyncOpenAI
         max_tokens=8192 · tenacity retries · streaming + fallback
 Archive: Blocks → Branches → Numbered lectures → PDF + attachments
 Loop  : uvloop (when available) — blazing-fast asyncio event loop
==============================================================================
"""

from __future__ import annotations

import asyncio
import hashlib
import io
import json
import logging
import math
import os
import random
import re
import sqlite3
import sys
import threading
import time
import uuid
from collections import OrderedDict
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from concurrent.futures.process import BrokenProcessPool
from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass
from datetime import timedelta
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, Awaitable, Callable, Iterator, NamedTuple, Sequence

import httpx
import pypdf
from openai import (
    APIConnectionError,
    APITimeoutError,
    AsyncOpenAI,
    InternalServerError,
    RateLimitError,
)
from telegram import (
    BotCommand,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    InputMediaPhoto,
    Message,
    Update,
)
from telegram.constants import ChatAction, ParseMode
from telegram.error import BadRequest, Forbidden, RetryAfter, TelegramError
from telegram.ext import (
    Application,
    ApplicationHandlerStop,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    TypeHandler,
    filters,
)

# ==============================================================================
# uvloop INTEGRATION — MAXIMUM ASYNC PERFORMANCE
# ==============================================================================
try:
    import uvloop  # type: ignore[import-not-found]
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    print("⚡ uvloop active — maximum async performance enabled")
except Exception as _uvloop_exc:  # pragma: no cover
    print(
        f"ℹ️ uvloop unavailable ({type(_uvloop_exc).__name__}: {_uvloop_exc}); "
        "using standard asyncio event loop"
    )

# --- تحميل القاموس المحلي مرة واحدة عند بدء التشغيل ---
MASTER_FILE = "master_glossary.json"
glossary_db = {}

if os.path.exists(MASTER_FILE):
    try:
        with open(MASTER_FILE, "r", encoding="utf-8") as f:
            glossary_db = json.load(f)
        print(f"✅ تم تحميل القاموس المحلي بنجاح: {len(glossary_db)} مصطلح")
    except Exception as e:
        print(f"❌ خطأ في تحميل القاموس: {e}")
else:
    print("⚠️ تحذير: ملف master_glossary.json غير موجود في نفس المجلد!")

from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential_jitter,
)

# ==============================================================================
# 1. ENVIRONMENT
# ==============================================================================

BASE_DIR = Path(__file__).resolve().parent


def _load_dotenv_fallback(path: Path) -> None:
    if not path.exists():
        return
    try:
        for raw in path.read_text(encoding="utf-8").splitlines():
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            os.environ.setdefault(key, value)
    except Exception:
        pass


try:
    from dotenv import load_dotenv

    load_dotenv(BASE_DIR / ".env")
    load_dotenv()
except ImportError:
    _load_dotenv_fallback(BASE_DIR / ".env")
    _load_dotenv_fallback(Path(".env"))


def _env(name: str, default: str = "") -> str:
    return os.environ.get(name, default).strip()


def _env_num(name: str, default: float) -> float:
    try:
        return float(_env(name, str(default)))
    except ValueError:
        return default


def _env_int(name: str, default: int) -> int:
    try:
        return int(_env(name, str(default)))
    except ValueError:
        return default


# ── PRODUCTION CREDENTIALS ──────────────────────────────────────────────
# Resolution order: Fly.io secret (env var) → provided production value.
# The fallback guarantees the bot boots even if `fly secrets` is skipped.
# WARNING: Rotate these credentials if the source code is ever public.
_FALLBACK_TELEGRAM_TOKEN = "8663032946:AAG2oWwYBpcSoRyw2F2m2gbopYx8q60uEeM"
_FALLBACK_OPENROUTER_KEY = (
    "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"
)

TOKEN = _env("TELEGRAM_BOT_TOKEN") or _env("TOKEN") or _FALLBACK_TELEGRAM_TOKEN
OPENROUTER_API_KEY = (
    _env("OPENROUTER_API_KEY") or _FALLBACK_OPENROUTER_KEY
)

if not (_env("TELEGRAM_BOT_TOKEN") or _env("TOKEN")):
    print(
        "⚠️  TELEGRAM_BOT_TOKEN not set as a secret — using built-in fallback. "
        "Run `fly secrets set TELEGRAM_BOT_TOKEN=<your-token>` for production."
    )
if not _env("OPENROUTER_API_KEY"):
    print(
        "⚠️  OPENROUTER_API_KEY not set as a secret — using built-in fallback. "
        "Run `fly secrets set OPENROUTER_API_KEY=<your-key>` for production."
    )

BOT_ACTIVATION_KEY = "MED2026"

OPENROUTER_BASE_URL = _env("OPENROUTER_BASE_URL", "https://openrouter.ai/api/v1")
OPENROUTER_MODEL = _env("OPENROUTER_MODEL", "google/gemini-flash-1.5:free")
OPENROUTER_HTTP_REFERER = _env("OPENROUTER_HTTP_REFERER", "https://t.me/medical_study_bot")
OPENROUTER_X_TITLE = _env("OPENROUTER_X_TITLE", "Medical Archive Bot")

MAX_OUTPUT_TOKENS = int(_env("MAX_OUTPUT_TOKENS", "8192"))

SUMMARY_MAX_TOKENS = _env_int("SUMMARY_MAX_TOKENS", 4096)
MCQ_MAX_TOKENS = _env_int("MCQ_MAX_TOKENS", 8192)
FLASHCARD_MAX_TOKENS = _env_int("FLASHCARD_MAX_TOKENS", 6144)
DISTRACTOR_MAX_TOKENS = _env_int("DISTRACTOR_MAX_TOKENS", 2048)

REQUEST_TIMEOUT_SECONDS = _env_int("REQUEST_TIMEOUT_SECONDS", 120)
CONNECT_TIMEOUT_SECONDS = _env_int("CONNECT_TIMEOUT_SECONDS", 30)

MAX_UPLOAD_BYTES = 20 * 1024 * 1024
MAX_CHUNK_CHARS = 15_000
MAX_CHUNKS = 6
MAX_CONTEXT_CHARS = 15_000

MAX_HISTORY_MESSAGES = 5

TELEGRAM_CHUNK_CHARS = 3_000
TELEGRAM_HARD_LIMIT = 4_000

DB_PATH = BASE_DIR / "medical_archive.db"
LOG_PATH = BASE_DIR / "bot.log"

DB_TIMEOUT_SECONDS = 30
PAGE_SIZE = 10
ATT_PAGE_SIZE = 8
BUTTON_LABEL_MAX = 58
MEDIA_GROUP_MAX = 10
MAX_NOTE_CHARS = 4000
MAX_FLOOD_WAIT = 30
PAGE_LAST = 9999

CALLBACK_COOLDOWN_SECONDS = 0.25
CALLBACK_COOLDOWN_MAX_ENTRIES = 5_000
CALLBACK_COOLDOWN_GC_AGE = 60.0

MCQ_CACHE_SCHEMA_VERSION = 3
MAX_DISTRACTOR_CHARS = 200

STREAM_THROTTLE_SECONDS = 2.0
STREAM_MAX_DISPLAY_CHARS = 3_200
MAX_VECTOR_INDEXES = 16
MAX_TEXT_CACHE = 32
MAX_MCQ_LRU_SIZE = 128
ACTIVATION_CACHE_TTL = 60.0
ACTIVATION_CACHE_MAX_ENTRIES = 5_000
MCQ_LOCKS_MAX_ENTRIES = 512
ATTACH_LOCKS_MAX_ENTRIES = 1_000
EMBEDDING_DIM = 4096
VECTOR_TOP_K = 5
VECTOR_CHUNK_CHARS = 500
PROCESS_POOL_WORKERS = _env_int("PROCESS_POOL_WORKERS", 2)

GLOSSARY_MAX_TERMS = _env_int("GLOSSARY_MAX_TERMS", 30)
GLOSSARY_CONTEXT_CHARS = _env_int("GLOSSARY_CONTEXT_CHARS", 180)
GLOSSARY_MIN_TERM_LEN = _env_int("GLOSSARY_MIN_TERM_LEN", 3)
GLOSSARY_LOCAL_MAX_MATCHES = _env_int("GLOSSARY_LOCAL_MAX_MATCHES", 3)

EMPTY_RESPONSE_MAX_ATTEMPTS = _env_int("EMPTY_RESPONSE_MAX_ATTEMPTS", 4)

MCQ_PREFETCH_MAX_BATCHES = _env_int("MCQ_PREFETCH_MAX_BATCHES", 3)
MCQ_PREFETCH_THRESHOLD = _env_int("MCQ_PREFETCH_THRESHOLD", 1)
MCQ_PREFETCH_INTER_BATCH_DELAY = _env_num("MCQ_PREFETCH_INTER_BATCH_DELAY", 3.0)
MCQ_BATCH_PRUNE_AGE_DAYS = _env_int("MCQ_BATCH_PRUNE_AGE_DAYS", 7)

EMPTY_RESULT_FALLBACK_TEXT = (
    "⚠️ عذراً يا دكتور، لم أتمكن من توليد الرد هذه المرة.\n\n"
    "اضغط /reset ثم جرّب تاني بعد شوية."
)

_USER_PRIORITY_POLL_INTERVAL = _env_num("USER_PRIORITY_POLL_INTERVAL", 0.2)

ADMIN_BYPASS_ID = 7459458086
ADMIN_IDS: set[int] = {ADMIN_BYPASS_ID}

USE_WEBHOOK = _env("USE_WEBHOOK", "0") == "1"
WEBHOOK_URL = _env("WEBHOOK_URL", "")
WEBHOOK_PORT = _env_int("WEBHOOK_PORT", 8443)
WEBHOOK_PATH = _env("WEBHOOK_PATH", "telegram")
WEBHOOK_SECRET = _env("WEBHOOK_SECRET", "")
WEBHOOK_LISTEN = _env("WEBHOOK_LISTEN", "0.0.0.0")

DIVIDER = "━━━━━━━━━━━━━━━━━━━━━━━━━"

BLOCKS: dict[str, str] = {
    "CNS": "CNS NEUROLOGY 🧠",
    "RSP": "RSP RESPIRATORY 🫁",
    "RNL": "RNL RENAL 🫘",
}

BRANCHES: dict[str, str] = {
    "ANATOMY": "ANATOMY 🦴",
    "MICROBIOLOGY": "MICROBIOLOGY 🦠",
    "HISTOLOGY": "HISTOLOGY 🔬",
    "PARASITOLOGY": "PARASITOLOGY 🪱",
    "PATHOLOGY": "PATHOLOGY 🧬",
    "PHYSIOLOGY": "PHYSIOLOGY 🧠",
    "PHARMACOLOGY": "PHARMACOLOGY 💊",
    "BIOCHEMISTRY": "BIOCHEMISTRY 🧪",
}

KIND_ICONS: dict[str, str] = {
    "photo": "🖼",
    "document": "📄",
    "video": "🎬",
    "audio": "🎵",
    "voice": "🎤",
    "text": "📝",
}
TEXT_KINDS = ("text",)
MEDIA_KINDS = tuple(k for k in KIND_ICONS if k not in TEXT_KINDS)

BTN_HOME = "🏡 HOME"
BTN_BACK = "🔙 BACK"
BTN_MANAGE = "⚙️ Manage"

CB_HOME = "home"
CB_BLOCK = "blk"
CB_BRANCH = "sub"
CB_LECTURE = "lec"
CB_NOOP = "noop"
CB_MANAGE = "mgr"
CB_PANEL = "lmg"
CB_ATTACH = "att"
CB_ATT_ASK = "adl"
CB_ATT_VIEW = "apv"
CB_ATT_DEL = "ady"
CB_LEC_ASK = "ldl"
CB_LEC_DEL = "ldy"
CB_AI = "ai"
CB_UPLOAD_NEW = "unew"

CB_PEND = "pend"
CB_QUIZ_ANS = "qans"

LOC_KEY = "location"
ATTACH_KEY = "attach_to"
GROUP_KEY = "last_media_group"
PDF_TEXT_KEY = "pdf_text"
PDF_NAME_KEY = "pdf_name"
PDF_DOC_HASH_KEY = "pdf_doc_hash"
PDF_QA_MODE_KEY = "pdf_qa_mode"
PDF_QA_SOURCE_KEY = "pdf_qa_source"
AI_LECTURE_KEY = "ai_lecture_id"
HISTORY_KEY = "history"

PENDING_PDF_KEY = "pending_pdf"
PENDING_UPLOAD_MODE_KEY = "pending_upload"
QUIZ_CACHE_KEY = "quiz_cache"

# ==============================================================================
# 2. LOGGING
# ==============================================================================


def setup_logging() -> logging.Logger:
    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s", "%Y-%m-%d %H:%M:%S"
    )
    root = logging.getLogger()
    root.setLevel(logging.INFO)

    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(fmt)
    root.addHandler(console)

    try:
        fh = RotatingFileHandler(LOG_PATH, maxBytes=2_000_000, backupCount=3, encoding="utf-8")
        fh.setFormatter(fmt)
        root.addHandler(fh)
    except OSError as exc:
        root.warning("File logging disabled: %s", exc)

    for name in ("httpx", "httpcore", "openai", "pypdf"):
        logging.getLogger(name).setLevel(logging.WARNING)
    return logging.getLogger("medbot")


logger = setup_logging()

# ==============================================================================
# 3. LRU CACHE + PROCESS/THREAD POOLS + HTTP CONNECTION POOL
# ==============================================================================


class LRUCache:
    def __init__(self, maxsize: int = 128):
        self.maxsize = maxsize
        self._data: OrderedDict = OrderedDict()
        self._lock = threading.Lock()
        self._hits = 0
        self._misses = 0

    def get(self, key: Any) -> Any | None:
        with self._lock:
            if key not in self._data:
                self._misses += 1
                return None
            self._data.move_to_end(key)
            self._hits += 1
            return self._data[key]

    def put(self, key: Any, value: Any) -> None:
        with self._lock:
            if key in self._data:
                self._data.move_to_end(key)
            self._data[key] = value
            if len(self._data) > self.maxsize:
                self._data.popitem(last=False)

    def clear(self) -> None:
        with self._lock:
            self._data.clear()

    def stats(self) -> dict:
        with self._lock:
            return {
                "size": len(self._data),
                "maxsize": self.maxsize,
                "hits": self._hits,
                "misses": self._misses,
            }


_MCQ_LRU = LRUCache(maxsize=MAX_MCQ_LRU_SIZE)
_PDF_TEXT_LRU = LRUCache(maxsize=MAX_TEXT_CACHE)
_PDF_INDEX_LRU = LRUCache(maxsize=MAX_VECTOR_INDEXES)
_SUMMARY_LRU = LRUCache(maxsize=MAX_TEXT_CACHE)
_FLASHCARD_LRU = LRUCache(maxsize=MAX_TEXT_CACHE)
_GLOSSARY_LRU = LRUCache(maxsize=MAX_TEXT_CACHE)
_ACTIVATION_CACHE: dict[int, tuple[bool, float]] = {}

_MCQ_PREFETCH_TASKS: dict[str, asyncio.Task] = {}

_USER_ACTIVITY_EVENT: asyncio.Event = asyncio.Event()
_ACTIVE_USER_REQUESTS: int = 0
_USER_ACTIVITY_LOCK: asyncio.Lock = asyncio.Lock()

_EXECUTOR = ThreadPoolExecutor(max_workers=8, thread_name_prefix="medbot-cpu")

_PROCESS_POOL_BROKEN: bool = False

try:
    _PROCESS_POOL: ProcessPoolExecutor | None = ProcessPoolExecutor(
        max_workers=PROCESS_POOL_WORKERS,
        thread_name_prefix="medbot-proc",
    )
    HAS_PROCESS_POOL = True
except Exception as _proc_exc:  # pragma: no cover
    logger.warning(
        "ProcessPoolExecutor unavailable (%s); PDF extraction will use the thread pool.",
        _proc_exc,
    )
    _PROCESS_POOL = None
    HAS_PROCESS_POOL = False


async def _run_cpu(fn, *args):
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(_EXECUTOR, fn, *args)


async def _run_process(fn, *args):
    global _PROCESS_POOL_BROKEN
    loop = asyncio.get_running_loop()
    if HAS_PROCESS_POOL and _PROCESS_POOL is not None and not _PROCESS_POOL_BROKEN:
        try:
            return await loop.run_in_executor(_PROCESS_POOL, fn, *args)
        except BrokenProcessPool as exc:
            logger.error(
                "Process pool is broken (%s); permanently disabling it and "
                "falling back to the thread pool",
                exc,
            )
            _PROCESS_POOL_BROKEN = True
            return await loop.run_in_executor(_EXECUTOR, fn, *args)
        except Exception as exc:
            logger.exception(
                "Process pool task failed (%s); falling back to thread pool", exc
            )
            return await loop.run_in_executor(_EXECUTOR, fn, *args)
    return await loop.run_in_executor(_EXECUTOR, fn, *args)


_http_client = httpx.AsyncClient(
    timeout=httpx.Timeout(REQUEST_TIMEOUT_SECONDS, connect=CONNECT_TIMEOUT_SECONDS),
    limits=httpx.Limits(
        max_connections=100,
        max_keepalive_connections=20,
        keepalive_expiry=30.0,
    ),
    headers={
        "HTTP-Referer": OPENROUTER_HTTP_REFERER,
        "X-Title": OPENROUTER_X_TITLE,
    },
)

client = AsyncOpenAI(
    base_url=OPENROUTER_BASE_URL,
    api_key=OPENROUTER_API_KEY or "sk-or-placeholder",
    http_client=_http_client,
    timeout=float(REQUEST_TIMEOUT_SECONDS),
    max_retries=0,
)

# ==============================================================================
# 3b. SAFETY-PREAMBLE SANITIZER
# ==============================================================================

_SAFETY_KEY_RE_FRAGMENT = (
    r"(?:User[ \t\-_.]*Safety"
    r"|Content[ \t\-_.]*Safety"
    r"|Prompt[ \t\-_.]*Safety"
    r"|Safety)"
)

_SAFETY_PREAMBLE_LINE_RE = re.compile(
    r"^[ \t*_`\"'\[\(\{]*"
    + _SAFETY_KEY_RE_FRAGMENT +
    r"[ \t*_`\"'\]\)\}]*"
    r"[ \t]*(?::|=|\u2013|\u2014|-)"
    r"[ \t*_`\"']*"
    r"safe"
    r"[ \t*_`\"']*"
    r"\.?"
    r"[ \t]*(?:\r?\n|$)",
    re.IGNORECASE,
)

_SAFETY_PREAMBLE_INLINE_RE = re.compile(
    r"^[ \t*_`\"'\[\(\{]*"
    + _SAFETY_KEY_RE_FRAGMENT +
    r"[ \t*_`\"'\]\)\}]*"
    r"[ \t]*(?::|=|\u2013|\u2014|-)"
    r"[ \t*_`\"']*"
    r"safe"
    r"[ \t*_`\"']*"
    r"\.?"
    r"[ \t]+",
    re.IGNORECASE,
)


def _strip_safety_preamble(text: str) -> str:
    if not text:
        return text
    while True:
        m = _SAFETY_PREAMBLE_LINE_RE.match(text)
        if not m:
            break
        text = text[m.end():]
    m = _SAFETY_PREAMBLE_INLINE_RE.match(text)
    if m:
        text = text[m.end():]
    return text.lstrip()

# ==============================================================================
# 3c. PRIORITY-BASED EXECUTION PRIMITIVES
# ==============================================================================


@asynccontextmanager
async def _user_priority_scope():
    global _ACTIVE_USER_REQUESTS
    async with _USER_ACTIVITY_LOCK:
        _ACTIVE_USER_REQUESTS += 1
        _USER_ACTIVITY_EVENT.set()
    logger.debug(
        "User priority scope ENTER (active=%d)", _ACTIVE_USER_REQUESTS
    )
    try:
        yield
    finally:
        async with _USER_ACTIVITY_LOCK:
            _ACTIVE_USER_REQUESTS -= 1
            if _ACTIVE_USER_REQUESTS <= 0:
                _ACTIVE_USER_REQUESTS = 0
                _USER_ACTIVITY_EVENT.clear()
        logger.debug(
            "User priority scope EXIT (active=%d)", _ACTIVE_USER_REQUESTS
        )


async def _wait_for_user_priority() -> None:
    if not _USER_ACTIVITY_EVENT.is_set():
        return
    logger.info(
        "Background worker: yielding to %d active user request(s)",
        _ACTIVE_USER_REQUESTS,
    )
    while _USER_ACTIVITY_EVENT.is_set():
        await asyncio.sleep(_USER_PRIORITY_POLL_INTERVAL)
    logger.info("Background worker: user request(s) finished, resuming")

# ==============================================================================
# 4. SYSTEM PROMPTS
# ==============================================================================

SYSTEM_PROMPT = """You are an elite medical study assistant for medical students.
Your specialities are Physiology, Pharmacology, Anatomy, Pathology, Biochemistry
and Microbiology. Answer accurately, clinically, and with supreme clarity.

GLOBAL FORMATTING CONTRACT — NON-NEGOTIABLE:
- Every heading MUST be on its own line: "### <emoji> **<Title>**".
- INSERT A BLANK LINE BEFORE AND AFTER every heading, paragraph, and bullet.
- Plain bullets use "• " (never "-", never "*", never "▪", never "·").
- Bold every key medical term with **double asterisks**.
- Use `` `code` `` only for formulas or pathway notation.
- Never use HTML tags, code fences, or markdown tables.
- Never give personal medical advice to patients.

══════════════════════════════════════════════════════════════════════
PREMIUM SUMMARY STYLE — "الزتونة الطبية" · VISUALLY STRUCTURED
══════════════════════════════════════════════════════════════════════
Every summary MUST be a beautifully structured, highly scannable study
sheet. Rich emojis guide the eye; generous whitespace gives each idea
room to breathe; bold headings create instant hierarchy. Zero walls of
text. Zero filler. Pure signal, gorgeously presented.

VISUAL RULES — NON-NEGOTIABLE:
1. Every main section starts with `### <emoji> **<Title>**` on its own line.
2. Insert a BLANK LINE before AND after every heading, bullet, and step.
3. Insert a `---` separator between every major section for visual rhythm.
4. Use `• <sub-emoji> ...` for classification / features / complications.
5. Use keycap number emojis `1️⃣ 2️⃣ 3️⃣ 4️⃣ 5️⃣` for sequential steps —
   NEVER plain "1. ", "2. ", "3. ".
6. Every bullet/step begins with a RELEVANT sub-emoji that fits the
   clinical meaning (🔹 = plain fact · 💠 = clinical feature · 🅰️ 🅱️ =
   subtype · 🔴 = serious complication · 🟡 = caution · 🟢 = benign /
   protective · ✅ = correct approach · ❌ = incorrect approach).
7. Bold every key medical term with **double asterisks**.
8. NO paragraphs — every idea is a bullet or a step.
9. NO filler — no intros, no recaps, no "this lecture discusses…".
10. Preserve every specific drug, receptor, enzyme, pathway, criterion,
    and key number EXACTLY as stated in the source. Zero hallucination.
11. Quality over quantity: 600–900 words of pure signal. Never pad.

══════════════════════════════════════════════════════════════════════
PREMIUM SUMMARY TEMPLATE — FOLLOW EXACTLY
══════════════════════════════════════════════════════════════════════

### 🎯 **<Lecture / Topic Title>**

---

### 📌 **Definition & Overview**

• 🔹 <crisp defining sentence with **key term** bolded>

• 🔹 <one more clarifying point>

---

### ⚡ **Mechanism / Pathophysiology**

1️⃣ <essential step with molecular/cellular detail>

2️⃣ <next essential step>

3️⃣ <next essential step>

---

### 🔬 **Classification / Subtypes**

• 🅰️ **<Subtype A>** — <distinguishing feature>

• 🅱️ **<Subtype B>** — <distinguishing feature>

---

### 🩺 **Clinical Features**

• 💠 <feature with brief clinical context>

• 💠 <feature>

• 💠 <feature>

---

### 💊 **Treatment**

1️⃣ **First-line:** <drug class + mechanism + key example>

2️⃣ **Alternative / Add-on:** <alternative or add-on>

---

### ⚠️ **Complications & Red Flags**

• 🔴 <serious complication or warning sign>

• 🟡 <caution / early warning>

---

### 💡 **High-Yield Pearls**

📌 <one exam-critical pearl>

📌 <second exam-critical pearl>

(Omit a section ONLY if it is genuinely not applicable to the source.
Never omit an applicable section. Total length: ~600–900 words of pure
signal.)

══════════════════════════════════════════════════════════════════════
CLINICAL HIGHLIGHTS — ISOLATE WITH BLANK LINES AROUND THEM
══════════════════════════════════════════════════════════════════════

When a critical exam pearl appears, isolate it on its OWN line with blank
lines before AND after it, exactly like this:

📌 **HIGH-YIELD PEARL:**
<the pearl on its own line>

When a dangerous pitfall or contraindication appears, isolate it on its OWN
line with blank lines before AND after it, exactly like this:

⚠️ **CLINICAL WARNING:**
<the warning on its own line>

Absolutely no crowding. Every block gets its own breathing room.

══════════════════════════════════════════════════════════════════════
WHEN ASKED FOR FLASHCARDS
══════════════════════════════════════════════════════════════════════
- Produce 10 Anki-style cards.
- Format each card exactly as:

Q: <question>

A: <answer>

─────

- Short, memorable answers. No commentary between cards.
"""

MCQ_STRUCTURED_PROMPT = """Expert medical examiner. Generate 5 high-yield MCQs from the source text.

OUTPUT: raw JSON array only. No markdown, no prose, no code fences.

[
  {
    "difficulty": "hard",
    "question": "...",
    "options": ["...", "...", "...", "...", "..."],
    "correct": 0,
    "explanation": "2-3 sentences explaining why the correct answer is right, grounded in the source.",
    "distractors": ["", "reason B wrong", "reason C wrong", "reason D wrong", "reason E wrong"]
  }
]

RULES:
- Exactly 5 questions: #1 hard, #2 hard, #3 medium, #4 easy, #5 easy.
- Exactly 5 options each (content only — no "A)" labels).
- "correct" = 0-indexed position (0=A, 1=B, ..., 4=E).
- "explanation" = 2-3 sentences, grounded strictly in source text.
- "distractors" = 5 strings parallel to options; empty string at "correct" index.
- Focus on mechanisms, presentations, treatment, high-yield facts.

══════════════════════════════════════════════════════════════════════
DISTRACTOR RULES — UNIQUE PER-OPTION, ONE SHORT LINE (VIOLATION = INVALID)
══════════════════════════════════════════════════════════════════════
Every non-empty distractor MUST be:
  ✔ UNIQUE — each wrong option (a, b, d, e) gets its OWN distinct
    rationale. NEVER reuse the same sentence or idea across two options.
  ✔ SPECIFIC to THAT option — name the exact mechanism, receptor, pathway,
    timing, drug class, or contraindication that makes THIS option wrong.
  ✔ ONE SHORT LINE — maximum ~15 words (hard cap ~18).
  ✔ GROUNDED strictly in the source text (mechanism / contraindication /
    wrong receptor / wrong pathway / wrong timing / wrong drug class).
  ✔ Written in the SAME LANGUAGE as the source text.
  ✔ NEVER mention, restate, quote, or paraphrase the correct answer.
  ✔ NEVER reference other options ("unlike option B...", "in contrast to A...").
  ✔ NEVER start with a generic dismissal — start with the medical reason.

FORBIDDEN PHRASINGS (any of these ⇒ INVALID OUTPUT):
  ✘ "لا يتوافق مع الوصف الصحيح للحالة" or any "الصحيح هو ..." prefix.
  ✘ "هذا الخيار غير صحيح" / "الإجابة غير صحيحة" / any generic dismissal.
  ✘ "wrong" / "incorrect" / "not the answer" / "not correct" / "not the
    best option" / "incorrect choice" / "not the correct answer".
  ✘ Restating or paraphrasing the option text as the whole rationale.
  ✘ Restating or paraphrasing the CORRECT answer text.
  ✘ Multi-sentence paragraphs. Any rationale > 1 short line.

EXAMPLES — GOOD (concise, direct, from lecture):
  B: "يُثبط COX-1 ويُسبب نزيفاً هضمياً."
  C: "يفاقم فرط البوتاسيوم عبر تثبيط الألدوستيرون."
  D: "يُسبب تسمماً كبدياً بجرعاته المعتادة."
  E: "يُبطئ التوصيل الأذيني البطيني بشكل خطر."

EXAMPLES — BAD (FORBIDDEN):
  ✘ "لا يتوافق مع الوصف الصحيح للحالة؛ الإجابة الصحيحة هي B)."
  ✘ "هذا الخيار غير صحيح لأن الدواء لا يعمل بهذه الطريقة."
  ✘ "ACE inhibitors are wrong."
  ✘ "Incorrect choice."
  ✘ Two options sharing the same rationale sentence.

The entry at the "correct" index MUST be an empty string "".
"""

DISTRACTOR_REGEN_PROMPT = """You are a medical educator. For each wrong MCQ option in the payload,
write ONE UNIQUE SHORT-LINE medical rationale (max ~15 words) explaining
WHY THAT SPECIFIC OPTION is clinically wrong — grounded strictly in the
lecture source.

INPUT FORMAT (JSON):
{
  "<question_index>": {
    "question": "...",
    "correct_option": "...",
    "wrong_options": {"B": "...", "C": "...", "D": "...", "E": "..."}
  },
  ...
}

OUTPUT FORMAT (JSON):
{
  "<question_index>": {
    "B": "one-short-line rationale",
    "C": "one-short-line rationale",
    "D": "one-short-line rationale",
    "E": "one-short-line rationale"
  },
  ...
}

══════════════════════════════════════════════════════════════════════
HARD RULES — VIOLATION = INVALID OUTPUT
══════════════════════════════════════════════════════════════════════
✔ UNIQUE per option — each rationale MUST be distinct from every other
  rationale for the same question. Do NOT reuse the same sentence.
✔ SPECIFIC to that option's content — name the actual mechanism /
  contraindication / wrong receptor / wrong pathway / wrong timing /
  wrong drug class that makes THIS option wrong.
✔ Ground each rationale strictly in the provided lecture source text.
✔ Write in the SAME LANGUAGE as the lecture source text.
✔ ONE SHORT LINE — maximum ~15 words. NO paragraphs.
✔ NEVER mention, restate, quote, or paraphrase the correct_option text.
✔ NEVER reference other options.

FORBIDDEN PHRASINGS (any ⇒ INVALID):
  ✘ "لا يتوافق مع الوصف الصحيح للحالة" (verboten).
  ✘ Any "الصحيح هو ..." boilerplate prefix.
  ✘ "هذا الخيار غير صحيح" / "الإجابة غير صحيحة" or equivalents.
  ✘ "wrong" / "incorrect" / "not the answer" / "not correct" /
    "not the best option" / "incorrect choice".
  ✘ Restating or paraphrasing the option text as the whole rationale.
  ✘ Restating or paraphrasing the correct_option text.
  ✘ Multi-sentence paragraphs.
  ✘ Duplicate rationales across two different wrong options.

EXAMPLES — GOOD:
  B: "يُثبط COX-1 ويُسبب نزيفاً هضمياً."
  C: "يفاقم فرط البوتاسيوم عبر تثبيط الألدوستيرون."

EXAMPLES — BAD (FORBIDDEN):
  ✘ "لا يتوافق مع الوصف الصحيح للحالة؛ الإجابة الصحيحة هي B)."
  ✘ "ACE inhibitors are wrong."
  ✘ "Incorrect choice."
  ✘ Same sentence repeated for B and D.

Provide a rationale for EVERY letter present in "wrong_options". Do NOT omit.
Output RAW JSON ONLY. No prose, no markdown, no code fences.
"""

# ==============================================================================
# 5. RETRY + SAFE AI CALL
# ==============================================================================


class EmptyResponseError(Exception):
    pass


_NETWORK_RETRYABLE = (
    RateLimitError,
    APIConnectionError,
    APITimeoutError,
    InternalServerError,
)


@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential_jitter(initial=2, max=30),
    retry=retry_if_exception_type(_NETWORK_RETRYABLE),
    before_sleep=before_sleep_log(logger, logging.WARNING),
    reraise=True,
)
async def _chat_completion_once(
    messages: list[dict],
    *,
    temperature: float = 0.4,
    max_tokens: int | None = None,
) -> str:
    response = await client.chat.completions.create(
        model=OPENROUTER_MODEL,
        messages=messages,
        max_tokens=max_tokens or MAX_OUTPUT_TOKENS,
        temperature=temperature,
    )
    content = (response.choices[0].message.content or "").strip()
    content = _strip_safety_preamble(content)
    if not content:
        logger.warning("Empty content from API (will retry via outer wrapper)")
        raise EmptyResponseError("Empty content from API")
    return content


async def _chat_completion(
    messages: list[dict],
    *,
    temperature: float = 0.4,
    max_tokens: int | None = None,
) -> str:
    last_exc: Exception | None = None
    for attempt in range(1, EMPTY_RESPONSE_MAX_ATTEMPTS + 1):
        try:
            return await _chat_completion_once(
                messages, temperature=temperature, max_tokens=max_tokens
            )
        except EmptyResponseError as exc:
            last_exc = exc
            logger.warning(
                "Empty AI response — attempt %d/%d failed",
                attempt, EMPTY_RESPONSE_MAX_ATTEMPTS,
            )
            if attempt < EMPTY_RESPONSE_MAX_ATTEMPTS:
                backoff = min(2 ** attempt, 20)
                jitter = random.uniform(0.0, 1.0)
                wait_s = backoff + jitter
                logger.info("Empty-response backoff: sleeping %.1fs before retry", wait_s)
                await asyncio.sleep(wait_s)

    raise RuntimeError(
        "⚠️ عذراً يا دكتور، الموديل المجاني رجّع رد فارغ بعد "
        f"{EMPTY_RESPONSE_MAX_ATTEMPTS} محاولات متتالية. "
        "اضغط /reset وجرّب تاني بعد شوية."
    ) from last_exc


@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential_jitter(initial=2, max=30),
    retry=retry_if_exception_type(_NETWORK_RETRYABLE),
    before_sleep=before_sleep_log(logger, logging.WARNING),
    reraise=True,
)
async def _stream_completion_once(
    messages: list[dict],
    *,
    temperature: float = 0.4,
    max_tokens: int | None = None,
    on_chunk: Callable[[str], Awaitable[None]] | None = None,
) -> str:
    stream = await client.chat.completions.create(
        model=OPENROUTER_MODEL,
        messages=messages,
        max_tokens=max_tokens or MAX_OUTPUT_TOKENS,
        temperature=temperature,
        stream=True,
    )
    buffer = ""
    async for event in stream:
        try:
            delta = event.choices[0].delta.content
        except (IndexError, AttributeError):
            delta = None
        if delta:
            buffer += delta
            if on_chunk is not None:
                try:
                    await on_chunk(_strip_safety_preamble(buffer))
                except Exception as cb_exc:  # pragma: no cover
                    logger.debug("on_chunk callback failed (ignored): %s", cb_exc)
    final = _strip_safety_preamble(buffer.strip())
    if not final:
        logger.warning("Empty streamed content from API (will retry via outer wrapper)")
        raise EmptyResponseError("Empty streamed content from API")
    return final


async def _stream_completion(
    messages: list[dict],
    *,
    temperature: float = 0.4,
    max_tokens: int | None = None,
    on_chunk: Callable[[str], Awaitable[None]] | None = None,
) -> str:
    last_exc: Exception | None = None
    for attempt in range(1, EMPTY_RESPONSE_MAX_ATTEMPTS + 1):
        try:
            return await _stream_completion_once(
                messages,
                temperature=temperature,
                max_tokens=max_tokens,
                on_chunk=on_chunk,
            )
        except EmptyResponseError as exc:
            last_exc = exc
            logger.warning(
                "Empty streamed AI response — attempt %d/%d failed",
                attempt, EMPTY_RESPONSE_MAX_ATTEMPTS,
            )
            if attempt < EMPTY_RESPONSE_MAX_ATTEMPTS:
                backoff = min(2 ** attempt, 20)
                jitter = random.uniform(0.0, 1.0)
                wait_s = backoff + jitter
                logger.info("Empty-stream backoff: sleeping %.1fs before retry", wait_s)
                await asyncio.sleep(wait_s)

    raise RuntimeError(
        "⚠️ عذراً يا دكتور، الموديل المجاني رجّع رد فارغ بعد "
        f"{EMPTY_RESPONSE_MAX_ATTEMPTS} محاولات متتالية. "
        "اضغط /reset وجرّب تاني بعد شوية."
    ) from last_exc


async def safe_ai_call(
    messages: list[dict],
    *,
    temperature: float = 0.4,
    max_tokens: int | None = None,
) -> str:
    try:
        return await _chat_completion(
            messages, temperature=temperature, max_tokens=max_tokens
        )
    except Exception as exc:
        status = getattr(exc, "status_code", None) or getattr(exc, "code", None)
        if status == 402:
            logger.error("OpenRouter 402 - free-tier budget exceeded: %s", exc)
            raise RuntimeError(
                "The AI service rejected the request because the free-tier "
                "budget was exceeded. Try a shorter document or a different free model."
            ) from exc
        if status == 404:
            logger.error("OpenRouter 404 - model %r not found: %s", OPENROUTER_MODEL, exc)
            raise RuntimeError(
                f"The configured model `{OPENROUTER_MODEL}` was not found on OpenRouter. "
                "Ask the admin to set OPENROUTER_MODEL to a valid `...:free` model."
            ) from exc
        if status == 429:
            logger.error("OpenRouter 429 after retries: %s", exc)
            raise RuntimeError(
                "The AI service is rate-limiting requests. Please wait a minute and try again."
            ) from exc
        logger.exception("AI call failed permanently")
        raise RuntimeError("The AI service is temporarily unavailable. Please try again shortly.") from exc


async def safe_ai_call_stream(
    messages: list[dict],
    *,
    temperature: float = 0.4,
    max_tokens: int | None = None,
    on_chunk: Callable[[str], Awaitable[None]] | None = None,
) -> str:
    try:
        return await _stream_completion(
            messages,
            temperature=temperature,
            max_tokens=max_tokens,
            on_chunk=on_chunk,
        )
    except Exception as exc:
        logger.warning(
            "Streaming failed after retries (%s); falling back to non-streaming call",
            exc,
        )
        return await safe_ai_call(
            messages, temperature=temperature, max_tokens=max_tokens
        )

# ==============================================================================
# 5b. MCQ CACHE + BACKGROUND TASK INFRASTRUCTURE
# ==============================================================================

_MCQ_LOCK_GUARD = asyncio.Lock()
_MCQ_LOCKS: dict[str, asyncio.Lock] = {}
_BACKGROUND_TASKS: set[asyncio.Task] = set()


def _evict_mcq_locks_locked() -> None:
    """Caller MUST hold _MCQ_LOCK_GUARD. Evicts non-held locks until under cap."""
    if len(_MCQ_LOCKS) < MCQ_LOCKS_MAX_ENTRIES:
        return
    target = MCQ_LOCKS_MAX_ENTRIES // 2
    for k in list(_MCQ_LOCKS.keys()):
        if len(_MCQ_LOCKS) <= target:
            break
        lk = _MCQ_LOCKS.get(k)
        if lk is not None and not lk.locked():
            _MCQ_LOCKS.pop(k, None)


async def _get_mcq_lock(text_hash: str) -> asyncio.Lock:
    async with _MCQ_LOCK_GUARD:
        lock = _MCQ_LOCKS.get(text_hash)
        if lock is None:
            _evict_mcq_locks_locked()
            lock = asyncio.Lock()
            _MCQ_LOCKS[text_hash] = lock
        return lock


def _fire_task(coro) -> asyncio.Task:
    task = asyncio.create_task(coro)
    _BACKGROUND_TASKS.add(task)
    task.add_done_callback(_BACKGROUND_TASKS.discard)
    return task


_CALLBACK_COOLDOWN: dict[int, float] = {}


def _callback_allowed(user_id: int) -> bool:
    now = time.monotonic()
    last = _CALLBACK_COOLDOWN.get(user_id, 0.0)
    if now - last < CALLBACK_COOLDOWN_SECONDS:
        return False
    _CALLBACK_COOLDOWN[user_id] = now

    if len(_CALLBACK_COOLDOWN) > CALLBACK_COOLDOWN_MAX_ENTRIES:
        cutoff = now - CALLBACK_COOLDOWN_GC_AGE
        stale = [k for k, v in _CALLBACK_COOLDOWN.items() if v < cutoff]
        for k in stale:
            _CALLBACK_COOLDOWN.pop(k, None)
    return True

# ==============================================================================
# 6. PDF EXTRACTION + CHUNKING + VECTOR INDEXING + GLOSSARY
# ==============================================================================


def extract_text_from_pdf(pdf_bytes: bytes) -> str:
    reader = pypdf.PdfReader(io.BytesIO(pdf_bytes))
    pages: list[str] = []
    for page in reader.pages:
        try:
            text = page.extract_text() or ""
        except Exception:
            text = ""
        if text.strip():
            pages.append(text.strip())
    return "\n\n".join(pages).strip()


def compute_doc_hash(pdf_bytes: bytes) -> str:
    return hashlib.md5(pdf_bytes).hexdigest()


def compute_text_hash(text: str) -> str:
    return hashlib.md5(text.encode("utf-8")).hexdigest()


def chunk_text(text: str, max_chars: int = MAX_CHUNK_CHARS) -> list[str]:
    if len(text) <= max_chars:
        return [text]
    chunks: list[str] = []
    remaining = text
    while remaining and len(chunks) < MAX_CHUNKS:
        if len(remaining) <= max_chars:
            chunks.append(remaining)
            break
        split_at = remaining.rfind("\n\n", 0, max_chars)
        if split_at < max_chars // 2:
            split_at = remaining.rfind(". ", 0, max_chars)
            if split_at < max_chars // 2:
                split_at = max_chars
            else:
                split_at += 1
        piece = remaining[:split_at].strip()
        if piece:
            chunks.append(piece)
        remaining = remaining[split_at:].strip()
    return chunks


def condense_for_context(text: str) -> str:
    chunks = chunk_text(text)
    if len(chunks) == 1:
        return chunks[0][:MAX_CONTEXT_CHARS]
    per_chunk = max(600, MAX_CONTEXT_CHARS // len(chunks))
    pieces = [c[:per_chunk].rstrip() for c in chunks[:MAX_CHUNKS]]
    return "\n\n---\n\n".join(pieces)[:MAX_CONTEXT_CHARS]


_STOPWORDS: frozenset[str] = frozenset({
    "the", "a", "an", "of", "and", "or", "in", "on", "at", "to", "for",
    "is", "are", "was", "were", "be", "been", "being", "have", "has", "had",
    "do", "does", "did", "will", "would", "could", "should", "may", "might",
    "must", "can", "this", "that", "these", "those", "it", "its", "from",
    "with", "by", "as", "which", "who", "whom", "whose", "what", "where",
    "when", "why", "how", "but", "not", "no", "so", "if", "then", "than",
    "من", "إلى", "على", "في", "عن", "مع", "هذا", "هذه", "ذلك", "تلك",
    "هو", "هي", "هم", "كان", "كانت", "يكون", "أن", "إن", "لا", "ما",
    "أو", "و", "ثم", "قد", "كل", "بعض", "بين", "بعد", "قبل", "حتى",
})

_TOKEN_RE = re.compile(r"[\w\u0600-\u06FF]+", re.UNICODE)


def _tokenize(text: str) -> list[str]:
    tokens = _TOKEN_RE.findall(text.lower())
    return [t for t in tokens if len(t) >= 3 and t not in _STOPWORDS]


def _hash_token(token: str, dim: int = EMBEDDING_DIM) -> int:
    h = hashlib.md5(token.encode("utf-8")).digest()
    return int.from_bytes(h[:4], "little") % dim


def _embed_sparse(text: str) -> dict[int, float]:
    tokens = _tokenize(text)
    if not tokens:
        return {}
    tf: dict[int, float] = {}
    for tok in tokens:
        idx = _hash_token(tok)
        tf[idx] = tf.get(idx, 0.0) + 1.0
    import math as _m
    values = {k: 1.0 + _m.log(v) for k, v in tf.items()}
    norm = _m.sqrt(sum(w * w for w in values.values()))
    if norm > 0:
        values = {k: v / norm for k, v in values.items()}
    return values


def _sparse_dot(v1: dict[int, float], v2: dict[int, float]) -> float:
    if not v1 or not v2:
        return 0.0
    if len(v1) > len(v2):
        v1, v2 = v2, v1
    return sum(val * v2.get(idx, 0.0) for idx, val in v1.items())


def _chunk_for_index(text: str, max_chars: int = VECTOR_CHUNK_CHARS) -> list[str]:
    paragraphs = re.split(r"\n\s*\n", text)
    chunks: list[str] = []
    for para in paragraphs:
        para = para.strip()
        if not para:
            continue
        if len(para) <= max_chars:
            chunks.append(para)
        else:
            sentences = re.split(r"(?<=[.!?؟])\s+", para)
            buf = ""
            for sent in sentences:
                if len(buf) + len(sent) + 1 <= max_chars:
                    buf = (buf + " " + sent).strip() if buf else sent
                else:
                    if buf:
                        chunks.append(buf)
                    buf = sent
            if buf:
                chunks.append(buf)
    return chunks


class PDFVectorIndex:
    def __init__(self, chunks: Sequence[str]):
        self.chunks: list[str] = list(chunks)
        self._vectors: list[dict[int, float]] = [
            _embed_sparse(c) for c in self.chunks
        ]

    def search(self, query: str, k: int = VECTOR_TOP_K) -> list[str]:
        if not self._vectors:
            return []
        qvec = _embed_sparse(query)
        if not qvec:
            return []
        scored: list[tuple[float, int]] = []
        for i, vec in enumerate(self._vectors):
            s = _sparse_dot(qvec, vec)
            if s > 0:
                scored.append((s, i))
        if not scored:
            return []
        scored.sort(reverse=True)
        top = scored[:k]
        return [self.chunks[i] for _, i in top]


def _build_vector_index(text: str) -> PDFVectorIndex:
    chunks = _chunk_for_index(text)
    return PDFVectorIndex(chunks)


def _get_or_build_index(key: str, text: str) -> PDFVectorIndex:
    cached = _PDF_INDEX_LRU.get(key)
    if cached is not None:
        return cached
    index = _build_vector_index(text)
    _PDF_INDEX_LRU.put(key, index)
    logger.info(
        "Built vector index for %s: %d chunks", key, len(index.chunks)
    )
    return index


_ABBREV_RE = re.compile(r"\b[A-Z]{2,6}(?:[A-Z0-9]+)?s?\b")
_CAP_TERM_RE = re.compile(
    r"\b[A-Z][a-z]{2,}(?:[-\s][A-Z][a-z]{2,}){0,3}\b"
)
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?؟۔])\s+")


def extract_medical_glossary(
    text: str,
    max_terms: int = GLOSSARY_MAX_TERMS,
    context_chars: int = GLOSSARY_CONTEXT_CHARS,
) -> dict[str, str]:
    if not text or not text.strip():
        return {}

    glossary: dict[str, str] = {}
    seen_lower: set[str] = set()

    sentences = _SENTENCE_SPLIT_RE.split(text)
    for sentence in sentences:
        if len(glossary) >= max_terms:
            break
        stripped = sentence.strip()
        if len(stripped) < 10:
            continue

        for match in _ABBREV_RE.finditer(stripped):
            term = match.group(0).strip()
            if len(term) < 2:
                continue
            key = term.lower()
            if key in seen_lower or key in _STOPWORDS:
                continue
            if term.isupper() is False and term != term.upper():
                continue
            context = stripped[:context_chars].strip()
            glossary[term] = context
            seen_lower.add(key)
            if len(glossary) >= max_terms:
                return glossary

        for match in _CAP_TERM_RE.finditer(stripped):
            term = match.group(0).strip()
            if len(term) < GLOSSARY_MIN_TERM_LEN:
                continue
            key = term.lower()
            if key in seen_lower or key in _STOPWORDS:
                continue
            if term.split()[0].lower() in {
                "the", "this", "these", "those", "when", "while", "after",
                "before", "during", "patient", "patients", "however",
                "therefore", "because", "although", "figure", "table",
            }:
                continue
            context = stripped[:context_chars].strip()
            glossary[term] = context
            seen_lower.add(key)
            if len(glossary) >= max_terms:
                return glossary

    return glossary


def format_glossary_for_prompt(glossary: dict[str, str]) -> str:
    if not glossary:
        return ""
    lines = ["KEY TERMINOLOGY from the source (use these names exactly):"]
    for term, ctx in glossary.items():
        lines.append(f"▪ {term} — {ctx}")
    return "\n".join(lines)


def _load_glossary_for_doc(doc_hash: str | None, text: str) -> dict[str, str]:
    if not text:
        return {}

    if doc_hash:
        cached = _GLOSSARY_LRU.get(doc_hash)
        if cached is not None:
            return cached
        try:
            raw = db_get_doc_cache_field(doc_hash, "glossary_json")
        except sqlite3.Error:
            raw = None
        if raw:
            try:
                data = json.loads(raw)
                if isinstance(data, dict):
                    _GLOSSARY_LRU.put(doc_hash, data)
                    return data
            except (json.JSONDecodeError, TypeError):
                pass

    glossary = extract_medical_glossary(text)

    if doc_hash and glossary:
        try:
            db_set_doc_cache_field(
                doc_hash, "glossary_json",
                json.dumps(glossary, ensure_ascii=False),
            )
        except sqlite3.Error:
            logger.exception("Could not persist glossary for %s", doc_hash[:8])
        _GLOSSARY_LRU.put(doc_hash, glossary)

    logger.info(
        "Extracted glossary: %d terms (doc_hash=%s)",
        len(glossary), (doc_hash or "?")[:8],
    )
    return glossary

# ==============================================================================
# 7. AI TASKS
# ==============================================================================

_EMOJI_BULLET_OR_STEP_RE = re.compile(
    r"^(?:[🔹🔸💠🔻🔺🅰🅱🔴🟡🟢✅❌⭐⚡]|[1-9]\uFE0F?\u20E3)"
)


def _normalize_summary_typography(text: str) -> str:
    if not text:
        return text

    text = _strip_safety_preamble(text)
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = "\n".join(line.rstrip() for line in text.split("\n"))

    text = re.sub(r"^[\-\*]\s+", "• ", text, flags=re.MULTILINE)
    text = re.sub(r"^▪\s*", "• ", text, flags=re.MULTILINE)
    text = re.sub(r"^·\s*", "• ", text, flags=re.MULTILINE)
    text = re.sub(r"^(\d+)\.\s*", r"\1. ", text, flags=re.MULTILINE)

    lines = text.split("\n")
    out: list[str] = []
    for line in lines:
        stripped = line.strip()
        is_heading = stripped.startswith("#")
        is_bullet = stripped.startswith("• ")
        is_numbered = bool(re.match(r"^\d+\.\s", stripped))
        is_pearl = stripped.startswith("📌") or stripped.startswith("⚠️")
        is_emoji_bullet_or_step = bool(
            _EMOJI_BULLET_OR_STEP_RE.match(stripped)
        )
        is_hr = stripped in ("---", "───", "___", "***", "———")

        if (
            is_heading
            or is_bullet
            or is_numbered
            or is_pearl
            or is_emoji_bullet_or_step
            or is_hr
        ):
            if out and out[-1].strip() != "":
                out.append("")
            out.append(line)
            out.append("")
        else:
            out.append(line)

    text = "\n".join(out)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


async def ai_summary(
    text: str,
    on_chunk: Callable[[str], Awaitable[None]] | None = None,
    *,
    doc_hash: str | None = None,
    skip_cache: bool = False,
) -> str:
    if doc_hash and not skip_cache:
        cached_lru = _SUMMARY_LRU.get(doc_hash)
        if cached_lru:
            logger.info("ai_summary: LRU HIT — doc_hash=%s", doc_hash[:8])
            return cached_lru
        try:
            cached_db = db_get_doc_cache_field(doc_hash, "summary")
        except sqlite3.Error:
            cached_db = None
        if cached_db:
            logger.info("ai_summary: SQLITE HIT — doc_hash=%s", doc_hash[:8])
            _SUMMARY_LRU.put(doc_hash, cached_db)
            return cached_db

    glossary = _load_glossary_for_doc(doc_hash, text)
    glossary_block = format_glossary_for_prompt(glossary)

    user_content = (
        "Produce a PREMIUM, BEAUTIFULLY STRUCTURED, HIGH-YIELD summary of "
        "the following medical text — 'الزتونة الطبية في نقاط مركزة'.\n\n"
        "This is study notes for a medical student: every section uses "
        "rich emojis, every idea is broken into a scannable bullet or "
        "step, generous whitespace, bold key terms — zero walls of text.\n\n"
        "VISUAL REQUIREMENTS (STRICT):\n"
        "▪️ Main sections: `### <emoji> **<Title>**` on its own line, "
        "with a blank line before AND after.\n"
        "▪️ Section emojis: 🎯 title · 📌 definition · ⚡ mechanism · "
        "🔬 classification · 🩺 clinical features · 💊 treatment · "
        "⚠️ complications · 💡 pearls.\n"
        "▪️ Separate major sections with `---` on its own line.\n"
        "▪️ Plain bullets: `• 🔹 ...` for facts, `• 💠 ...` for clinical "
        "features, `• 🅰️ / • 🅱️ ...` for subtypes, `• 🔴 ...` for "
        "serious complications, `• 🟡 ...` for cautions.\n"
        "▪️ Sequential steps: `1️⃣ ...`, `2️⃣ ...`, `3️⃣ ...` — NEVER "
        "plain `1. 2. 3.`.\n"
        "▪️ Insert a BLANK LINE between every bullet and every step — "
        "the output MUST breathe.\n"
        "▪️ Bold every **key medical term** with double asterisks.\n\n"
        "CONTENT REQUIREMENTS (STRICT):\n"
        "▪️ Preserve every specific drug, receptor, enzyme, pathway, "
        "criterion, and key number — never vague-ify.\n"
        "▪️ ZERO hallucination: only facts present in the source.\n"
        "▪️ ZERO filler: no intros, no recaps, no repetition.\n"
        "▪️ COVER ALL core ideas — do not skip whole sections.\n"
        "▪️ Total length: ~600–900 words of pure signal.\n\n"
        "Follow the PREMIUM SUMMARY TEMPLATE from your system prompt "
        "EXACTLY.\n\n"
    )
    if glossary_block:
        user_content += glossary_block + "\n\n---\n\n"
    user_content += "Lecture text:\n\n" + condense_for_context(text)

    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": user_content},
    ]

    if on_chunk is not None:
        raw = await safe_ai_call_stream(
            messages,
            temperature=0.3,
            max_tokens=SUMMARY_MAX_TOKENS,
            on_chunk=on_chunk,
        )
    else:
        raw = await safe_ai_call(
            messages, temperature=0.3, max_tokens=SUMMARY_MAX_TOKENS
        )
    result = _normalize_summary_typography(raw)

    if not result or not result.strip():
        logger.warning(
            "ai_summary: EMPTY result after normalization (doc_hash=%s) — "
            "raw_len=%d",
            (doc_hash or "?")[:8], len(raw or ""),
        )

    if doc_hash and result:
        _SUMMARY_LRU.put(doc_hash, result)
        try:
            db_set_doc_cache_field(doc_hash, "summary", result)
            logger.info("ai_summary: cached summary doc_hash=%s", doc_hash[:8])
        except sqlite3.Error:
            logger.exception("Could not cache summary for %s", doc_hash[:8])

    return result


async def ai_flashcards(
    text: str,
    on_chunk: Callable[[str], Awaitable[None]] | None = None,
    *,
    doc_hash: str | None = None,
    skip_cache: bool = False,
) -> str:
    if doc_hash and not skip_cache:
        cached_lru = _FLASHCARD_LRU.get(doc_hash)
        if cached_lru:
            logger.info("ai_flashcards: LRU HIT — doc_hash=%s", doc_hash[:8])
            return cached_lru
        try:
            cached_db = db_get_doc_cache_field(doc_hash, "flashcards")
        except sqlite3.Error:
            cached_db = None
        if cached_db:
            logger.info("ai_flashcards: SQLITE HIT — doc_hash=%s", doc_hash[:8])
            _FLASHCARD_LRU.put(doc_hash, cached_db)
            return cached_db

    glossary = _load_glossary_for_doc(doc_hash, text)
    glossary_block = format_glossary_for_prompt(glossary)

    user_content = (
        "Create 10 Anki-style flashcards from the following text. "
        "Follow the flashcard format in your instructions exactly.\n\n"
    )
    if glossary_block:
        user_content += glossary_block + "\n\n---\n\n"
    user_content += condense_for_context(text)

    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": user_content},
    ]
    if on_chunk is not None:
        raw = await safe_ai_call_stream(
            messages,
            temperature=0.4,
            max_tokens=FLASHCARD_MAX_TOKENS,
            on_chunk=on_chunk,
        )
    else:
        raw = await safe_ai_call(
            messages, temperature=0.4, max_tokens=FLASHCARD_MAX_TOKENS
        )
    raw = _strip_safety_preamble(raw)
    raw = raw.replace("\r\n", "\n").replace("\r", "\n")
    raw = re.sub(r"\n{3,}", "\n\n", raw)
    raw = "\n".join(line.rstrip() for line in raw.split("\n"))
    result = raw.strip()

    if not result or not result.strip():
        logger.warning(
            "ai_flashcards: EMPTY result after normalization (doc_hash=%s) — "
            "raw_len=%d",
            (doc_hash or "?")[:8], len(raw or ""),
        )

    if doc_hash and result:
        _FLASHCARD_LRU.put(doc_hash, result)
        try:
            db_set_doc_cache_field(doc_hash, "flashcards", result)
            logger.info("ai_flashcards: cached doc_hash=%s", doc_hash[:8])
        except sqlite3.Error:
            logger.exception("Could not cache flashcards for %s", doc_hash[:8])

    return result


async def ai_chat(history: list[dict], user_text: str) -> str:
    messages = (
        [{"role": "system", "content": SYSTEM_PROMPT}]
        + history
        + [{"role": "user", "content": user_text}]
    )
    return await safe_ai_call(messages, temperature=0.6)

# ==============================================================================
# 7b. MCQ JSON PARSING
# ==============================================================================


def _validate_mcq_list(data) -> list[dict]:
    if not isinstance(data, list):
        return []
    out: list[dict] = []
    for item in data[:5]:
        if not isinstance(item, dict):
            continue
        q = item.get("question")
        opts = item.get("options")
        expl = item.get("explanation")
        correct = item.get("correct")
        if not isinstance(q, str) or not isinstance(expl, str):
            continue
        if not isinstance(opts, list) or len(opts) != 5:
            continue
        try:
            c = int(correct)
        except (TypeError, ValueError):
            continue
        if not 0 <= c < 5:
            continue
        diff = str(item.get("difficulty", "medium")).lower()
        if diff not in ("easy", "medium", "hard"):
            diff = "medium"

        raw_distractors = item.get("distractors")
        distractors: list[str] = ["", "", "", "", ""]
        if isinstance(raw_distractors, list) and len(raw_distractors) == 5:
            distractors = [str(d or "").strip() for d in raw_distractors]
            distractors[c] = ""

        out.append(
            {
                "difficulty": diff,
                "question": q.strip(),
                "options": [str(o).strip() for o in opts],
                "correct": c,
                "explanation": expl.strip(),
                "distractors": distractors,
            }
        )
    return out


def _strip_fences(raw: str) -> str:
    if not raw:
        return ""
    text = raw.strip()
    text = _strip_safety_preamble(text)
    text = re.sub(r"^\s*```(?:json|JSON|javascript|js)?\s*\r?\n?", "", text)
    text = re.sub(r"\r?\n?\s*```\s*$", "", text)
    return text.strip()


def _balanced_json_slice(text: str, open_ch: str, close_ch: str) -> str | None:
    if not text or not open_ch or not close_ch or open_ch == close_ch:
        return None
    start = text.find(open_ch)
    if start == -1:
        return None
    depth = 0
    in_str = False
    escape = False
    for i in range(start, len(text)):
        ch = text[i]
        if in_str:
            if escape:
                escape = False
            elif ch == "\\":
                escape = True
            elif ch == '"':
                in_str = False
            continue
        if ch == '"':
            in_str = True
        elif ch == open_ch:
            depth += 1
        elif ch == close_ch:
            depth -= 1
            if depth == 0:
                return text[start:i + 1]
    return None


def _try_load_array(candidate: str) -> list | None:
    if not candidate:
        return None
    try:
        data = json.loads(candidate)
    except json.JSONDecodeError:
        repaired = re.sub(r",\s*([\]}])", r"\1", candidate)
        try:
            data = json.loads(repaired)
        except json.JSONDecodeError:
            return None
    return data if isinstance(data, list) else None


def _salvage_objects(raw: str) -> list[dict]:
    if not raw:
        return []
    objs: list[dict] = []
    n = len(raw)
    i = 0
    while i < n:
        if raw[i] != "{":
            i += 1
            continue
        depth = 0
        in_str = False
        escape = False
        start = i
        j = i
        closed = False
        while j < n:
            ch = raw[j]
            if in_str:
                if escape:
                    escape = False
                elif ch == "\\":
                    escape = True
                elif ch == '"':
                    in_str = False
                j += 1
                continue
            if ch == '"':
                in_str = True
            elif ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0:
                    try:
                        obj = json.loads(raw[start:j + 1])
                        if isinstance(obj, dict):
                            objs.append(obj)
                    except json.JSONDecodeError:
                        repaired = re.sub(r",\s*([\]}])", r"\1", raw[start:j + 1])
                        try:
                            obj = json.loads(repaired)
                            if isinstance(obj, dict):
                                objs.append(obj)
                        except json.JSONDecodeError:
                            pass
                    i = j + 1
                    closed = True
                    break
            j += 1
        if not closed:
            break
    return objs


def _unescape_json_str(s: str) -> str:
    if not s:
        return s
    try:
        return json.loads(f'"{s}"')
    except json.JSONDecodeError:
        return (
            s.replace('\\"', '"')
            .replace("\\n", "\n")
            .replace("\\t", "\t")
            .replace("\\\\", "\\")
        )


_MCQ_OBJ_REGEX = re.compile(
    r'\{\s*'
    r'(?:"difficulty"\s*:\s*"(?P<difficulty>[^"\\]*)"\s*,\s*)?'
    r'"question"\s*:\s*"(?P<question>(?:[^"\\]|\\.)*)"\s*,\s*'
    r'"options"\s*:\s*\[(?P<options>[^\]]*)\]\s*,\s*'
    r'"correct"\s*:\s*(?P<correct>-?\d+)\s*,\s*'
    r'"explanation"\s*:\s*"(?P<explanation>(?:[^"\\]|\\.)*)"'
    r'(?:\s*,\s*"distractors"\s*:\s*\[(?P<distractors>[^\]]*)\])?'
    r'[^{}]*\}',
    re.DOTALL,
)

_STR_LITERAL_REGEX = re.compile(r'"((?:[^"\\]|\\.)*)"')


def _regex_extract_mcqs(raw: str) -> list[dict]:
    if not raw:
        return []
    results: list[dict] = []
    for m in _MCQ_OBJ_REGEX.finditer(raw):
        try:
            q_text = _unescape_json_str(m.group("question")).strip()
            options_raw = m.group("options") or ""
            options = [
                _unescape_json_str(o).strip()
                for o in _STR_LITERAL_REGEX.findall(options_raw)
            ]
            if len(options) != 5:
                continue
            try:
                correct = int(m.group("correct"))
            except (TypeError, ValueError):
                continue
            if not 0 <= correct < 5:
                continue
            explanation = _unescape_json_str(m.group("explanation") or "").strip()
            if not q_text or not explanation:
                continue

            distractors: list[str] = ["", "", "", "", ""]
            distractors_raw = m.group("distractors")
            if distractors_raw:
                parsed = [
                    _unescape_json_str(d).strip()
                    for d in _STR_LITERAL_REGEX.findall(distractors_raw)
                ]
                if len(parsed) == 5:
                    distractors = parsed
                    distractors[correct] = ""

            diff = (m.group("difficulty") or "medium").lower()
            if diff not in ("easy", "medium", "hard"):
                diff = "medium"

            results.append(
                {
                    "difficulty": diff,
                    "question": q_text,
                    "options": options,
                    "correct": correct,
                    "explanation": explanation,
                    "distractors": distractors,
                }
            )
        except (ValueError, TypeError):
            continue
    return results


def parse_mcqs_json(raw: str) -> list[dict]:
    if not raw or not raw.strip():
        logger.warning("MCQ parse: empty raw response")
        return []

    original = raw
    cleaned = _strip_fences(raw)

    data = _try_load_array(cleaned)
    if data:
        validated = _validate_mcq_list(data)
        if validated:
            logger.debug("MCQ parse: strategy 1 — %d questions", len(validated))
            return validated

    arr_slice = _balanced_json_slice(cleaned, "[", "]")
    if arr_slice:
        data = _try_load_array(arr_slice)
        if data:
            validated = _validate_mcq_list(data)
            if validated:
                logger.debug("MCQ parse: strategy 2 — %d questions", len(validated))
                return validated

    obj_slice = _balanced_json_slice(cleaned, "{", "}")
    if obj_slice:
        try:
            repaired = re.sub(r",\s*([\]}])", r"\1", obj_slice)
            obj = json.loads(repaired)
        except json.JSONDecodeError:
            obj = None
        if isinstance(obj, dict):
            for key in ("questions", "mcqs", "quiz", "items", "data", "result"):
                inner = obj.get(key)
                if isinstance(inner, list):
                    validated = _validate_mcq_list(inner)
                    if validated:
                        logger.debug(
                            "MCQ parse: strategy 3 (key %r) — %d questions",
                            key, len(validated),
                        )
                        return validated

    fence_match = re.search(
        r"```(?:json)?\s*(\[.*?\])\s*```",
        original,
        re.DOTALL | re.IGNORECASE,
    )
    if fence_match:
        data = _try_load_array(fence_match.group(1))
        if data:
            validated = _validate_mcq_list(data)
            if validated:
                logger.debug("MCQ parse: strategy 4 — %d questions", len(validated))
                return validated

    salvaged = _salvage_objects(cleaned)
    if salvaged:
        validated = _validate_mcq_list(salvaged)
        if validated:
            logger.info(
                "MCQ parse: strategy 5 (salvaged) — %d/%d valid",
                len(validated), len(salvaged),
            )
            return validated

    regex_hits = _regex_extract_mcqs(original)
    if not regex_hits:
        regex_hits = _regex_extract_mcqs(cleaned)
    if regex_hits:
        validated = _validate_mcq_list(regex_hits)
        if validated:
            logger.info(
                "MCQ parse: strategy 6 (regex recovery) — %d/%d valid",
                len(validated), len(regex_hits),
            )
            return validated

    logger.warning(
        "MCQ parse failed after all 6 strategies. len=%d head=%r tail=%r",
        len(original),
        original[:300],
        original[-200:],
    )
    return []

# ==============================================================================
# 7c. DISTRACTOR RATIONALE
# ==============================================================================

_GENERIC_DISMISSAL_PATTERNS = (
    r"^(this|that|it|the option)\s+is\s+(wrong|incorrect|not correct|not right)\b",
    r"^(this|that|it|the option)\s+is\s+not\s+the\s+(correct|right|best)\s+(answer|choice|option)\b",
    r"^(wrong|incorrect|not correct|not right|false)\b",
    r"^not\s+(the\s+)?(correct|right|best)\s+(answer|choice|option)\b",
    r"^incorrect\s+(choice|answer|option)\b",
    r"^not\s+the\s+answer\b",
    r"^see\s+(the\s+)?explanation\b",
)

_FILLER_PREFIX_PATTERNS = (
    r"^لا\s+يتوافق\s+مع\s+الوصف\s+الصحيح\s+للحالة\s*[؛;:،,]?\s*",
    r"^لا\s+يتوافق\s+مع\s+الوصف\s+الصحيح\s*[؛;:،,]?\s*",
    r"^هذا\s+الخيار\s+غير\s+صحيح\s*(لأن|حيث)?\s*[؛;:،,]?\s*",
    r"^هذا\s+الخيار\s+خطأ\s*(لأن|حيث)?\s*[؛;:،,]?\s*",
    r"^الإجابة\s+الصحيحة\s+هي\s+[A-Ea-e]\)?\s*[—\-–]\s*",
    r"^الصحيح\s+هو\s+[A-Ea-e]\)?\s*[—\-–]\s*",
    r"^this\s+option\s+is\s+(wrong|incorrect)\s*(because|since)?\s*[:,،]?\s*",
    r"^the\s+correct\s+answer\s+is\s+[A-Ea-e]\)?\s*[—\-–]\s*",
)

_FILLER_STRIP_MAX_ITER = len(_FILLER_PREFIX_PATTERNS) + 2


def _strip_filler_prefix(reason: str) -> str:
    r = reason.strip()
    if not r:
        return r
    for _ in range(_FILLER_STRIP_MAX_ITER):
        changed = False
        for pat in _FILLER_PREFIX_PATTERNS:
            m = re.match(pat, r, flags=re.IGNORECASE)
            if m:
                stripped = r[m.end():].strip()
                if stripped:
                    r = stripped
                    changed = True
                    break
        if not changed:
            break
    return r


def _compact_rationale(reason: str) -> str:
    if not reason:
        return ""
    r = _strip_filler_prefix(reason)
    r = " ".join(r.split())
    if not r:
        return ""

    if len(r) > MAX_DISTRACTOR_CHARS:
        m = re.match(r"^(.{20,190}[.!?؟])(\s|$)", r)
        if m:
            r = m.group(1).strip()
        else:
            r = r[:MAX_DISTRACTOR_CHARS].rstrip() + "…"

    return r


def _clean_distractor_reason(
    reason: str, option: str, correct_answer: str = ""
) -> str:
    if not reason:
        return ""
    r = reason.strip()
    if not r:
        return ""

    r = _strip_filler_prefix(r)
    if not r:
        return ""

    rl = r.lower().strip(" .,;:—-!?")
    o = option.strip().lower().strip(" .,;:—-!?")

    if correct_answer:
        ca = correct_answer.strip().lower().strip(" .,;:—-!?")
        if ca and len(ca) >= 8:
            if (
                rl == ca
                or rl.startswith(ca + " ")
                or rl.startswith(ca + ".")
                or rl.startswith(ca + "،")
                or rl.startswith(ca + "؛")
                or rl.startswith(ca + ":")
            ):
                logger.debug(
                    "Rejecting distractor reason: duplicates correct answer text"
                )
                return ""

    if not o:
        return _compact_rationale(r)

    if rl == o:
        return ""

    for pat in _GENERIC_DISMISSAL_PATTERNS:
        if re.match(pat, rl):
            return ""

    if o in rl and len(r) <= len(option) + 20:
        return ""

    if rl.startswith(o):
        remainder = rl[len(o):].strip(" .,;:—-!?")
        if remainder in (
            "is wrong", "is incorrect", "is not correct",
            "is not right", "is not the answer", "wrong",
            "incorrect", "",
        ):
            return ""

    return _compact_rationale(r)


def _build_fallback_reason(
    option: str, correct_letter: str, explanation_snippet: str
) -> str:
    if explanation_snippet:
        return f"لا ينطبق عليه الشرح الطبي أعلاه؛ الصحيح {correct_letter})."
    return f"غير مُطابق للمعطيات الطبية؛ الصحيح {correct_letter})."


def _find_bad_distractor_indices(q: dict) -> list[int]:
    bad: list[int] = []
    distractors = q.get("distractors") or ["", "", "", "", ""]
    correct_option_text = q["options"][q["correct"]]
    for i in range(5):
        if i == q["correct"]:
            continue
        reason = distractors[i] if i < len(distractors) else ""
        if not _clean_distractor_reason(
            reason, q["options"][i], correct_option_text
        ):
            bad.append(i)
    return bad


async def _regenerate_bad_distractors(questions: list[dict], context: str) -> None:
    bad_map: dict[int, list[int]] = {}
    for qi, q in enumerate(questions):
        bad = _find_bad_distractor_indices(q)
        if bad:
            bad_map[qi] = bad

    if not bad_map:
        logger.debug("_regenerate_bad_distractors: nothing to fix")
        return

    total_bad = sum(len(v) for v in bad_map.values())
    logger.info(
        "_regenerate_bad_distractors: %d bad distractor(s) across %d question(s)",
        total_bad, len(bad_map),
    )

    payload: dict[str, dict] = {}
    for qi, bad_indices in bad_map.items():
        q = questions[qi]
        payload[str(qi)] = {
            "question": q["question"],
            "correct_option": q["options"][q["correct"]],
            "wrong_options": {
                chr(65 + i): q["options"][i] for i in bad_indices
            },
        }

    messages = [
        {"role": "system", "content": DISTRACTOR_REGEN_PROMPT},
        {
            "role": "user",
            "content": (
                f"Lecture source text:\n\n{context}\n\n"
                f"---\n\n"
                f"Questions needing UNIQUE, option-specific distractor rationales:\n\n"
                f"{json.dumps(payload, ensure_ascii=False, indent=2)}\n\n"
                f"Return ONE UNIQUE SHORT-LINE rationale per wrong option "
                f"(~15 words max), in the same language as the lecture text, "
                f"each explaining WHY THAT SPECIFIC OPTION is medically wrong. "
                f"NO filler phrases. NO echoing of the correct_option. "
                f"NO duplicate sentences across options. "
                f"Do NOT reference other options."
            ),
        },
    ]

    try:
        raw = await safe_ai_call(
            messages, temperature=0.25, max_tokens=DISTRACTOR_MAX_TOKENS
        )
    except RuntimeError as exc:
        logger.warning("_regenerate_bad_distractors: AI call failed — %s", exc)
        return

    cleaned = _strip_fences(raw)
    obj_slice = _balanced_json_slice(cleaned, "{", "}")
    if not obj_slice:
        logger.warning(
            "_regenerate_bad_distractors: no JSON object found. head=%r",
            raw[:200],
        )
        return

    try:
        data = json.loads(re.sub(r",\s*([\]}])", r"\1", obj_slice))
    except json.JSONDecodeError:
        logger.warning(
            "_regenerate_bad_distractors: JSON decode failed. head=%r",
            obj_slice[:200],
        )
        return

    if not isinstance(data, dict):
        logger.warning("_regenerate_bad_distractors: parsed data is not a dict")
        return

    applied = 0
    for qi_str, letter_map in data.items():
        try:
            qi = int(qi_str)
        except (ValueError, TypeError):
            continue
        if qi not in bad_map or not isinstance(letter_map, dict):
            continue
        q = questions[qi]
        correct_text = q["options"][q["correct"]]
        for letter, reason in letter_map.items():
            if not isinstance(letter, str) or len(letter) != 1:
                continue
            i = ord(letter.upper()) - ord("A")
            if not 0 <= i < 5:
                continue
            if i not in bad_map[qi]:
                continue
            if not isinstance(reason, str):
                continue
            reason = reason.strip()
            if not reason:
                continue
            if _clean_distractor_reason(reason, q["options"][i], correct_text):
                q["distractors"][i] = reason
                applied += 1

    logger.info(
        "_regenerate_bad_distractors: applied %d/%d regenerated rationales",
        applied, total_bad,
    )


async def _generate_mcqs_internal(
    text: str,
    on_progress: Callable[[int], Awaitable[None]] | None = None,
    *,
    doc_hash: str | None = None,
) -> list[dict]:
    context = condense_for_context(text)

    glossary = _load_glossary_for_doc(doc_hash, text)
    glossary_block = format_glossary_for_prompt(glossary)

    user_content = (
        "Generate exactly 5 MCQs from the following medical lecture text. "
        "Return ONLY the JSON array as specified. "
        "Every non-empty `distractors` entry MUST be ONE UNIQUE SHORT LINE "
        "(~15 words max) stating the SPECIFIC medical reason that option "
        "is wrong — grounded strictly in the source. Each wrong option "
        "(a, b, d, e) must have its OWN distinct rationale — never the "
        "same sentence. NEVER echo, restate, or paraphrase the correct "
        "answer. NO filler phrases, NO generic dismissals, NO cross-"
        "references to other options. Write in the same language as "
        "the source.\n\n"
    )
    if glossary_block:
        user_content += glossary_block + "\n\n---\n\n"
    user_content += context

    base_messages: list[dict] = [
        {"role": "system", "content": MCQ_STRUCTURED_PROMPT},
        {"role": "user", "content": user_content},
    ]

    last_progress = [0.0]

    async def _progress_cb(buffer: str) -> None:
        if on_progress is None:
            return
        now = time.monotonic()
        if now - last_progress[0] < 1.5:
            return
        last_progress[0] = now
        count = buffer.count('"question"')
        try:
            await on_progress(min(count, 5))
        except Exception:
            pass

    questions: list[dict] = []

    try:
        raw_primary = await safe_ai_call_stream(
            base_messages,
            temperature=0.4,
            max_tokens=MCQ_MAX_TOKENS,
            on_chunk=_progress_cb,
        )
    except RuntimeError as exc:
        logger.error("_generate_mcqs_internal: primary AI call failed — %s", exc)
        raise

    questions = parse_mcqs_json(raw_primary)
    if questions:
        logger.info(
            "_generate_mcqs_internal: primary attempt succeeded — %d questions parsed",
            len(questions),
        )
    else:
        logger.warning(
            "_generate_mcqs_internal: primary attempt returned unparseable output; "
            "retrying with strict-JSON instruction (temp=0.15)"
        )

        retry_messages: list[dict] = list(base_messages)
        retry_messages.append({"role": "assistant", "content": raw_primary[:2000]})
        retry_messages.append(
            {
                "role": "user",
                "content": (
                    "Your previous reply could not be parsed as JSON. "
                    "Reply now with ONLY a raw JSON array — starting with `[` and "
                    "ending with `]`. No prose. No code fences. No markdown. "
                    "No explanations. No trailing commas. Just the array of 5 "
                    "question objects in the exact schema specified. Every non-empty "
                    "`distractors` entry MUST be ONE UNIQUE SHORT LINE (~15 words), "
                    "grounded in the source, specific to THAT option, with NO "
                    "filler phrases and NO echo of the correct answer."
                ),
            }
        )

        try:
            raw_retry = await safe_ai_call(
                retry_messages, temperature=0.15, max_tokens=MCQ_MAX_TOKENS
            )
        except RuntimeError as exc:
            logger.error("_generate_mcqs_internal: retry AI call failed — %s", exc)
            raise

        questions = parse_mcqs_json(raw_retry)
        if questions:
            logger.info(
                "_generate_mcqs_internal: retry attempt succeeded — %d questions parsed",
                len(questions),
            )
        else:
            logger.error(
                "_generate_mcqs_internal: both attempts failed. primary_len=%d retry_len=%d",
                len(raw_primary), len(raw_retry),
            )
            return []

    await _regenerate_bad_distractors(questions, context)

    return questions


async def ai_mcqs_structured(
    text: str,
    *,
    force_refresh: bool = False,
    on_progress: Callable[[int], Awaitable[None]] | None = None,
    doc_hash: str | None = None,
) -> list[dict]:
    context = condense_for_context(text)
    text_hash = compute_text_hash(context)

    if doc_hash and not force_refresh:
        cached_doc = _MCQ_LRU.get(f"doc:{doc_hash}")
        if cached_doc is not None:
            logger.info(
                "ai_mcqs_structured: DOC LRU HIT — doc_hash=%s — %d questions",
                doc_hash[:8], len(cached_doc),
            )
            return cached_doc
        try:
            raw = db_get_doc_cache_field(doc_hash, "mcqs_json")
        except sqlite3.Error:
            raw = None
        if raw:
            try:
                data = json.loads(raw)
                validated = _validate_mcq_list(data)
                if validated:
                    logger.info(
                        "ai_mcqs_structured: DOC SQLITE HIT — doc_hash=%s",
                        doc_hash[:8],
                    )
                    _MCQ_LRU.put(f"doc:{doc_hash}", validated)
                    return validated
            except (json.JSONDecodeError, TypeError):
                pass

    if not force_refresh:
        cached_lru = _MCQ_LRU.get(text_hash)
        if cached_lru is not None:
            logger.info(
                "ai_mcqs_structured: LRU HIT — hash=%s — %d questions",
                text_hash[:8], len(cached_lru),
            )
            return cached_lru

    if not force_refresh:
        try:
            cached = await _run_cpu(db_get_mcq_cache, text_hash)
        except sqlite3.Error:
            logger.exception("MCQ cache read failed")
            cached = None
        if cached is not None:
            logger.info(
                "ai_mcqs_structured: DB CACHE HIT — hash=%s — %d questions",
                text_hash[:8], len(cached),
            )
            _MCQ_LRU.put(text_hash, cached)
            if doc_hash:
                _MCQ_LRU.put(f"doc:{doc_hash}", cached)
            return cached

    lock = await _get_mcq_lock(text_hash)
    async with lock:
        if not force_refresh:
            cached_lru = _MCQ_LRU.get(text_hash)
            if cached_lru is not None:
                return cached_lru
            try:
                cached = await _run_cpu(db_get_mcq_cache, text_hash)
            except sqlite3.Error:
                cached = None
            if cached is not None:
                _MCQ_LRU.put(text_hash, cached)
                if doc_hash:
                    _MCQ_LRU.put(f"doc:{doc_hash}", cached)
                return cached

        questions = await _generate_mcqs_internal(
            text, on_progress=on_progress, doc_hash=doc_hash
        )
        if questions:
            try:
                await _run_cpu(db_set_mcq_cache, text_hash, questions)
                logger.info(
                    "ai_mcqs_structured: CACHE MISS → STORED — hash=%s — %d questions",
                    text_hash[:8], len(questions),
                )
            except sqlite3.Error:
                logger.exception("MCQ cache write failed (continuing)")
            if doc_hash:
                try:
                    await _run_cpu(
                        db_set_doc_cache_field,
                        doc_hash,
                        "mcqs_json",
                        json.dumps(questions, ensure_ascii=False),
                    )
                    _MCQ_LRU.put(f"doc:{doc_hash}", questions)
                except sqlite3.Error:
                    logger.exception("doc_cache MCQ write failed (continuing)")
            _MCQ_LRU.put(text_hash, questions)
        return questions

# ==============================================================================
# 7d. MCQ BATCH PREFETCH QUEUE
# ==============================================================================


async def _get_mcq_prefetch_lock(doc_hash: str) -> asyncio.Lock:
    return await _get_mcq_lock(f"prefetch:{doc_hash}")


def db_count_mcq_batches(doc_hash: str) -> int:
    if not doc_hash:
        return 0
    with db_connection() as conn:
        row = conn.execute(
            "SELECT COUNT(*) FROM mcq_batch_queue "
            "WHERE doc_hash = ? AND consumed_at IS NULL",
            (doc_hash,),
        ).fetchone()
    return row[0] if row else 0


def db_push_mcq_batch(doc_hash: str, questions: list[dict]) -> None:
    if not doc_hash or not questions:
        return
    try:
        payload = json.dumps(questions, ensure_ascii=False)
    except (TypeError, ValueError):
        logger.exception("Could not serialize MCQ batch for doc_hash=%s", doc_hash[:8])
        return
    with db_connection() as conn:
        conn.execute(
            "INSERT INTO mcq_batch_queue (doc_hash, questions_json) VALUES (?, ?)",
            (doc_hash, payload),
        )


def db_claim_next_mcq_batch(doc_hash: str) -> list[dict] | None:
    if not doc_hash:
        return None
    with db_connection() as conn:
        row = conn.execute(
            "SELECT id, questions_json FROM mcq_batch_queue "
            "WHERE doc_hash = ? AND consumed_at IS NULL "
            "ORDER BY id ASC LIMIT 1",
            (doc_hash,),
        ).fetchone()
        if row is None:
            return None
        conn.execute(
            "UPDATE mcq_batch_queue SET consumed_at = CURRENT_TIMESTAMP WHERE id = ?",
            (row["id"],),
        )
    try:
        data = json.loads(row["questions_json"])
    except (json.JSONDecodeError, TypeError):
        logger.warning(
            "MCQ batch %s for doc %s is corrupt; skipping",
            row["id"], doc_hash[:8],
        )
        return None
    validated = _validate_mcq_list(data)
    return validated or None


def db_prune_old_mcq_batches(max_age_days: int = 7) -> None:
    try:
        with db_connection() as conn:
            deleted = conn.execute(
                "DELETE FROM mcq_batch_queue "
                "WHERE consumed_at IS NOT NULL "
                "  AND consumed_at < datetime('now', ?)",
                (f"-{int(max_age_days)} days",),
            ).rowcount
        if deleted:
            logger.info(
                "Pruned %d consumed MCQ batch(es) older than %d day(s)",
                deleted, max_age_days,
            )
    except sqlite3.Error:
        logger.exception("Could not prune old MCQ batches (ignored)")


async def _prefetch_mcq_batches(doc_hash: str, text: str) -> None:
    if not doc_hash or not text or not text.strip():
        return

    await _wait_for_user_priority()

    existing = await _run_cpu(db_count_mcq_batches, doc_hash)
    if existing >= MCQ_PREFETCH_MAX_BATCHES:
        logger.info(
            "MCQ prefetch: already at capacity (%d/%d) — doc_hash=%s",
            existing, MCQ_PREFETCH_MAX_BATCHES, doc_hash[:8],
        )
        return

    lock = await _get_mcq_prefetch_lock(doc_hash)
    try:
        while True:
            await _wait_for_user_priority()

            async with lock:
                if _USER_ACTIVITY_EVENT.is_set():
                    logger.info(
                        "MCQ prefetch: yielding to active user request(s) — doc_hash=%s",
                        doc_hash[:8],
                    )
                    continue

                current = await _run_cpu(db_count_mcq_batches, doc_hash)
                if current >= MCQ_PREFETCH_MAX_BATCHES:
                    logger.info(
                        "MCQ prefetch: reached max (%d) — stopping (doc_hash=%s)",
                        current, doc_hash[:8],
                    )
                    break

                logger.info(
                    "MCQ prefetch: generating batch %d/%d — doc_hash=%s",
                    current + 1, MCQ_PREFETCH_MAX_BATCHES, doc_hash[:8],
                )
                batch = await _generate_mcqs_internal(text, doc_hash=doc_hash)
                if not batch:
                    logger.warning(
                        "MCQ prefetch: empty batch — stopping (doc_hash=%s)",
                        doc_hash[:8],
                    )
                    break
                await _run_cpu(db_push_mcq_batch, doc_hash, batch)
                logger.info(
                    "MCQ prefetch: batch %d pushed (%d Qs) — doc_hash=%s",
                    current + 1, len(batch), doc_hash[:8],
                )
            await asyncio.sleep(MCQ_PREFETCH_INTER_BATCH_DELAY)
    except asyncio.CancelledError:
        logger.info("MCQ prefetch cancelled — doc_hash=%s", doc_hash[:8])
        raise
    except Exception:
        logger.exception("MCQ prefetch failed — doc_hash=%s", doc_hash[:8])


def _schedule_mcq_prefetch(doc_hash: str, text: str) -> None:
    if not doc_hash or not text or not text.strip():
        return

    existing = _MCQ_PREFETCH_TASKS.get(doc_hash)
    if existing is not None and not existing.done():
        return

    task = _fire_task(_prefetch_mcq_batches(doc_hash, text))
    _MCQ_PREFETCH_TASKS[doc_hash] = task

    def _cleanup(t: asyncio.Task) -> None:
        if _MCQ_PREFETCH_TASKS.get(doc_hash) is t:
            _MCQ_PREFETCH_TASKS.pop(doc_hash, None)

    task.add_done_callback(_cleanup)


async def _get_mcq_batch_for_user(
    text: str,
    *,
    doc_hash: str | None = None,
    on_progress: Callable[[int], Awaitable[None]] | None = None,
) -> list[dict]:
    if doc_hash:
        batch = await _run_cpu(db_claim_next_mcq_batch, doc_hash)
        if batch:
            logger.info(
                "MCQ serve: PREFETCH HIT — doc_hash=%s — %d Qs",
                doc_hash[:8], len(batch),
            )
            remaining = await _run_cpu(db_count_mcq_batches, doc_hash)
            if remaining < MCQ_PREFETCH_THRESHOLD:
                _schedule_mcq_prefetch(doc_hash, text)
            return batch

        try:
            raw = db_get_doc_cache_field(doc_hash, "mcqs_json")
        except sqlite3.Error:
            raw = None
        if raw:
            try:
                validated = _validate_mcq_list(json.loads(raw))
                if validated:
                    logger.info(
                        "MCQ serve: LEGACY doc_cache HIT — doc_hash=%s",
                        doc_hash[:8],
                    )
                    _schedule_mcq_prefetch(doc_hash, text)
                    return validated
            except (json.JSONDecodeError, TypeError):
                pass

        lock = await _get_mcq_prefetch_lock(doc_hash)
        async with lock:
            batch = await _run_cpu(db_claim_next_mcq_batch, doc_hash)
            if batch:
                logger.info(
                    "MCQ serve: PREFETCH HIT (post-lock) — doc_hash=%s — %d Qs",
                    doc_hash[:8], len(batch),
                )
                remaining = await _run_cpu(db_count_mcq_batches, doc_hash)
                if remaining < MCQ_PREFETCH_THRESHOLD:
                    _schedule_mcq_prefetch(doc_hash, text)
                return batch

            logger.info(
                "MCQ serve: on-demand generation — doc_hash=%s",
                doc_hash[:8],
            )
            batch = await _generate_mcqs_internal(
                text, on_progress=on_progress, doc_hash=doc_hash
            )
            if batch:
                _schedule_mcq_prefetch(doc_hash, text)
            return batch

    context = condense_for_context(text)
    text_hash = compute_text_hash(context)
    cached = _MCQ_LRU.get(text_hash)
    if cached is not None:
        return cached
    try:
        cached_db = await _run_cpu(db_get_mcq_cache, text_hash)
    except sqlite3.Error:
        cached_db = None
    if cached_db is not None:
        _MCQ_LRU.put(text_hash, cached_db)
        return cached_db

    batch = await _generate_mcqs_internal(
        text, on_progress=on_progress, doc_hash=None
    )
    if batch:
        _MCQ_LRU.put(text_hash, batch)
        try:
            await _run_cpu(db_set_mcq_cache, text_hash, batch)
        except sqlite3.Error:
            logger.exception("MCQ text_hash cache write failed (continuing)")
    return batch


async def _eager_generate_and_cache(
    text: str, doc_hash: str | None = None
) -> None:
    if not text or not text.strip():
        return
    if doc_hash:
        try:
            await _prefetch_mcq_batches(doc_hash, text)
        except Exception:
            logger.debug("Eager MCQ prefetch failed (silent)", exc_info=True)
        return
    try:
        questions = await ai_mcqs_structured(text, doc_hash=doc_hash)
        if questions:
            logger.info("Eager MCQ warm-up complete (%d questions)", len(questions))
    except Exception:
        logger.debug("Eager MCQ warm-up failed (silent)", exc_info=True)


def _schedule_eager_mcqs(text: str, doc_hash: str | None = None) -> None:
    if not text or not text.strip():
        return
    if doc_hash:
        _schedule_mcq_prefetch(doc_hash, text)
        return
    _fire_task(_eager_generate_and_cache(text, doc_hash=None))


async def _eager_download_and_generate(bot, file_id: str) -> None:
    try:
        tg_file = await _call(
            bot.get_file,
            file_id=file_id,
            read_timeout=REQUEST_TIMEOUT_SECONDS,
            write_timeout=REQUEST_TIMEOUT_SECONDS,
            connect_timeout=REQUEST_TIMEOUT_SECONDS,
        )
        pdf_bytes = bytes(await tg_file.download_as_bytearray())
        doc_hash = compute_doc_hash(pdf_bytes)
        text = await _run_process(extract_text_from_pdf, pdf_bytes)
        if text and text.strip():
            await _precompute_pipeline(text, doc_hash)
    except Exception:
        logger.warning(
            "Background upload-time precompute failed (file_id=%s)",
            file_id[:12] if file_id else "?",
            exc_info=True,
        )


async def _precompute_pipeline(text: str, doc_hash: str) -> None:
    if not text or not text.strip() or not doc_hash:
        return

    logger.info("Precompute START — doc_hash=%s", doc_hash[:8])

    try:
        await _run_cpu(_load_glossary_for_doc, doc_hash, text)
    except Exception:
        logger.warning("Precompute: glossary failed (continuing)", exc_info=True)

    try:
        _get_or_build_index(f"doc:{doc_hash}", text)
    except Exception:
        logger.warning("Precompute: vector index failed (continuing)", exc_info=True)

    try:
        already = db_get_doc_cache_field(doc_hash, "summary")
        if not already:
            summary = await ai_summary(text, doc_hash=doc_hash)
            logger.info(
                "Precompute: summary cached (%d chars) doc_hash=%s",
                len(summary), doc_hash[:8],
            )
    except Exception as exc:
        logger.warning("Precompute: summary failed — %s", exc)

    try:
        existing_count = await _run_cpu(db_count_mcq_batches, doc_hash)
        if existing_count == 0:
            _schedule_mcq_prefetch(doc_hash, text)
            logger.info(
                "Precompute: MCQ prefetch scheduled — doc_hash=%s",
                doc_hash[:8],
            )
    except Exception as exc:
        logger.warning("Precompute: MCQ prefetch scheduling failed — %s", exc)

    logger.info("Precompute DONE — doc_hash=%s", doc_hash[:8])


def _schedule_precompute(text: str, doc_hash: str) -> None:
    if not text or not text.strip() or not doc_hash:
        return
    _fire_task(_precompute_pipeline(text, doc_hash))

# ==============================================================================
# 8. FORMATTING HELPERS
# ==============================================================================


def escape_html(text: str) -> str:
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def markdown_to_html(text: str) -> str:
    text = _strip_safety_preamble(text)
    text = escape_html(text)
    text = re.sub(r"^#{1,6}\s*(.+)$", r"<b>\1</b>", text, flags=re.MULTILINE)
    text = re.sub(r"\*\*(.+?)\*\*", r"<b>\1</b>", text)
    text = re.sub(r"`([^`\n]+)`", r"<code>\1</code>", text)
    return text


def strip_markdown(text: str) -> str:
    text = _strip_safety_preamble(text)
    text = re.sub(r"^#{1,6}\s*", "", text, flags=re.MULTILINE)
    text = re.sub(r"\*\*(.+?)\*\*", r"\1", text)
    text = re.sub(r"`([^`\n]+)`", r"\1", text)
    return text


def split_for_telegram(text: str, max_len: int = TELEGRAM_CHUNK_CHARS) -> list[str]:
    if not text:
        return []
    if len(text) <= max_len:
        return [text]

    chunks: list[str] = []
    remaining = text
    half = max_len // 2

    while len(remaining) > max_len:
        split_at = remaining.rfind("\n\n", 0, max_len)

        if split_at < half:
            split_at = remaining.rfind("\n", 0, max_len)

        if split_at < half:
            for term in (". ", "! ", "? ", "؟ ", "۔ ", "। ", ".", "!", "?", "؟"):
                idx = remaining.rfind(term, 0, max_len)
                if idx > split_at:
                    split_at = idx + len(term)

        if split_at < half:
            for term in (", ", "; ", ": ", "، ", "؛ "):
                idx = remaining.rfind(term, 0, max_len)
                if idx > split_at:
                    split_at = idx + len(term) - 1

        if split_at < half:
            split_at = remaining.rfind(" ", 0, max_len)

        if split_at < half:
            split_at = max_len

        if split_at <= 0:
            split_at = max_len

        chunk = remaining[:split_at].rstrip()
        if chunk:
            chunks.append(chunk)
        remaining = remaining[split_at:].lstrip()

    if remaining:
        chunks.append(remaining)
    return chunks


async def _send_one_chunk_safe(
    bot,
    chat_id: int,
    markdown_chunk: str,
    reply_markup=None,
) -> bool:
    if not markdown_chunk or not markdown_chunk.strip():
        return True

    html_chunk = markdown_to_html(markdown_chunk)
    if not html_chunk or not html_chunk.strip():
        return True
    if len(html_chunk) <= TELEGRAM_HARD_LIMIT:
        try:
            await bot.send_message(
                chat_id=chat_id,
                text=html_chunk,
                parse_mode=ParseMode.HTML,
                reply_markup=reply_markup,
                disable_web_page_preview=True,
            )
            return True
        except BadRequest as exc:
            logger.warning(
                "HTML chunk rejected (%s); falling back to plain text", exc
            )
        except TelegramError:
            logger.exception("Telegram error sending HTML chunk")
            return False

    plain = strip_markdown(markdown_chunk)
    if not plain or not plain.strip():
        return True
    if len(plain) <= TELEGRAM_HARD_LIMIT:
        try:
            await bot.send_message(
                chat_id=chat_id,
                text=plain,
                reply_markup=reply_markup,
                disable_web_page_preview=True,
            )
            return True
        except TelegramError:
            logger.exception("Telegram error sending plain chunk")
            return False

    logger.warning(
        "Chunk too large even as plain text (%d chars); re-splitting at 1500",
        len(plain),
    )
    sub_chunks = split_for_telegram(markdown_chunk, max_len=1500)
    all_ok = True
    for i, sub in enumerate(sub_chunks):
        is_last = (i == len(sub_chunks) - 1)
        sub_markup = reply_markup if is_last else None
        ok = await _send_one_chunk_safe(bot, chat_id, sub, sub_markup)
        if not ok:
            all_ok = False
    return all_ok


async def send_chunked_response(
    bot,
    chat_id: int,
    text: str,
    reply_markup=None,
) -> None:
    if not text or not text.strip():
        if reply_markup is not None:
            try:
                await bot.send_message(
                    chat_id=chat_id,
                    text="📋",
                    reply_markup=reply_markup,
                )
            except TelegramError:
                logger.exception("Could not send empty-response markup footer")
        return

    chunks = split_for_telegram(text)
    total = len(chunks)
    last_chunk_ok = False

    for i, chunk in enumerate(chunks):
        is_last = (i == total - 1)
        markup = reply_markup if is_last else None
        ok = await _send_one_chunk_safe(bot, chat_id, chunk, markup)
        if is_last:
            last_chunk_ok = ok
        if not is_last:
            await asyncio.sleep(0.15)

    if reply_markup is not None and not last_chunk_ok:
        try:
            await bot.send_message(
                chat_id=chat_id,
                text="📋",
                reply_markup=reply_markup,
            )
        except TelegramError:
            logger.exception("Could not send fallback action-menu footer")


async def send_formatted(message, text: str, reply_markup=None) -> None:
    await send_chunked_response(
        message.get_bot(), message.chat_id, text, reply_markup=reply_markup
    )


async def _keep_typing(bot, chat_id: int) -> None:
    try:
        while True:
            try:
                await bot.send_chat_action(chat_id=chat_id, action=ChatAction.TYPING)
            except TelegramError:
                return
            await asyncio.sleep(4)
    except asyncio.CancelledError:
        pass


class typing_indicator:
    def __init__(self, bot, chat_id: int):
        self.bot = bot
        self.chat_id = chat_id
        self.task: "asyncio.Task | None" = None

    async def __aenter__(self):
        self.task = asyncio.create_task(_keep_typing(self.bot, self.chat_id))
        return self

    async def __aexit__(self, exc_type, exc, tb):
        if self.task:
            self.task.cancel()
            try:
                await self.task
            except asyncio.CancelledError:
                pass

# ==============================================================================
# 8b. STREAMING UI
# ==============================================================================


async def _stream_and_finalize(
    bot,
    chat_id: int,
    generator: Callable[[Callable[[str], Awaitable[None]]], Awaitable[str]],
    reply_markup: InlineKeyboardMarkup | None,
    progress_text: str,
) -> str:
    progress_msg = None
    try:
        progress_msg = await bot.send_message(
            chat_id=chat_id,
            text=progress_text,
            parse_mode=ParseMode.HTML,
        )
    except TelegramError:
        logger.exception("Could not send streaming progress placeholder")

    last_edit = [0.0]
    last_buffer = [""]

    async def on_chunk(buffer: str) -> None:
        if progress_msg is None:
            return
        buffer = _strip_safety_preamble(buffer)
        if not buffer:
            return
        now = time.monotonic()
        if now - last_edit[0] < STREAM_THROTTLE_SECONDS:
            return
        if buffer == last_buffer[0]:
            return
        display = buffer[:STREAM_MAX_DISPLAY_CHARS]
        try:
            await progress_msg.edit_text(
                markdown_to_html(display),
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
            )
            last_edit[0] = now
            last_buffer[0] = buffer
        except BadRequest:
            pass
        except TelegramError:
            pass

    try:
        result = await generator(on_chunk)
    finally:
        if progress_msg is not None:
            try:
                await progress_msg.delete()
            except TelegramError:
                pass

    stripped_result = (
        _strip_safety_preamble(result).strip() if isinstance(result, str) else ""
    )
    if not stripped_result:
        logger.warning(
            "Streaming result was empty after safety strip — sending "
            "polite fallback (chat_id=%s).",
            chat_id,
        )
        try:
            await bot.send_message(
                chat_id=chat_id,
                text=EMPTY_RESULT_FALLBACK_TEXT,
                reply_markup=reply_markup,
                parse_mode=ParseMode.HTML,
            )
        except TelegramError:
            logger.exception("Could not send empty-result fallback message")
        return result or ""

    await send_chunked_response(bot, chat_id, result, reply_markup)

    return result

# ==============================================================================
# 9. DATABASE LAYER
# ==============================================================================

ATT_SAVED = "saved"
ATT_DUPLICATE = "duplicate"
ATT_MISSING = "missing"


@dataclass(frozen=True)
class AttachmentData:
    kind: str
    file_id: str | None = None
    file_unique_id: str | None = None
    file_name: str | None = None
    caption: str | None = None
    text: str | None = None


class DeletedLecture(NamedTuple):
    block: str
    subject: str
    number: int
    file_name: str
    removed_attachments: int


@contextmanager
def db_connection() -> Iterator[sqlite3.Connection]:
    conn = sqlite3.connect(DB_PATH, timeout=DB_TIMEOUT_SECONDS)
    try:
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        with conn:
            yield conn
    finally:
        conn.close()


def _migrate_v1(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS lectures (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            block          TEXT    NOT NULL,
            subject        TEXT    NOT NULL,
            file_id        TEXT    NOT NULL,
            file_unique_id TEXT    NOT NULL,
            file_name      TEXT    NOT NULL,
            uploaded_by    INTEGER,
            created_at     TEXT    NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_lectures_block_subject
            ON lectures (block, subject);
        CREATE UNIQUE INDEX IF NOT EXISTS idx_lectures_unique
            ON lectures (block, subject, file_unique_id);
        """
    )


def _migrate_v2(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS attachments (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            lecture_id     INTEGER NOT NULL REFERENCES lectures (id) ON DELETE CASCADE,
            kind           TEXT    NOT NULL,
            file_id        TEXT,
            file_unique_id TEXT,
            file_name      TEXT,
            caption        TEXT,
            text           TEXT,
            uploaded_by    INTEGER,
            created_at     TEXT    NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_attachments_lecture
            ON attachments (lecture_id, id);
        CREATE UNIQUE INDEX IF NOT EXISTS idx_attachments_unique
            ON attachments (lecture_id, file_unique_id)
            WHERE file_unique_id IS NOT NULL;
        """
    )


def _migrate_v3(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS users (
            user_id      INTEGER PRIMARY KEY,
            username     TEXT,
            first_name   TEXT,
            activated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        """
    )

    cols = [row["name"] for row in conn.execute("PRAGMA table_info(lectures)").fetchall()]
    if "owner_id" not in cols:
        conn.execute("ALTER TABLE lectures ADD COLUMN owner_id INTEGER")

    conn.execute("DROP INDEX IF EXISTS idx_lectures_unique")
    conn.executescript(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_lectures_owner_unique
            ON lectures (owner_id, block, subject, file_unique_id);
        CREATE INDEX IF NOT EXISTS idx_lectures_owner
            ON lectures (owner_id, block, subject);
        """
    )


def _migrate_v4(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS mcq_cache (
            text_hash      TEXT PRIMARY KEY,
            questions_json TEXT NOT NULL,
            created_at     TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_mcq_cache_created
            ON mcq_cache (created_at);
        """
    )


def _migrate_v5(conn: sqlite3.Connection) -> None:
    cols = [row["name"] for row in conn.execute("PRAGMA table_info(mcq_cache)").fetchall()]
    if "schema_version" not in cols:
        conn.execute(
            "ALTER TABLE mcq_cache ADD COLUMN schema_version INTEGER NOT NULL DEFAULT 1"
        )
    conn.execute(
        "DELETE FROM mcq_cache WHERE schema_version < ?",
        (MCQ_CACHE_SCHEMA_VERSION,),
    )
    logger.info(
        "MCQ cache purged for schema upgrade → v%d", MCQ_CACHE_SCHEMA_VERSION
    )


def _migrate_v6(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS doc_cache (
            doc_hash      TEXT PRIMARY KEY,
            summary       TEXT,
            flashcards    TEXT,
            mcqs_json     TEXT,
            glossary_json TEXT,
            created_at    TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_doc_cache_created
            ON doc_cache (created_at);
        """
    )
    logger.info("doc_cache table ready (V12.0 Pillar 2)")


def _migrate_v7(conn: sqlite3.Connection) -> None:
    cols = [row["name"] for row in conn.execute("PRAGMA table_info(lectures)").fetchall()]
    if "doc_hash" not in cols:
        conn.execute("ALTER TABLE lectures ADD COLUMN doc_hash TEXT")
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_lectures_doc_hash ON lectures (doc_hash)"
    )


def _migrate_v8(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS mcq_batch_queue (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            doc_hash       TEXT    NOT NULL,
            questions_json TEXT    NOT NULL,
            consumed_at    TEXT,
            created_at     TEXT    NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_mcq_batch_doc_pending
            ON mcq_batch_queue (doc_hash, consumed_at, id);
        """
    )
    logger.info("mcq_batch_queue table ready (V13.0 Issue 2 · MCQ prefetch)")


MIGRATIONS: list[Callable[[sqlite3.Connection], None]] = [
    _migrate_v1,
    _migrate_v2,
    _migrate_v3,
    _migrate_v4,
    _migrate_v5,
    _migrate_v6,
    _migrate_v7,
    _migrate_v8,
]


def db_migrate(conn: sqlite3.Connection) -> None:
    current = conn.execute("PRAGMA user_version").fetchone()[0]
    for version, fn in enumerate(MIGRATIONS, start=1):
        if version <= current:
            continue
        logger.info("Applying database migration v%d (was v%d)", version, current)
        fn(conn)
        conn.execute(f"PRAGMA user_version = {version}")


def db_init() -> None:
    with db_connection() as conn:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA temp_store=MEMORY")
        conn.execute("PRAGMA cache_size=-8000")
        db_migrate(conn)
    logger.info("Database ready at %s (WAL + optimized indexes)", DB_PATH)


def db_get_mcq_cache(text_hash: str) -> list[dict] | None:
    with db_connection() as conn:
        row = conn.execute(
            "SELECT questions_json, schema_version FROM mcq_cache WHERE text_hash = ?",
            (text_hash,),
        ).fetchone()
    if row is None:
        return None

    version = row["schema_version"] if "schema_version" in row.keys() else 1
    if version < MCQ_CACHE_SCHEMA_VERSION:
        logger.info(
            "MCQ cache HIT but schema_version=%d < %d — treating as miss",
            version, MCQ_CACHE_SCHEMA_VERSION,
        )
        return None

    try:
        data = json.loads(row["questions_json"])
    except (json.JSONDecodeError, TypeError):
        logger.warning("MCQ cache row %s is corrupt; ignoring", text_hash[:8])
        return None
    if not isinstance(data, list) or not data:
        return None
    validated = _validate_mcq_list(data)
    return validated if validated else None


def db_set_mcq_cache(text_hash: str, questions: list[dict]) -> None:
    if not questions:
        return
    try:
        payload = json.dumps(questions, ensure_ascii=False)
    except (TypeError, ValueError):
        logger.exception("Could not serialize MCQs for cache")
        return
    with db_connection() as conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO mcq_cache
                (text_hash, questions_json, schema_version)
            VALUES (?, ?, ?)
            """,
            (text_hash, payload, MCQ_CACHE_SCHEMA_VERSION),
        )


_ALLOWED_DOC_CACHE_FIELDS = frozenset({
    "summary", "flashcards", "mcqs_json", "glossary_json",
})


def db_get_doc_cache_field(doc_hash: str, field: str) -> str | None:
    if not doc_hash or field not in _ALLOWED_DOC_CACHE_FIELDS:
        return None
    with db_connection() as conn:
        row = conn.execute(
            f"SELECT {field} FROM doc_cache WHERE doc_hash = ?",
            (doc_hash,),
        ).fetchone()
    if row is None:
        return None
    value = row[field]
    return value if value else None


def db_set_doc_cache_field(doc_hash: str, field: str, value: str) -> None:
    if not doc_hash or field not in _ALLOWED_DOC_CACHE_FIELDS:
        return
    if value is None:
        return
    with db_connection() as conn:
        conn.execute(
            "INSERT OR IGNORE INTO doc_cache (doc_hash) VALUES (?)",
            (doc_hash,),
        )
        conn.execute(
            f"UPDATE doc_cache SET {field} = ? WHERE doc_hash = ?",
            (value, doc_hash),
        )


def db_set_lecture_doc_hash(lecture_id: int, doc_hash: str) -> None:
    if not doc_hash:
        return
    with db_connection() as conn:
        conn.execute(
            "UPDATE lectures SET doc_hash = ? WHERE id = ?",
            (doc_hash, lecture_id),
        )


def db_get_lecture_doc_hash(lecture_id: int) -> str | None:
    with db_connection() as conn:
        row = conn.execute(
            "SELECT doc_hash FROM lectures WHERE id = ?",
            (lecture_id,),
        ).fetchone()
    if row is None:
        return None
    return row["doc_hash"] if row["doc_hash"] else None


def _evict_activation_cache_locked(now: float) -> None:
    if len(_ACTIVATION_CACHE) < ACTIVATION_CACHE_MAX_ENTRIES:
        return
    cutoff = now - ACTIVATION_CACHE_TTL
    stale = [k for k, v in _ACTIVATION_CACHE.items() if v[1] < cutoff]
    for k in stale:
        _ACTIVATION_CACHE.pop(k, None)
    if len(_ACTIVATION_CACHE) >= ACTIVATION_CACHE_MAX_ENTRIES:
        # Hard cap: drop oldest third
        drop = len(_ACTIVATION_CACHE) // 3
        for k in list(_ACTIVATION_CACHE.keys())[:drop]:
            _ACTIVATION_CACHE.pop(k, None)


def is_activated(user_id: int) -> bool:
    if user_id == ADMIN_BYPASS_ID:
        return True
    if not BOT_ACTIVATION_KEY:
        return True

    now = time.monotonic()
    cached = _ACTIVATION_CACHE.get(user_id)
    if cached is not None and now - cached[1] < ACTIVATION_CACHE_TTL:
        return cached[0]

    with db_connection() as conn:
        row = conn.execute(
            "SELECT 1 FROM users WHERE user_id = ?", (user_id,)
        ).fetchone()
    result = row is not None
    _ACTIVATION_CACHE[user_id] = (result, now)
    _evict_activation_cache_locked(now)
    return result


def db_activate_user(user_id: int, username: str | None, first_name: str | None) -> None:
    with db_connection() as conn:
        conn.execute(
            "INSERT OR REPLACE INTO users (user_id, username, first_name) VALUES (?, ?, ?)",
            (user_id, username, first_name),
        )
    _ACTIVATION_CACHE[user_id] = (True, time.monotonic())


def _clamp_page(total: int, page: int, page_size: int) -> tuple[int, int]:
    pages = max(1, math.ceil(total / page_size))
    return min(max(page, 0), pages - 1), pages


def _fetch_lecture(
    conn: sqlite3.Connection, lecture_id: int, owner_id: int | None
) -> sqlite3.Row | None:
    sql = """
        SELECT l.id, l.block, l.subject, l.file_id, l.file_name, l.owner_id,
               l.doc_hash,
               (SELECT COUNT(*) FROM lectures x
                 WHERE x.block  = l.block
                   AND x.subject = l.subject
                   AND x.owner_id = l.owner_id
                   AND x.id <= l.id)                                  AS number,
               (SELECT COUNT(*) FROM attachments a
                 WHERE a.lecture_id = l.id)                           AS att_count
          FROM lectures l
         WHERE l.id = ?
    """
    params: list = [lecture_id]
    if owner_id is not None:
        sql += " AND l.owner_id = ?"
        params.append(owner_id)
    return conn.execute(sql, params).fetchone()


def db_add_lecture(
    block: str,
    subject: str,
    file_id: str,
    file_unique_id: str,
    file_name: str,
    uploaded_by: int | None,
    owner_id: int,
) -> tuple[bool, sqlite3.Row]:
    with db_connection() as conn:
        cur = conn.execute(
            """
            INSERT OR IGNORE INTO lectures
                (block, subject, file_id, file_unique_id, file_name, uploaded_by, owner_id)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (block, subject, file_id, file_unique_id, file_name, uploaded_by, owner_id),
        )
        inserted = cur.rowcount > 0
        if inserted:
            lecture_id = cur.lastrowid
        else:
            lecture_id = conn.execute(
                """
                SELECT id FROM lectures
                 WHERE owner_id = ? AND block = ? AND subject = ? AND file_unique_id = ?
                """,
                (owner_id, block, subject, file_unique_id),
            ).fetchone()["id"]
        lecture = _fetch_lecture(conn, lecture_id, owner_id)
    return inserted, lecture


def db_get_lectures_page(
    block: str, subject: str, page: int, owner_id: int, page_size: int = PAGE_SIZE
) -> tuple[list[sqlite3.Row], int, int, int]:
    with db_connection() as conn:
        total = conn.execute(
            "SELECT COUNT(*) FROM lectures WHERE owner_id = ? AND block = ? AND subject = ?",
            (owner_id, block, subject),
        ).fetchone()[0]

        page, pages = _clamp_page(total, page, page_size)

        rows = conn.execute(
            """
            SELECT l.id, l.file_name,
                   (SELECT COUNT(*) FROM attachments a
                     WHERE a.lecture_id = l.id) AS att_count
              FROM lectures l
             WHERE l.owner_id = ? AND l.block = ? AND l.subject = ?
             ORDER BY l.id
             LIMIT ? OFFSET ?
            """,
            (owner_id, block, subject, page_size, page * page_size),
        ).fetchall()
    return rows, total, page, pages


def db_get_lecture(lecture_id: int, owner_id: int) -> sqlite3.Row | None:
    with db_connection() as conn:
        return _fetch_lecture(conn, lecture_id, owner_id)


def db_get_lecture_bundle(
    lecture_id: int, owner_id: int
) -> tuple[sqlite3.Row, list[sqlite3.Row]] | None:
    with db_connection() as conn:
        lecture = _fetch_lecture(conn, lecture_id, owner_id)
        if lecture is None:
            return None
        attachments = conn.execute(
            """
            SELECT id, kind, file_id, file_name, caption, text
              FROM attachments
             WHERE lecture_id = ?
             ORDER BY id
            """,
            (lecture_id,),
        ).fetchall()
    return lecture, attachments


def db_get_panel(
    lecture_id: int, page: int, owner_id: int
) -> tuple[sqlite3.Row, list[sqlite3.Row], int, int, int] | None:
    with db_connection() as conn:
        lecture = _fetch_lecture(conn, lecture_id, owner_id)
        if lecture is None:
            return None
        total = lecture["att_count"]
        page, pages = _clamp_page(total, page, ATT_PAGE_SIZE)
        rows = conn.execute(
            """
            SELECT id, kind, file_name, caption, text
              FROM attachments
             WHERE lecture_id = ?
             ORDER BY id
             LIMIT ? OFFSET ?
            """,
            (lecture_id, ATT_PAGE_SIZE, page * ATT_PAGE_SIZE),
        ).fetchall()
    return lecture, rows, total, page, pages


def db_get_attachment(
    attachment_id: int, owner_id: int
) -> tuple[sqlite3.Row, sqlite3.Row] | None:
    with db_connection() as conn:
        att = conn.execute(
            """
            SELECT a.id, a.lecture_id, a.kind, a.file_id, a.file_name, a.caption, a.text,
                   (SELECT COUNT(*) FROM attachments x
                     WHERE x.lecture_id = a.lecture_id AND x.id <= a.id) AS position
              FROM attachments a
              JOIN lectures l ON l.id = a.lecture_id
             WHERE a.id = ? AND l.owner_id = ?
            """,
            (attachment_id, owner_id),
        ).fetchone()
        if att is None:
            return None
        lecture = _fetch_lecture(conn, att["lecture_id"], owner_id)
    if lecture is None:
        return None
    return att, lecture


def db_add_attachment(
    lecture_id: int, data: AttachmentData, uploaded_by: int | None, owner_id: int
) -> tuple[str, sqlite3.Row | None]:
    with db_connection() as conn:
        if _fetch_lecture(conn, lecture_id, owner_id) is None:
            return ATT_MISSING, None
        try:
            cur = conn.execute(
                """
                INSERT OR IGNORE INTO attachments
                    (lecture_id, kind, file_id, file_unique_id, file_name,
                     caption, text, uploaded_by)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    lecture_id, data.kind, data.file_id, data.file_unique_id,
                    data.file_name, data.caption, data.text, uploaded_by,
                ),
            )
        except sqlite3.IntegrityError:
            return ATT_MISSING, None
        status = ATT_SAVED if cur.rowcount > 0 else ATT_DUPLICATE
        lecture = _fetch_lecture(conn, lecture_id, owner_id)
    return status, lecture


def db_delete_attachment(
    attachment_id: int, lecture_id: int, owner_id: int
) -> int | None:
    with db_connection() as conn:
        row = conn.execute(
            """
            SELECT (SELECT COUNT(*) FROM attachments x
                     WHERE x.lecture_id = a.lecture_id AND x.id <= a.id) AS position
              FROM attachments a
              JOIN lectures l ON l.id = a.lecture_id
             WHERE a.id = ? AND a.lecture_id = ? AND l.owner_id = ?
            """,
            (attachment_id, lecture_id, owner_id),
        ).fetchone()
        if row is None:
            return None
        conn.execute("DELETE FROM attachments WHERE id = ?", (attachment_id,))
    return row["position"]


def db_delete_lecture(lecture_id: int, owner_id: int) -> DeletedLecture | None:
    with db_connection() as conn:
        lecture = _fetch_lecture(conn, lecture_id, owner_id)
        if lecture is None:
            return None
        conn.execute("DELETE FROM attachments WHERE lecture_id = ?", (lecture_id,))
        conn.execute(
            "DELETE FROM lectures WHERE id = ? AND owner_id = ?",
            (lecture_id, owner_id),
        )
    return DeletedLecture(
        lecture["block"], lecture["subject"], lecture["number"],
        lecture["file_name"], lecture["att_count"],
    )

# ==============================================================================
# 10. KEYBOARD & TEXT BUILDERS
# ==============================================================================


def _chunk(items: Sequence, size: int) -> list[list]:
    return [list(items[i : i + size]) for i in range(0, len(items), size)]


def _shorten(text: str, limit: int) -> str:
    text = " ".join(text.split())
    if len(text) > limit:
        text = text[: max(limit - 1, 1)].rstrip() + "…"
    return text


def _nav_row(back_callback: str) -> list[InlineKeyboardButton]:
    return [
        InlineKeyboardButton(BTN_HOME, callback_data=CB_HOME),
        InlineKeyboardButton(BTN_BACK, callback_data=back_callback),
    ]


def _pager_row(
    page: int, pages: int, make_callback: Callable[[int], str]
) -> list[InlineKeyboardButton]:
    if pages <= 1:
        return []
    row: list[InlineKeyboardButton] = []
    if page > 0:
        row.append(InlineKeyboardButton("◀️ Prev", callback_data=make_callback(page - 1)))
    row.append(InlineKeyboardButton(f"{page + 1}/{pages}", callback_data=CB_NOOP))
    if page < pages - 1:
        row.append(InlineKeyboardButton("Next ▶️", callback_data=make_callback(page + 1)))
    return row


def _where(block: str, branch: str) -> str:
    return (
        f"📘 <b>{escape_html(BLOCKS.get(block, block))}</b>\n"
        f"└ 📂 <b>{escape_html(BRANCHES.get(branch, branch))}</b>"
    )


def main_menu_keyboard() -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(label, callback_data=f"{CB_BLOCK}:{key}")]
        for key, label in BLOCKS.items()
    ]
    return InlineKeyboardMarkup(rows)


def branch_menu_keyboard(block: str) -> InlineKeyboardMarkup:
    buttons = [
        InlineKeyboardButton(label, callback_data=f"{CB_BRANCH}:{block}:{key}:0")
        for key, label in BRANCHES.items()
    ]
    rows = _chunk(buttons, 2)
    rows.append(_nav_row(back_callback=CB_HOME))
    return InlineKeyboardMarkup(rows)


def clean_name(file_name: str) -> str:
    name = file_name[:-4] if file_name.lower().endswith(".pdf") else file_name
    return name.replace("_", " ").strip() or "Lecture"


def lecture_title(number: int, file_name: str) -> str:
    return f"{number}. {clean_name(file_name)}"


def lecture_label(number: int, file_name: str, att_count: int = 0) -> str:
    prefix = f"{number}. "
    suffix = f"  📎{att_count}" if att_count else ""
    budget = max(BUTTON_LABEL_MAX - len(prefix) - len(suffix), 10)
    return f"{prefix}{_shorten(clean_name(file_name), budget)}{suffix}"


def lecture_menu_keyboard(
    block: str,
    branch: str,
    rows_data: Sequence[sqlite3.Row],
    page: int,
    pages: int,
    *,
    manage: bool = False,
    can_manage: bool = False,
) -> InlineKeyboardMarkup:
    first = page * PAGE_SIZE + 1
    item_cb = f"{CB_PANEL}:{{id}}:0" if manage else f"{CB_LECTURE}:{{id}}"
    page_prefix = CB_MANAGE if manage else CB_BRANCH

    rows: list[list[InlineKeyboardButton]] = [
        [
            InlineKeyboardButton(
                lecture_label(first + i, r["file_name"], r["att_count"]),
                callback_data=item_cb.format(id=r["id"]),
            )
        ]
        for i, r in enumerate(rows_data)
    ]

    pager = _pager_row(page, pages, lambda p: f"{page_prefix}:{block}:{branch}:{p}")
    if pager:
        rows.append(pager)

    if not manage and can_manage and rows_data:
        rows.append(
            [InlineKeyboardButton(BTN_MANAGE, callback_data=f"{CB_MANAGE}:{block}:{branch}:{page}")]
        )

    back = f"{CB_BRANCH}:{block}:{branch}:{page}" if manage else f"{CB_BLOCK}:{block}"
    rows.append(_nav_row(back_callback=back))
    return InlineKeyboardMarkup(rows)


MAIN_MENU_TEXT = (
    "🏥 <b>Medical Archive</b>\n"
    "\n"
    f"{DIVIDER}\n"
    "\n"
    "⭐ <b>Welcome to your personal study vault</b>\n"
    "\n"
    "Select a medical block below to explore your lectures, attachments, and AI study tools.\n"
    "\n"
    f"{DIVIDER}\n"
    "\n"
    "💡 <i>Send any PDF in this chat to unlock Summary, MCQs, Flashcards, Ask, and Upload.</i>"
)


def branch_menu_text(block: str) -> str:
    return (
        f"📘 <b>{escape_html(BLOCKS[block])}</b>\n"
        "\n"
        f"{DIVIDER}\n"
        "\n"
        "🧠 <b>Choose a branch</b>\n"
        "\n"
        "Pick a subject below to browse its lectures.\n"
        "\n"
        "👇 <i>Tap one of the buttons below.</i>"
    )


def lecture_menu_text(block: str, branch: str, total: int, can_manage: bool = False) -> str:
    header = (
        f"{_where(block, branch)}\n"
        "\n"
        f"{DIVIDER}\n"
        "\n"
    )
    if total == 0:
        return (
            header
            + "📭 <b>No lectures here yet</b>\n"
            "\n"
            "This branch is completely empty.\n"
            "\n"
            "📎 <i>Send a PDF in this chat and tap 📂 Upload to add the first one.</i>"
        )
    text = (
        header
        + f"📚 <b>{total} lecture(s)</b> available\n"
        "\n"
        "Tap a lecture to receive its PDF, attachments, and AI study tools.\n"
        "\n"
        "📎 <i>To add a new lecture here, send a PDF and tap 📂 Upload.</i>"
    )
    if can_manage:
        text += "\n\n⚙️ <i>Use Manage to add attachments or delete lectures.</i>"
    return text


def manage_menu_text(block: str, branch: str, total: int) -> str:
    return (
        "⚙️ <b>Manage Lectures</b>\n"
        "\n"
        f"{_where(block, branch)}\n"
        "\n"
        f"{DIVIDER}\n"
        "\n"
        f"📚 <b>{total} lecture(s)</b> in this branch\n"
        "\n"
        "Tap any lecture below to add attachments, preview files, or delete it.\n"
        "\n"
        "💡 <i>Every action is scoped to your account only.</i>"
    )


def attachment_label(att: sqlite3.Row) -> str:
    kind = att["kind"]
    icon = KIND_ICONS.get(kind, "📎")
    if kind == "text":
        return f"{icon} {_shorten(att['text'] or 'Note', 40)}"
    if kind == "photo":
        detail = "Photo"
    elif kind == "voice":
        detail = "Voice note"
    else:
        detail = att["file_name"] or kind.title()
    if att["caption"]:
        detail += f" — {att['caption']}"
    return f"{icon} {_shorten(detail, 40)}"


def panel_text(
    lecture: sqlite3.Row, rows: Sequence[sqlite3.Row], total: int, page: int, pages: int
) -> str:
    lines = [
        "⚙️ <b>Manage Lecture</b>",
        "",
        _where(lecture["block"], lecture["subject"]),
        "",
        DIVIDER,
        "",
        f"📄 <b>{escape_html(lecture_title(lecture['number'], lecture['file_name']))}</b>",
        "",
    ]
    if total == 0:
        lines += [
            DIVIDER,
            "",
            "📭 <b>No attachments yet</b>",
            "",
            "You can add images, mind maps, files, media, or text notes.",
            "",
            "➕ <i>Tap the Add button below to begin.</i>",
        ]
    else:
        pager = f"  ·  page {page + 1}/{pages}" if pages > 1 else ""
        lines += [
            DIVIDER,
            "",
            f"📎 <b>Attachments ({total})</b>{pager}",
            "",
        ]
        for i, att in enumerate(rows, start=page * ATT_PAGE_SIZE + 1):
            lines.append(f"▪️ {i}. {escape_html(attachment_label(att))}")
            lines.append("")
        lines.append("🗑 <i>Tap the 🗑 button on an item to remove it.</i>")
    return "\n".join(lines).rstrip()


def panel_keyboard(
    lecture: sqlite3.Row, rows_data: Sequence[sqlite3.Row], page: int, pages: int
) -> InlineKeyboardMarkup:
    lid = lecture["id"]
    rows: list[list[InlineKeyboardButton]] = [
        [InlineKeyboardButton("➕ Add attachment / note", callback_data=f"{CB_ATTACH}:{lid}")]
    ]
    for i, att in enumerate(rows_data, start=page * ATT_PAGE_SIZE + 1):
        rows.append(
            [
                InlineKeyboardButton(
                    _shorten(f"🗑 {i}. {attachment_label(att)}", BUTTON_LABEL_MAX),
                    callback_data=f"{CB_ATT_ASK}:{att['id']}",
                )
            ]
        )
    pager = _pager_row(page, pages, lambda p: f"{CB_PANEL}:{lid}:{p}")
    if pager:
        rows.append(pager)
    rows.append([InlineKeyboardButton("🗑 Delete this lecture", callback_data=f"{CB_LEC_ASK}:{lid}")])

    list_page = (lecture["number"] - 1) // PAGE_SIZE
    rows.append(_nav_row(f"{CB_MANAGE}:{lecture['block']}:{lecture['subject']}:{list_page}"))
    return InlineKeyboardMarkup(rows)


def attach_prompt_text(lecture: sqlite3.Row) -> str:
    return (
        "➕ <b>Add Attachments</b>\n"
        "\n"
        f"📄 <b>{escape_html(lecture_title(lecture['number'], lecture['file_name']))}</b>\n"
        "\n"
        f"{DIVIDER}\n"
        "\n"
        "⭐ <b>Send me now, in this chat:</b>\n"
        "\n"
        "▪️ 🖼 Study images / mind maps\n"
        "\n"
        "▪️ 📄 Any file (PDF, Word, ...)\n"
        "\n"
        "▪️ 🎬 Video  ·  🎤 Voice  ·  🎵 Audio\n"
        "\n"
        "▪️ 📝 A text note (just type it and send)\n"
        "\n"
        f"{DIVIDER}\n"
        "\n"
        "✅ <i>Send as many items as you like, then tap Done.</i>"
    )


def done_keyboard(lecture_id: int, with_nav: bool = False) -> InlineKeyboardMarkup:
    rows = [[InlineKeyboardButton("✅ Done", callback_data=f"{CB_PANEL}:{lecture_id}:{PAGE_LAST}")]]
    if with_nav:
        rows.append(_nav_row(f"{CB_PANEL}:{lecture_id}:0"))
    return InlineKeyboardMarkup(rows)


def confirm_attachment_text(att: sqlite3.Row, lecture: sqlite3.Row) -> str:
    return (
        "🗑 <b>Remove Attachment?</b>\n"
        "\n"
        f"{DIVIDER}\n"
        "\n"
        f"📄 <b>From:</b> {escape_html(lecture_title(lecture['number'], lecture['file_name']))}\n"
        "\n"
        f"📎 <b>Item:</b> {att['position']}. {escape_html(attachment_label(att))}\n"
        "\n"
        f"{DIVIDER}\n"
        "\n"
        "⚠️ Not sure which one it is?\n"
        "\n"
        "Tap 👁 Preview first to see it before removing.\n"
        "\n"
        "🔴 <i>This action cannot be undone.</i>"
    )


def confirm_attachment_keyboard(att: sqlite3.Row) -> InlineKeyboardMarkup:
    lid, aid = att["lecture_id"], att["id"]
    back = f"{CB_PANEL}:{lid}:{(att['position'] - 1) // ATT_PAGE_SIZE}"
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("👁 Preview", callback_data=f"{CB_ATT_VIEW}:{aid}")],
            [
                InlineKeyboardButton("🗑 Yes, remove", callback_data=f"{CB_ATT_DEL}:{lid}:{aid}"),
                InlineKeyboardButton("❌ Cancel", callback_data=back),
            ],
            _nav_row(back),
        ]
    )


def confirm_lecture_text(lecture: sqlite3.Row) -> str:
    extra = (
        f"⚠️ It has <b>{lecture['att_count']} attachment(s)</b> that will be deleted too.\n\n"
        if lecture["att_count"]
        else ""
    )
    return (
        "⚠️ <b>Delete This Lecture?</b>\n"
        "\n"
        f"{DIVIDER}\n"
        "\n"
        f"📄 <b>{escape_html(lecture_title(lecture['number'], lecture['file_name']))}</b>\n"
        "\n"
        f"{_where(lecture['block'], lecture['subject'])}\n"
        "\n"
        f"{DIVIDER}\n"
        "\n"
        f"{extra}"
        "🔢 <i>Later lectures will be renumbered automatically.</i>\n"
        "\n"
        "🔴 <b>This action cannot be undone.</b>"
    )


def confirm_lecture_keyboard(lecture: sqlite3.Row) -> InlineKeyboardMarkup:
    lid = lecture["id"]
    back = f"{CB_PANEL}:{lid}:0"
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("🗑 Yes, delete", callback_data=f"{CB_LEC_DEL}:{lid}"),
                InlineKeyboardButton("❌ Cancel", callback_data=back),
            ],
            _nav_row(back),
        ]
    )


def ai_actions_keyboard(lecture_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("📝 Summary", callback_data=f"{CB_AI}:sum:{lecture_id}"),
                InlineKeyboardButton("❓ MCQs", callback_data=f"{CB_AI}:mcq:{lecture_id}"),
            ],
            [
                InlineKeyboardButton("🃏 Flashcards", callback_data=f"{CB_AI}:flash:{lecture_id}"),
                InlineKeyboardButton("💬 Ask about this", callback_data=f"{CB_AI}:ask:{lecture_id}"),
            ],
            [
                InlineKeyboardButton("📂 Upload new PDF", callback_data=CB_UPLOAD_NEW),
            ],
        ]
    )


def pending_pdf_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("📝 Summary", callback_data=f"{CB_PEND}:sum"),
                InlineKeyboardButton("🤖 MCQs", callback_data=f"{CB_PEND}:mcq"),
            ],
            [
                InlineKeyboardButton("📑 Flashcards", callback_data=f"{CB_PEND}:flash"),
                InlineKeyboardButton("💬 Ask about this / Infographic", callback_data=f"{CB_PEND}:ask"),
            ],
            [
                InlineKeyboardButton("📂 Upload to Archive", callback_data=f"{CB_PEND}:upload"),
            ],
        ]
    )


def _diff_emoji(difficulty: str) -> str:
    return {"easy": "🟢", "medium": "🟡", "hard": "🔴"}.get(difficulty, "⚪")


def _quiz_initial_keyboard(quiz_id: str) -> InlineKeyboardMarkup:
    buttons = [
        InlineKeyboardButton(f"{chr(65 + i)}", callback_data=f"{CB_QUIZ_ANS}:{quiz_id}:{i}")
        for i in range(5)
    ]
    return InlineKeyboardMarkup([buttons])


def _quiz_result_keyboard(q: dict, selected_idx: int, is_correct: bool) -> InlineKeyboardMarkup:
    buttons = []
    for i in range(5):
        if i == q["correct"]:
            label = f"🟢 {chr(65 + i)}"
        elif i == selected_idx and not is_correct:
            label = f"🔴 {chr(65 + i)}"
        else:
            label = f"⚪ {chr(65 + i)}"
        buttons.append(InlineKeyboardButton(label, callback_data=CB_NOOP))
    return InlineKeyboardMarkup([buttons])


def _format_quiz_initial(num: int, q: dict) -> str:
    lines = [
        f"<b>Q{num}.</b>  {_diff_emoji(q['difficulty'])}  <b>{q['difficulty'].upper()}</b>",
        "",
        DIVIDER,
        "",
        f"<b>{escape_html(q['question'])}</b>",
        "",
    ]
    for i, opt in enumerate(q["options"]):
        lines.append(f"⚪  <b>{chr(65 + i)})</b>  {escape_html(opt)}")
        lines.append("")
    lines += [
        DIVIDER,
        "",
        "👇 <i>Tap an option below to lock in your answer.</i>",
    ]
    return "\n".join(lines)


def _extract_explanation_snippet(explanation: str, max_len: int = 180) -> str:
    if not explanation:
        return ""
    exp = explanation.strip()
    if not exp:
        return ""
    m = re.match(r"^[^.!?؟]+[.!?؟]", exp)
    if m:
        result = m.group(0).strip()
        if len(result) <= max_len:
            return result
        return result[:max_len].rstrip() + "…"
    if len(exp) <= max_len:
        return exp
    return exp[:max_len].rstrip() + "…"


def _format_quiz_result(q: dict, selected_idx: int, is_correct: bool) -> str:
    correct_letter = chr(65 + q["correct"])
    correct_text = escape_html(q["options"][q["correct"]])

    lines: list[str] = [
        f"<b>Q{q['num']}.</b>  {_diff_emoji(q['difficulty'])}  <b>{q['difficulty'].upper()}</b>",
        "",
        DIVIDER,
        "",
        f"<b>{escape_html(q['question'])}</b>",
        "",
    ]
    for i, opt in enumerate(q["options"]):
        if i == q["correct"]:
            mark = "🟢"
        elif i == selected_idx and not is_correct:
            mark = "🔴"
        else:
            mark = "⚪"
        suffix = "  <i>← your answer</i>" if i == selected_idx else ""
        lines.append(f"{mark}  <b>{chr(65 + i)})</b>  {escape_html(opt)}{suffix}")
        lines.append("")

    lines += [DIVIDER, ""]
    if is_correct:
        lines.append("✅ <b>Correct — well done!</b>")
    else:
        lines.append(
            f"❌ <b>Incorrect.</b>  You picked <b>{chr(65 + selected_idx)})</b>"
        )
    lines.append("")
    lines.append(
        f"🟢 <b>Correct answer:</b>  <b>{correct_letter})</b>  {correct_text}"
    )

    expl_text = (q.get("explanation") or "").strip()
    if expl_text:
        lines += [
            "",
            DIVIDER,
            "",
            f"💡 <b>Why {correct_letter}) is correct:</b>",
            "",
            escape_html(expl_text),
        ]

    distractors = q.get("distractors") or []
    explanation_snippet = _extract_explanation_snippet(expl_text)
    correct_option_text = q["options"][q["correct"]]

    distractor_lines: list[str] = []
    for i, opt in enumerate(q["options"]):
        if i == q["correct"]:
            continue
        reason = distractors[i] if i < len(distractors) else ""
        cleaned = _clean_distractor_reason(reason or "", opt, correct_option_text)
        if not cleaned:
            cleaned = _build_fallback_reason(opt, correct_letter, explanation_snippet)
        distractor_lines.append(
            f"❌ <b>{chr(97 + i)})</b> {escape_html(cleaned)}"
        )

    if distractor_lines:
        lines += [
            "",
            DIVIDER,
            "",
            "🔍 <b>لماذا الخيارات الأخرى خاطئة:</b>",
            "",
        ]
        for dl in distractor_lines:
            lines.append(dl)
            lines.append("")

    return "\n".join(lines).rstrip()

# ==============================================================================
# 11. TELEGRAM HELPERS
# ==============================================================================

_ATTACH_LOCKS: dict[int, asyncio.Lock] = {}


def _get_attach_lock(user_id: int) -> asyncio.Lock:
    lock = _ATTACH_LOCKS.get(user_id)
    if lock is not None:
        return lock
    if len(_ATTACH_LOCKS) >= ATTACH_LOCKS_MAX_ENTRIES:
        target = ATTACH_LOCKS_MAX_ENTRIES // 2
        for k in list(_ATTACH_LOCKS.keys()):
            if len(_ATTACH_LOCKS) <= target:
                break
            lk = _ATTACH_LOCKS.get(k)
            if lk is not None and not lk.locked():
                _ATTACH_LOCKS.pop(k, None)
    lock = asyncio.Lock()
    _ATTACH_LOCKS[user_id] = lock
    return lock


async def safe_answer(query, text: str | None = None, show_alert: bool = False) -> None:
    try:
        await query.answer(text=text, show_alert=show_alert)
    except TelegramError as exc:
        logger.debug("Could not answer callback query: %s", exc)


async def render(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    text: str,
    markup: InlineKeyboardMarkup,
) -> None:
    query = update.callback_query
    try:
        await query.edit_message_text(text=text, reply_markup=markup, parse_mode=ParseMode.HTML)
    except BadRequest as exc:
        if "not modified" in str(exc).lower():
            return
        logger.warning("edit_message_text failed (%s); sending a new message instead", exc)
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text=text,
            reply_markup=markup,
            parse_mode=ParseMode.HTML,
        )


def reset_nav_modes(context: ContextTypes.DEFAULT_TYPE) -> None:
    for key in (LOC_KEY, ATTACH_KEY, GROUP_KEY):
        context.user_data.pop(key, None)


def reset_all_modes(context: ContextTypes.DEFAULT_TYPE) -> None:
    for key in (
        LOC_KEY, ATTACH_KEY, GROUP_KEY,
        PDF_TEXT_KEY, PDF_NAME_KEY, PDF_DOC_HASH_KEY,
        PDF_QA_MODE_KEY, PDF_QA_SOURCE_KEY,
        AI_LECTURE_KEY, HISTORY_KEY,
        PENDING_PDF_KEY, PENDING_UPLOAD_MODE_KEY, QUIZ_CACHE_KEY,
    ):
        context.user_data.pop(key, None)


def is_admin(update: Update) -> bool:
    user = update.effective_user
    return user is not None and is_activated(user.id)


async def require_admin(update: Update) -> bool:
    user = update.effective_user
    if user is not None and is_activated(user.id):
        return True
    await safe_answer(
        update.callback_query, "🔐 Please activate the bot first (send /start).", show_alert=True
    )
    return False


def cb_ints(data: str, required: int, optional: int = 0) -> list[int] | None:
    parts = data.split(":")[1:]
    if not required <= len(parts) <= required + optional:
        return None
    try:
        return [int(p) for p in parts]
    except ValueError:
        return None


def parse_location(data: str) -> tuple[str, str, int] | None:
    try:
        _, block, branch, page_raw = data.split(":")
        page = int(page_raw)
    except ValueError:
        logger.warning("Malformed location callback: %r", data)
        return None
    if block not in BLOCKS or branch not in BRANCHES:
        return None
    return block, branch, page


def first_of_album(context: ContextTypes.DEFAULT_TYPE, message: Message) -> bool:
    group_id = message.media_group_id
    if not group_id:
        return True
    is_first = context.user_data.get(GROUP_KEY) != group_id
    context.user_data[GROUP_KEY] = group_id
    return is_first


def extract_attachment(message: Message) -> AttachmentData | None:
    caption = (message.caption or "").strip() or None
    if message.photo:
        photo = message.photo[-1]
        return AttachmentData("photo", photo.file_id, photo.file_unique_id, None, caption)
    if message.document:
        doc = message.document
        return AttachmentData("document", doc.file_id, doc.file_unique_id, doc.file_name, caption)
    if message.video:
        video = message.video
        return AttachmentData("video", video.file_id, video.file_unique_id, video.file_name, caption)
    if message.audio:
        audio = message.audio
        return AttachmentData(
            "audio", audio.file_id, audio.file_unique_id, audio.file_name or audio.title, caption
        )
    if message.voice:
        voice = message.voice
        return AttachmentData("voice", voice.file_id, voice.file_unique_id, None, caption)
    if message.text and message.text.strip():
        return AttachmentData("text", text=message.text.strip())
    return None


def _retry_seconds(exc: RetryAfter) -> float:
    value = exc.retry_after
    return value.total_seconds() if isinstance(value, timedelta) else float(value)


async def _call(method, /, **kwargs):
    try:
        return await method(**kwargs)
    except RetryAfter as exc:
        wait = min(_retry_seconds(exc) + 1, MAX_FLOOD_WAIT)
        logger.warning("Flood control hit - waiting %.0fs", wait)
        await asyncio.sleep(wait)
        return await method(**kwargs)


async def send_attachment(bot, chat_id: int, att: sqlite3.Row) -> None:
    kind = att["kind"]
    if kind == "text":
        await _call(bot.send_message, chat_id=chat_id, text=f"📝 {att['text']}")
        return
    if kind not in MEDIA_KINDS:
        raise ValueError(f"unsupported attachment kind {kind!r}")
    sender = getattr(bot, f"send_{kind}")
    await _call(sender, chat_id=chat_id, caption=att["caption"], **{kind: att["file_id"]})


async def _send_single(bot, chat_id: int, att: sqlite3.Row) -> int:
    try:
        await send_attachment(bot, chat_id, att)
        return 0
    except (TelegramError, ValueError):
        logger.exception("Could not send attachment %s (%s)", att["id"], att["kind"])
        return 1


async def _send_photo_batch(bot, chat_id: int, photos: Sequence[sqlite3.Row]) -> int:
    if len(photos) == 1:
        return await _send_single(bot, chat_id, photos[0])
    media = [InputMediaPhoto(media=p["file_id"], caption=p["caption"]) for p in photos]
    try:
        await _call(bot.send_media_group, chat_id=chat_id, media=media)
        return 0
    except TelegramError:
        logger.warning("Album send failed; falling back to single photos", exc_info=True)
        return sum([await _send_single(bot, chat_id, p) for p in photos])


async def send_lecture_bundle(
    bot, chat_id: int, lecture: sqlite3.Row, attachments: Sequence[sqlite3.Row]
) -> None:
    try:
        await _call(
            bot.send_document,
            chat_id=chat_id,
            document=lecture["file_id"],
            caption=f"📄 {lecture['file_name']}"[:1000],
            reply_markup=ai_actions_keyboard(lecture["id"]),
            read_timeout=REQUEST_TIMEOUT_SECONDS,
            write_timeout=REQUEST_TIMEOUT_SECONDS,
            connect_timeout=REQUEST_TIMEOUT_SECONDS,
        )
    except TelegramError:
        logger.exception("Could not send lecture %s", lecture["id"])
        await bot.send_message(
            chat_id=chat_id,
            text="⚠️ Sorry, I couldn't send this file. Please upload it again.",
        )

    failed = 0
    photos: list[sqlite3.Row] = []
    for att in attachments:
        if att["kind"] == "photo":
            photos.append(att)
            if len(photos) == MEDIA_GROUP_MAX:
                failed += await _send_photo_batch(bot, chat_id, photos)
                photos = []
            continue
        if photos:
            failed += await _send_photo_batch(bot, chat_id, photos)
            photos = []
        failed += await _send_single(bot, chat_id, att)
    if photos:
        failed += await _send_photo_batch(bot, chat_id, photos)

    if failed:
        await bot.send_message(
            chat_id=chat_id,
            text=f"⚠️ {failed} attachment(s) could not be sent.",
        )


async def _send_task_menu_footer(
    bot,
    chat_id: int,
    markup: InlineKeyboardMarkup,
    *,
    header: str = "📋 <b>Task complete!</b>",
    hint: str = "👇 <i>Pick your next study tool.</i>",
) -> None:
    try:
        await bot.send_message(
            chat_id=chat_id,
            text=(
                f"{header}\n"
                "\n"
                f"{DIVIDER}\n"
                "\n"
                f"{hint}"
            ),
            reply_markup=markup,
            parse_mode=ParseMode.HTML,
        )
    except TelegramError:
        logger.exception("Could not send task-completion footer")

# ==============================================================================
# 12. ACTIVATION FLOW
# ==============================================================================

ACTIVATION_PROMPT = (
    "🔒 <b>Access Restricted</b>\n"
    "\n"
    f"{DIVIDER}\n"
    "\n"
    "⭐ This bot is protected and requires an activation key.\n"
    "\n"
    "Please enter the valid <b>activation key</b> provided by the administrator to unlock all features.\n"
    "\n"
    f"{DIVIDER}\n"
    "\n"
    "💡 <i>Don't have a key? Contact the bot administrator.</i>"
)

WELCOME_MESSAGE = (
    "🎉 <b>Welcome to your Medical Intelligence Hub!</b> 🏥\n"
    "\n"
    f"{DIVIDER}\n"
    "\n"
    "✨ <b>Activation successful — you're all set!</b> ✨\n"
    "\n"
    f"{DIVIDER}\n"
    "\n"
    "🧠 <b>Here's what you can now do:</b>\n"
    "\n"
    "📚 <b>Build your personal archive</b>\n"
    "\n"
    "▪️ Upload PDFs to CNS 🧠 · RSP 🫁 · RNL 🫘 blocks\n"
    "\n"
    "▪️ Organize across 8 medical branches\n"
    "\n"
    "📝 <b>High-yield summaries</b>\n"
    "\n"
    "▪️ One tap turns any lecture into a structured study guide\n"
    "\n"
    "❓ <b>Interactive MCQs</b>\n"
    "\n"
    "▪️ 5-question quizzes with green / red feedback\n"
    "\n"
    "▪️ Comprehensive explanations plus per-distractor rationale\n"
    "\n"
    "📑 <b>Flashcards</b>\n"
    "\n"
    "▪️ 10 Anki-style cards per lecture\n"
    "\n"
    "💬 <b>Ask about this lecture</b>\n"
    "\n"
    "▪️ Free-form Q&A grounded in your uploaded PDF\n"
    "\n"
    "🔒 <b>Your data is 100% private</b>\n"
    "\n"
    "▪️ Every lecture, note and attachment stays in your own isolated workspace\n"
    "\n"
    "▪️ Never shared with other users\n"
    "\n"
    f"{DIVIDER}\n"
    "\n"
    "🚀 <i>Let's begin your medical journey!</i>"
)


async def _show_welcome(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(WELCOME_MESSAGE, parse_mode=ParseMode.HTML)
    await asyncio.sleep(0.4)
    await update.message.reply_text(
        "🏥 <b>Choose your block to begin:</b>\n\n👇 <i>Tap one below.</i>",
        reply_markup=main_menu_keyboard(),
        parse_mode=ParseMode.HTML,
    )


async def activation_gate(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    if user is None:
        return

    if update.callback_query is not None:
        if not _callback_allowed(user.id):
            await safe_answer(update.callback_query, "⏳ Slow down a moment, please.")
            raise ApplicationHandlerStop

    if user.id in ADMIN_IDS:
        return

    if is_activated(user.id):
        return

    message = update.effective_message
    incoming_text = (message.text or "").strip() if message and message.text else ""

    if incoming_text == BOT_ACTIVATION_KEY:
        try:
            db_activate_user(user.id, user.username, user.first_name)
            logger.info("User %s (%s) activated successfully", user.id, user.username)
        except sqlite3.Error:
            logger.exception("DB error while activating user %s", user.id)
            try:
                await context.bot.send_message(
                    chat_id=update.effective_chat.id,
                    text="⚠️ Database error during activation. Please try again.",
                )
            except TelegramError:
                pass
            raise ApplicationHandlerStop

        try:
            await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text=WELCOME_MESSAGE,
                parse_mode=ParseMode.HTML,
            )
            await asyncio.sleep(0.4)
            await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text="🏥 <b>Choose your block to begin:</b>\n\n👇 <i>Tap one below.</i>",
                reply_markup=main_menu_keyboard(),
                parse_mode=ParseMode.HTML,
            )
        except TelegramError:
            logger.exception("Could not send welcome to user %s", user.id)
        raise ApplicationHandlerStop

    if update.callback_query:
        await safe_answer(update.callback_query, "🔒 Access Restricted", show_alert=True)
        try:
            await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text=ACTIVATION_PROMPT,
                parse_mode=ParseMode.HTML,
            )
        except TelegramError:
            pass
    elif message is not None:
        try:
            await message.reply_text(ACTIVATION_PROMPT, parse_mode=ParseMode.HTML)
        except TelegramError:
            pass
    raise ApplicationHandlerStop


async def _gate(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    user = update.effective_user
    if user is not None and is_activated(user.id):
        return True
    if update.callback_query:
        await safe_answer(update.callback_query, "🔐 Activation required.", show_alert=True)
        try:
            await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text=ACTIVATION_PROMPT,
                parse_mode=ParseMode.HTML,
            )
        except TelegramError:
            pass
    elif update.effective_message:
        await update.effective_message.reply_text(ACTIVATION_PROMPT, parse_mode=ParseMode.HTML)
    return False

# ==============================================================================
# 13. PDF TEXT LOADERS
# ==============================================================================


async def _load_lecture_pdf_text(
    update: Update, context: ContextTypes.DEFAULT_TYPE, lecture: sqlite3.Row
) -> str | None:
    cache_key = f"lec:{lecture['id']}"

    cached_id = context.user_data.get(AI_LECTURE_KEY)
    cached_text = context.user_data.get(PDF_TEXT_KEY)
    if cached_id == lecture["id"] and cached_text:
        _PDF_TEXT_LRU.put(cache_key, cached_text)
        doc_hash = context.user_data.get(PDF_DOC_HASH_KEY) or lecture["doc_hash"]
        if doc_hash:
            _schedule_precompute(cached_text, doc_hash)
        return cached_text

    lru_hit = _PDF_TEXT_LRU.get(cache_key)
    if lru_hit:
        context.user_data[PDF_TEXT_KEY] = lru_hit
        context.user_data[PDF_NAME_KEY] = lecture["file_name"]
        context.user_data[AI_LECTURE_KEY] = lecture["id"]
        doc_hash = lecture["doc_hash"] or compute_text_hash(lru_hit)
        context.user_data[PDF_DOC_HASH_KEY] = doc_hash
        _schedule_precompute(lru_hit, doc_hash)
        return lru_hit

    status_msg = await update.effective_message.reply_text("📥 Downloading lecture PDF...")
    try:
        tg_file = await _call(
            context.bot.get_file,
            file_id=lecture["file_id"],
            read_timeout=REQUEST_TIMEOUT_SECONDS,
            write_timeout=REQUEST_TIMEOUT_SECONDS,
            connect_timeout=REQUEST_TIMEOUT_SECONDS,
        )
        pdf_bytes = bytes(await tg_file.download_as_bytearray())
    except TelegramError as exc:
        logger.exception("Could not download lecture PDF %s", lecture["id"])
        await status_msg.edit_text(f"⚠️ Could not download PDF: {exc}")
        return None

    doc_hash = compute_doc_hash(pdf_bytes)
    if not lecture["doc_hash"]:
        try:
            await _run_cpu(db_set_lecture_doc_hash, lecture["id"], doc_hash)
        except sqlite3.Error:
            logger.exception("Could not persist doc_hash for lecture %s", lecture["id"])

    try:
        text = await _run_process(extract_text_from_pdf, pdf_bytes)
    except Exception:
        logger.exception("PDF extraction failed for lecture %s", lecture["id"])
        await status_msg.edit_text("⚠️ Could not read this PDF. It may be scanned or encrypted.")
        return None

    if not text.strip():
        await status_msg.edit_text(
            "⚠️ No selectable text in this PDF (scanned document). AI requires a text-based PDF."
        )
        return None

    context.user_data[PDF_TEXT_KEY] = text
    context.user_data[PDF_NAME_KEY] = lecture["file_name"]
    context.user_data[PDF_DOC_HASH_KEY] = doc_hash
    context.user_data[AI_LECTURE_KEY] = lecture["id"]
    _PDF_TEXT_LRU.put(cache_key, text)

    _schedule_precompute(text, doc_hash)

    try:
        await status_msg.edit_text(f"✅ Extracted {len(text):,} characters from this PDF.")
    except TelegramError:
        pass
    return text


async def _load_pending_pdf_text(
    update: Update, context: ContextTypes.DEFAULT_TYPE, pending: dict
) -> str | None:
    cache_key = f"pend:{pending['file_unique_id']}"

    if (
        context.user_data.get(AI_LECTURE_KEY) == pending["file_unique_id"]
        and context.user_data.get(PDF_TEXT_KEY)
    ):
        cached = context.user_data[PDF_TEXT_KEY]
        _PDF_TEXT_LRU.put(cache_key, cached)
        doc_hash = context.user_data.get(PDF_DOC_HASH_KEY)
        if doc_hash:
            _schedule_precompute(cached, doc_hash)
        return cached

    lru_hit = _PDF_TEXT_LRU.get(cache_key)
    if lru_hit:
        context.user_data[PDF_TEXT_KEY] = lru_hit
        context.user_data[PDF_NAME_KEY] = pending["file_name"]
        context.user_data[AI_LECTURE_KEY] = pending["file_unique_id"]
        doc_hash = context.user_data.get(PDF_DOC_HASH_KEY) or compute_text_hash(lru_hit)
        context.user_data[PDF_DOC_HASH_KEY] = doc_hash
        _schedule_precompute(lru_hit, doc_hash)
        return lru_hit

    status_msg = await update.effective_message.reply_text("📥 Downloading PDF...")
    try:
        tg_file = await _call(
            context.bot.get_file,
            file_id=pending["file_id"],
            read_timeout=REQUEST_TIMEOUT_SECONDS,
            write_timeout=REQUEST_TIMEOUT_SECONDS,
            connect_timeout=REQUEST_TIMEOUT_SECONDS,
        )
        pdf_bytes = bytes(await tg_file.download_as_bytearray())
    except TelegramError as exc:
        logger.exception("Could not download pending PDF")
        await status_msg.edit_text(f"⚠️ Could not download PDF: {exc}")
        return None

    doc_hash = compute_doc_hash(pdf_bytes)

    try:
        text = await _run_process(extract_text_from_pdf, pdf_bytes)
    except Exception:
        logger.exception("PDF extraction failed for pending PDF")
        await status_msg.edit_text("⚠️ Could not read this PDF. It may be scanned or encrypted.")
        return None

    if not text.strip():
        await status_msg.edit_text(
            "⚠️ No selectable text in this PDF (scanned document). AI requires a text-based PDF."
        )
        return None

    context.user_data[PDF_TEXT_KEY] = text
    context.user_data[PDF_NAME_KEY] = pending["file_name"]
    context.user_data[PDF_DOC_HASH_KEY] = doc_hash
    context.user_data[AI_LECTURE_KEY] = pending["file_unique_id"]
    _PDF_TEXT_LRU.put(cache_key, text)

    _schedule_precompute(text, doc_hash)

    try:
        await status_msg.edit_text(f"✅ Extracted {len(text):,} characters from this PDF.")
    except TelegramError:
        pass
    return text

# ==============================================================================
# 14. QUIZ ENGINE
# ==============================================================================


async def send_quiz(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    questions: list[dict],
    source_id: str,
) -> None:
    chat_id = update.effective_chat.id
    cache: dict = context.user_data.setdefault(QUIZ_CACHE_KEY, {})

    for k in [k for k, v in cache.items() if v.get("source_id") == source_id]:
        del cache[k]

    for num, q in enumerate(questions, start=1):
        quiz_id = uuid.uuid4().hex[:12]
        cache[quiz_id] = {
            "num": num,
            "source_id": source_id,
            "difficulty": q["difficulty"],
            "question": q["question"],
            "options": q["options"],
            "correct": q["correct"],
            "explanation": q["explanation"],
            "distractors": q.get("distractors") or ["", "", "", "", ""],
            "answered": False,
        }
        if len(cache) > 100:
            del cache[next(iter(cache))]

        try:
            await context.bot.send_message(
                chat_id=chat_id,
                text=_format_quiz_initial(num, q),
                reply_markup=_quiz_initial_keyboard(quiz_id),
                parse_mode=ParseMode.HTML,
            )
        except TelegramError:
            logger.exception("Could not send quiz question %d", num)
        await asyncio.sleep(0.35)


async def cb_quiz_answer(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    parts = query.data.split(":")
    if len(parts) != 3:
        await safe_answer(query, "⚠️ Invalid.")
        return
    quiz_id = parts[1]
    try:
        opt_idx = int(parts[2])
    except ValueError:
        await safe_answer(query, "⚠️ Invalid.")
        return
    if not 0 <= opt_idx <= 4:
        await safe_answer(query, "⚠️ Invalid.")
        return

    cache: dict = context.user_data.get(QUIZ_CACHE_KEY, {})
    q = cache.get(quiz_id)
    if q is None:
        await safe_answer(query, "ℹ️ This quiz has expired.", show_alert=True)
        return
    if q["answered"]:
        await safe_answer(query, "ℹ️ Already answered.")
        return

    q["answered"] = True
    is_correct = (opt_idx == q["correct"])

    await safe_answer(query, "✅ Correct!" if is_correct else "❌ Incorrect")

    try:
        await query.edit_message_text(
            text=_format_quiz_result(q, opt_idx, is_correct),
            reply_markup=_quiz_result_keyboard(q, opt_idx, is_correct),
            parse_mode=ParseMode.HTML,
        )
    except BadRequest as exc:
        logger.warning("Could not edit quiz message: %s", exc)

# ==============================================================================
# 15. HANDLERS — Commands
# ==============================================================================


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await _gate(update, context):
        return
    reset_nav_modes(context)
    await update.effective_message.reply_text(
        MAIN_MENU_TEXT, reply_markup=main_menu_keyboard(), parse_mode=ParseMode.HTML
    )


async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await _gate(update, context):
        return
    await update.effective_message.reply_text(
        "ℹ️ <b>دليل المستخدم — بوت الأرشيف الطبي</b>\n"
        "\n"
        f"{DIVIDER}\n"
        "\n"
        "⭐ <b>البدء السريع</b>\n"
        "\n"
        "▪️ 1. اضغط /start لفتح القائمة الرئيسية.\n"
        "\n"
        "▪️ 2. اختر البلوك والفرع المطلوب.\n"
        "\n"
        "▪️ 3. أرسل ملف PDF مباشرة في الشات لتفعيل 5 إجراءات دراسية.\n"
        "\n"
        f"{DIVIDER}\n"
        "\n"
        "📄 <b>إجراءات الـ PDF الخمسة</b>\n"
        "\n"
        "▪️ 📝 <b>ملخص</b> — الزتونة الطبية في نقاط مركزة (high-yield).\n"
        "\n"
        "▪️ 🤖 <b>MCQs</b> — 5 أسئلة تفاعلية مع شرح وافٍ لكل خيار.\n"
        "\n"
        "▪️ 📑 <b>بطاقات</b> — 10 بطاقات بأسلوب Anki.\n"
        "\n"
        "▪️ 💬 <b>اسأل عن المحاضرة</b> — أسئلة حرة على محتوى الـ PDF.\n"
        "\n"
        "▪️ 📂 <b>رفع للأرشيف</b> — حفظ بترقيم تلقائي متسلسل.\n"
        "\n"
        f"{DIVIDER}\n"
        "\n"
        "❓ <b>تفاصيل اختبار MCQs</b>\n"
        "\n"
        "▪️ 5 أسئلة لكل محاضرة: 2 صعب · 1 متوسط · 2 سهل.\n"
        "\n"
        "▪️ اضغط على أي خيار للتقييم: 🟢 صح · 🔴 خطأ · ⚪ لم يُجب.\n"
        "\n"
        "▪️ الشرح الوفير يظهر دائمًا: \"💡 لماذا الإجابة صحيحة\".\n"
        "\n"
        "▪️ تحليل مختصر وفريد لكل خيار خاطئ بصيغة \"❌ a) سبب مختصر\".\n"
        "\n"
        f"{DIVIDER}\n"
        "\n"
        "⚙️ <b>لوحة الإدارة</b>\n"
        "\n"
        "▪️ إضافة مرفقات وصور وملاحظات نصية.\n"
        "\n"
        "▪️ معاينة المرفقات قبل حذفها.\n"
        "\n"
        "▪️ حذف محاضرات كاملة (إعادة ترقيم تلقائي للباقي).\n"
        "\n"
        f"{DIVIDER}\n"
        "\n"
        "🔒 <b>الخصوصية</b>\n"
        "\n"
        "▪️ كل محاضرة ترفعها مخزنة في مساحتك الخاصة فقط.\n"
        "\n"
        "▪️ لا يمكن لأي مستخدم آخر رؤية أو فتح أو حذف محتواك.\n"
        "\n"
        f"{DIVIDER}\n"
        "\n"
        "💡 <b>الأوامر</b>\n"
        "\n"
        "▪️ /start — القائمة الرئيسية.\n"
        "\n"
        "▪️ /help — هذا الدليل.\n"
        "\n"
        "▪️ /reset — مسح الجلسة وحالة الذكاء الاصطناعي.\n"
        "\n"
        f"{DIVIDER}\n"
        "\n"
        "⚠️ <b>تنبيه مهم</b>\n"
        "\n"
        "▪️ تأكد من رفع ملفات PDF نصية (غير ممسوحة ضوئيًا) للحصول على نتائج AI.\n"
        "\n"
        "▪️ الملفات الممسوحة ضوئيًا لن تنتج نصًا قابلًا للقراءة.\n"
        "\n"
        f"{DIVIDER}",
        parse_mode=ParseMode.HTML,
    )


async def cmd_reset(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await _gate(update, context):
        return
    reset_all_modes(context)
    await update.effective_message.reply_text(
        "🧹 <b>Session Cleared</b>\n"
        "\n"
        f"{DIVIDER}\n"
        "\n"
        "▪️ Navigation state reset\n"
        "\n"
        "▪️ Uploaded PDF context cleared\n"
        "\n"
        "▪️ PDF Q&A mode disabled\n"
        "\n"
        "▪️ Pending uploads discarded\n"
        "\n"
        "▪️ Chat history cleared\n"
        "\n"
        f"{DIVIDER}\n"
        "\n"
        "✅ <i>You're ready for a fresh start.</i>",
        parse_mode=ParseMode.HTML,
    )

# ==============================================================================
# 16. HANDLERS — Navigation
# ==============================================================================


async def cb_home(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await _gate(update, context):
        return
    await safe_answer(update.callback_query)
    reset_nav_modes(context)
    context.user_data.pop(PENDING_UPLOAD_MODE_KEY, None)
    await render(update, context, MAIN_MENU_TEXT, main_menu_keyboard())


async def cb_block(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await _gate(update, context):
        return
    query = update.callback_query
    await safe_answer(query)

    block = query.data.split(":", 1)[1]
    if block not in BLOCKS:
        await render(update, context, MAIN_MENU_TEXT, main_menu_keyboard())
        return

    reset_nav_modes(context)
    await render(update, context, branch_menu_text(block), branch_menu_keyboard(block))


async def show_lecture_list(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    block: str,
    branch: str,
    page: int,
    *,
    manage: bool = False,
    toast: str | None = None,
) -> None:
    query = update.callback_query
    owner_id = update.effective_user.id
    try:
        rows, total, page, pages = await _run_cpu(
            db_get_lectures_page, block, branch, page, owner_id
        )
    except sqlite3.Error:
        logger.exception("DB error while listing lectures (%s / %s)", block, branch)
        await safe_answer(query, "⚠️ Database is busy, please try again.", show_alert=True)
        return

    reset_nav_modes(context)
    context.user_data[LOC_KEY] = (block, branch)

    can_manage = is_admin(update)
    manage = manage and can_manage and total > 0
    text = (
        manage_menu_text(block, branch, total)
        if manage
        else lecture_menu_text(block, branch, total, can_manage)
    )
    await safe_answer(query, toast)
    await render(
        update,
        context,
        text,
        lecture_menu_keyboard(block, branch, rows, page, pages, manage=manage, can_manage=can_manage),
    )


async def cb_branch(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await _gate(update, context):
        return
    parsed = parse_location(update.callback_query.data)
    if parsed is None:
        await safe_answer(update.callback_query)
        await render(update, context, MAIN_MENU_TEXT, main_menu_keyboard())
        return

    if context.user_data.get(PENDING_UPLOAD_MODE_KEY):
        block, branch, _ = parsed
        await _commit_pending_upload(update, context, block, branch)
        return

    await show_lecture_list(update, context, *parsed, manage=False)


async def cb_manage(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await _gate(update, context):
        return
    if not await require_admin(update):
        return
    parsed = parse_location(update.callback_query.data)
    if parsed is None:
        await safe_answer(update.callback_query)
        await render(update, context, MAIN_MENU_TEXT, main_menu_keyboard())
        return
    await show_lecture_list(update, context, *parsed, manage=True)


async def cb_lecture(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await _gate(update, context):
        return
    query = update.callback_query
    chat_id = update.effective_chat.id

    ids = cb_ints(query.data, 1)
    if ids is None:
        await safe_answer(query, "⚠️ Invalid button.", show_alert=True)
        return
    lecture_id = ids[0]
    owner_id = update.effective_user.id

    try:
        bundle = await _run_cpu(db_get_lecture_bundle, lecture_id, owner_id)
    except sqlite3.Error:
        logger.exception("DB error while loading lecture %s", lecture_id)
        await safe_answer(query, "⚠️ Database is busy, please try again.", show_alert=True)
        return

    if bundle is None:
        await safe_answer(query, "⚠️ This lecture is no longer available.", show_alert=True)
        return

    lecture, attachments = bundle
    await safe_answer(
        query,
        f"📤 Sending PDF + {len(attachments)} attachment(s)..." if attachments else "📤 Sending...",
    )
    await send_lecture_bundle(context.bot, chat_id, lecture, attachments)

# ==============================================================================
# 17. HANDLERS — Pending PDF upload
# ==============================================================================


async def cb_pending_action(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await _gate(update, context):
        return
    query = update.callback_query
    action = query.data.split(":", 1)[1]

    pending = context.user_data.get(PENDING_PDF_KEY)
    if not pending:
        await safe_answer(query, "⚠️ PDF expired — please send it again.", show_alert=True)
        return

    if action == "upload":
        await safe_answer(query)
        context.user_data[PENDING_UPLOAD_MODE_KEY] = True
        await render(
            update,
            context,
            "📂 <b>Where should this PDF go?</b>\n"
            "\n"
            f"{DIVIDER}\n"
            "\n"
            "Choose a block below, then a branch:\n"
            "\n"
            "👇 <i>Tap one of the buttons to continue.</i>",
            main_menu_keyboard(),
        )
        return

    async with _user_priority_scope():
        text = await _load_pending_pdf_text(update, context, pending)
        if text is None:
            return

        doc_hash = context.user_data.get(PDF_DOC_HASH_KEY)

        if action == "ask":
            context.user_data[PDF_QA_MODE_KEY] = True
            context.user_data[PDF_QA_SOURCE_KEY] = ("pending", None)
            if doc_hash:
                _get_or_build_index(f"doc:{doc_hash}", text)
            await safe_answer(query, "💬 Ask your question below.")
            await query.message.reply_text(
                "💬 <b>PDF Q&A Mode Enabled</b>\n"
                "\n"
                f"{DIVIDER}\n"
                "\n"
                f"📄 <b>{escape_html(pending['file_name'])}</b>\n"
                "\n"
                "Ask anything about this PDF and I'll answer using its content.\n"
                "\n"
                f"{DIVIDER}\n"
                "\n"
                "💡 <i>Send /reset when you're done.</i>",
                parse_mode=ParseMode.HTML,
                reply_markup=pending_pdf_keyboard(),
            )
            return

        await safe_answer(query, "⏳ Running AI...")

        try:
            async with typing_indicator(context.bot, update.effective_chat.id):
                if action == "sum":
                    await _stream_and_finalize(
                        context.bot,
                        update.effective_chat.id,
                        lambda on_chunk: ai_summary(text, on_chunk=on_chunk, doc_hash=doc_hash),
                        pending_pdf_keyboard(),
                        "📝 Generating structured study summary...",
                    )
                    _schedule_eager_mcqs(text, doc_hash=doc_hash)
                elif action == "flash":
                    await _stream_and_finalize(
                        context.bot,
                        update.effective_chat.id,
                        lambda on_chunk: ai_flashcards(text, on_chunk=on_chunk, doc_hash=doc_hash),
                        pending_pdf_keyboard(),
                        "🃏 Generating flashcards...",
                    )
                elif action == "mcq":
                    progress_msg = await query.message.reply_text(
                        "❓ Generating 5 interactive MCQs..."
                    )

                    last_p = [0.0]

                    async def on_mcq_progress(count: int) -> None:
                        now = time.monotonic()
                        if now - last_p[0] < 1.5:
                            return
                        last_p[0] = now
                        try:
                            await progress_msg.edit_text(
                                f"❓ Parsing questions... {count}/5 ready"
                            )
                        except BadRequest:
                            pass

                    questions = await _get_mcq_batch_for_user(
                        text, doc_hash=doc_hash, on_progress=on_mcq_progress
                    )
                    try:
                        await progress_msg.delete()
                    except TelegramError:
                        pass
                    if not questions:
                        await context.bot.send_message(
                            chat_id=update.effective_chat.id,
                            text="⚠️ Could not parse the quiz. Please try again.",
                            reply_markup=pending_pdf_keyboard(),
                        )
                        return
                    await send_quiz(
                        update,
                        context,
                        questions,
                        source_id=f"pend:{pending['file_unique_id']}",
                    )
                    await _send_task_menu_footer(
                        context.bot,
                        update.effective_chat.id,
                        pending_pdf_keyboard(),
                        header="📋 <b>Quiz ready — 5 questions above!</b>",
                        hint="👇 <i>Pick your next study tool.</i>",
                    )
                else:
                    try:
                        await query.message.reply_text("⚠️ Unknown action.")
                    except TelegramError:
                        pass
        except RuntimeError as exc:
            try:
                await query.message.reply_text(
                    str(exc), reply_markup=pending_pdf_keyboard()
                )
            except TelegramError:
                pass


async def _commit_pending_upload(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    block: str,
    branch: str,
) -> None:
    query = update.callback_query
    pending = context.user_data.get(PENDING_PDF_KEY)
    if not pending:
        await safe_answer(query, "⚠️ PDF expired — please send it again.", show_alert=True)
        context.user_data.pop(PENDING_UPLOAD_MODE_KEY, None)
        return

    user = update.effective_user
    pdf_text_cached = context.user_data.get(PDF_TEXT_KEY)
    doc_hash = context.user_data.get(PDF_DOC_HASH_KEY)
    pending_file_id = pending["file_id"]

    try:
        inserted, lecture = await _run_cpu(
            db_add_lecture,
            block,
            branch,
            pending["file_id"],
            pending["file_unique_id"],
            pending["file_name"],
            user.id,
            user.id,
        )
    except sqlite3.Error:
        logger.exception("DB error registering pending PDF upload")
        await safe_answer(query, "⚠️ Database error.", show_alert=True)
        return

    if doc_hash:
        try:
            await _run_cpu(db_set_lecture_doc_hash, lecture["id"], doc_hash)
        except sqlite3.Error:
            logger.exception("Could not persist doc_hash for lecture %s", lecture["id"])

    context.user_data.pop(PENDING_PDF_KEY, None)
    context.user_data.pop(PENDING_UPLOAD_MODE_KEY, None)
    context.user_data.pop(PDF_TEXT_KEY, None)
    context.user_data.pop(PDF_NAME_KEY, None)
    context.user_data.pop(PDF_DOC_HASH_KEY, None)
    context.user_data.pop(AI_LECTURE_KEY, None)
    context.user_data.pop(PDF_QA_MODE_KEY, None)
    context.user_data.pop(PDF_QA_SOURCE_KEY, None)

    if pdf_text_cached and pdf_text_cached.strip() and doc_hash:
        _schedule_precompute(pdf_text_cached, doc_hash)
    elif not doc_hash:
        _fire_task(_eager_download_and_generate(context.bot, pending_file_id))

    await safe_answer(query, "✅ Uploaded!")

    title = escape_html(lecture_title(lecture["number"], lecture["file_name"]))
    if inserted:
        text = (
            "✅ <b>Lecture Uploaded Successfully!</b>\n"
            "\n"
            f"{DIVIDER}\n"
            "\n"
            f"📄 <b>{title}</b>\n"
            "\n"
            f"{_where(block, branch)}\n"
            "\n"
            f"{DIVIDER}\n"
            "\n"
            f"🔢 <b>Sequential Lecture #:</b> <code>{lecture['number']}</code>\n"
            "\n"
            "⚡ <i>Summary and MCQs are being pre-generated in the background — "
            "the first tap will be near-instant.</i>"
        )
    else:
        text = (
            "ℹ️ <b>Duplicate File Detected</b>\n"
            "\n"
            f"{DIVIDER}\n"
            "\n"
            f"This PDF already exists in your archive as:\n"
            "\n"
            f"📄 <b>{title}</b>\n"
            "\n"
            f"{_where(block, branch)}"
        )

    page = (lecture["number"] - 1) // PAGE_SIZE
    keyboard = InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("📤 Open Lecture", callback_data=f"{CB_LECTURE}:{lecture['id']}")],
            [
                InlineKeyboardButton(
                    "📚 View Branch Lectures",
                    callback_data=f"{CB_BRANCH}:{block}:{branch}:{page}",
                )
            ],
            _nav_row(CB_HOME),
        ]
    )
    await render(update, context, text, keyboard)

# ==============================================================================
# 18. HANDLERS — Manage Panel
# ==============================================================================


async def show_panel(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    lecture_id: int,
    page: int,
    toast: str | None = None,
) -> None:
    query = update.callback_query
    owner_id = update.effective_user.id
    try:
        panel = await _run_cpu(db_get_panel, lecture_id, page, owner_id)
    except sqlite3.Error:
        logger.exception("DB error while opening editor for lecture %s", lecture_id)
        await safe_answer(query, "⚠️ Database is busy, please try again.", show_alert=True)
        return

    reset_nav_modes(context)
    if panel is None:
        await safe_answer(query, "⚠️ This lecture no longer exists.", show_alert=True)
        await render(update, context, MAIN_MENU_TEXT, main_menu_keyboard())
        return

    lecture, rows, total, page, pages = panel
    await safe_answer(query, toast)
    await render(
        update, context,
        panel_text(lecture, rows, total, page, pages),
        panel_keyboard(lecture, rows, page, pages),
    )


async def cb_panel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await _gate(update, context):
        return
    if not await require_admin(update):
        return
    ids = cb_ints(update.callback_query.data, 1, optional=1)
    if ids is None:
        await safe_answer(update.callback_query, "⚠️ Invalid button.", show_alert=True)
        return
    await show_panel(update, context, ids[0], ids[1] if len(ids) > 1 else 0)


async def cb_attach_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await _gate(update, context):
        return
    query = update.callback_query
    if not await require_admin(update):
        return
    ids = cb_ints(query.data, 1)
    if ids is None:
        await safe_answer(query, "⚠️ Invalid button.", show_alert=True)
        return
    lecture_id = ids[0]
    owner_id = update.effective_user.id

    try:
        lecture = await _run_cpu(db_get_lecture, lecture_id, owner_id)
    except sqlite3.Error:
        logger.exception("DB error starting attach mode for lecture %s", lecture_id)
        await safe_answer(query, "⚠️ Database is busy, please try again.", show_alert=True)
        return
    if lecture is None:
        await safe_answer(query, "⚠️ This lecture no longer exists.", show_alert=True)
        await render(update, context, MAIN_MENU_TEXT, main_menu_keyboard())
        return

    reset_nav_modes(context)
    context.user_data[ATTACH_KEY] = lecture_id
    await safe_answer(query)
    await render(
        update, context, attach_prompt_text(lecture), done_keyboard(lecture_id, with_nav=True)
    )


async def cb_attachment_ask(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await _gate(update, context):
        return
    query = update.callback_query
    if not await require_admin(update):
        return
    ids = cb_ints(query.data, 1)
    if ids is None:
        await safe_answer(query, "⚠️ Invalid button.", show_alert=True)
        return
    owner_id = update.effective_user.id

    try:
        found = await _run_cpu(db_get_attachment, ids[0], owner_id)
    except sqlite3.Error:
        logger.exception("DB error loading attachment %s", ids[0])
        await safe_answer(query, "⚠️ Database is busy, please try again.", show_alert=True)
        return
    if found is None:
        await safe_answer(query, "ℹ️ This attachment was already removed.", show_alert=True)
        return

    att, lecture = found
    await safe_answer(query)
    await render(update, context, confirm_attachment_text(att, lecture), confirm_attachment_keyboard(att))


async def cb_attachment_preview(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await _gate(update, context):
        return
    query = update.callback_query
    if not await require_admin(update):
        return
    ids = cb_ints(query.data, 1)
    if ids is None:
        await safe_answer(query, "⚠️ Invalid button.", show_alert=True)
        return
    owner_id = update.effective_user.id

    try:
        found = await _run_cpu(db_get_attachment, ids[0], owner_id)
    except sqlite3.Error:
        logger.exception("DB error loading attachment %s", ids[0])
        await safe_answer(query, "⚠️ Database is busy, please try again.", show_alert=True)
        return
    if found is None:
        await safe_answer(query, "ℹ️ This attachment was already removed.", show_alert=True)
        return

    await safe_answer(query, "📤 Sending...")
    if await _send_single(context.bot, update.effective_chat.id, found[0]):
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text="⚠️ I couldn't send this attachment. It is safe to remove it and add it again.",
        )


async def cb_attachment_delete(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await _gate(update, context):
        return
    query = update.callback_query
    if not await require_admin(update):
        return
    ids = cb_ints(query.data, 2)
    if ids is None:
        await safe_answer(query, "⚠️ Invalid button.", show_alert=True)
        return
    lecture_id, attachment_id = ids
    owner_id = update.effective_user.id

    try:
        position = await _run_cpu(
            db_delete_attachment, attachment_id, lecture_id, owner_id
        )
    except sqlite3.Error:
        logger.exception("DB error deleting attachment %s", attachment_id)
        await safe_answer(query, "⚠️ Database is busy, please try again.", show_alert=True)
        return

    if position is None:
        await show_panel(update, context, lecture_id, 0, toast="ℹ️ Already removed.")
        return
    user = update.effective_user
    logger.info(
        "Attachment %s removed from lecture %s (user %s)",
        attachment_id, lecture_id, user.id if user else "?",
    )
    await show_panel(
        update, context, lecture_id, (position - 1) // ATT_PAGE_SIZE, toast="🗑 Attachment removed."
    )


async def cb_lecture_ask(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await _gate(update, context):
        return
    query = update.callback_query
    if not await require_admin(update):
        return
    ids = cb_ints(query.data, 1)
    if ids is None:
        await safe_answer(query, "⚠️ Invalid button.", show_alert=True)
        return
    owner_id = update.effective_user.id

    try:
        lecture = await _run_cpu(db_get_lecture, ids[0], owner_id)
    except sqlite3.Error:
        logger.exception("DB error loading lecture %s", ids[0])
        await safe_answer(query, "⚠️ Database is busy, please try again.", show_alert=True)
        return
    if lecture is None:
        await safe_answer(query, "ℹ️ This lecture was already deleted.", show_alert=True)
        await render(update, context, MAIN_MENU_TEXT, main_menu_keyboard())
        return

    await safe_answer(query)
    await render(update, context, confirm_lecture_text(lecture), confirm_lecture_keyboard(lecture))


async def cb_lecture_delete(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await _gate(update, context):
        return
    query = update.callback_query
    if not await require_admin(update):
        return
    ids = cb_ints(query.data, 1)
    if ids is None:
        await safe_answer(query, "⚠️ Invalid button.", show_alert=True)
        return
    owner_id = update.effective_user.id

    try:
        deleted = await _run_cpu(db_delete_lecture, ids[0], owner_id)
    except sqlite3.Error:
        logger.exception("DB error deleting lecture %s", ids[0])
        await safe_answer(query, "⚠️ Database is busy, please try again.", show_alert=True)
        return

    if deleted is None:
        await safe_answer(query, "ℹ️ This lecture was already deleted.", show_alert=True)
        await render(update, context, MAIN_MENU_TEXT, main_menu_keyboard())
        return

    user = update.effective_user
    logger.info(
        "Lecture %s %r deleted from %s/%s with %d attachment(s) (user %s)",
        ids[0], deleted.file_name, deleted.block, deleted.subject,
        deleted.removed_attachments, user.id if user else "?",
    )
    await show_lecture_list(
        update, context, deleted.block, deleted.subject,
        (deleted.number - 1) // PAGE_SIZE,
        manage=True, toast="🗑 Lecture deleted.",
    )


async def cb_noop(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await safe_answer(update.callback_query)


async def cb_unknown(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await safe_answer(update.callback_query, "ℹ️ This button has expired. Send /start.", show_alert=True)


async def cb_upload_new(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await _gate(update, context):
        return
    await safe_answer(update.callback_query, "📂 Send any PDF to upload it")
    try:
        await update.callback_query.message.reply_text(
            "📂 <b>Upload a New PDF</b>\n"
            "\n"
            f"{DIVIDER}\n"
            "\n"
            "Just send any PDF in this chat and the full study menu will appear.\n"
            "\n"
            "👇 <i>Send your PDF now.</i>",
            parse_mode=ParseMode.HTML,
        )
    except TelegramError:
        logger.exception("Could not send upload-new prompt")

# ==============================================================================
# 19. HANDLERS — AI actions on lectures (STREAMING)
# ==============================================================================

_AI_BUSY: set[int] = set()


async def cb_ai_action(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await _gate(update, context):
        return
    query = update.callback_query
    parts = query.data.split(":")
    if len(parts) != 3 or not parts[2].isdigit():
        await safe_answer(query, "⚠️ Invalid button.", show_alert=True)
        return
    action, lecture_id = parts[1], int(parts[2])

    user = update.effective_user
    uid = user.id if user else 0
    if uid in _AI_BUSY:
        await safe_answer(query, "⏳ AI is already working on a request. Please wait.", show_alert=True)
        return

    try:
        lecture = await _run_cpu(db_get_lecture, lecture_id, uid)
    except sqlite3.Error:
        await safe_answer(query, "⚠️ Database is busy.", show_alert=True)
        return
    if lecture is None:
        await safe_answer(query, "⚠️ This lecture no longer exists.", show_alert=True)
        return

    _AI_BUSY.add(uid)
    try:
        async with _user_priority_scope():
            text = await _load_lecture_pdf_text(update, context, lecture)
            if text is None:
                return

            doc_hash = context.user_data.get(PDF_DOC_HASH_KEY) or lecture["doc_hash"]

            if action == "ask":
                context.user_data[PDF_QA_MODE_KEY] = True
                context.user_data[PDF_QA_SOURCE_KEY] = ("lecture", lecture_id)
                if doc_hash:
                    _get_or_build_index(f"doc:{doc_hash}", text)
                else:
                    _get_or_build_index(f"lec:{lecture_id}", text)
                await query.message.reply_text(
                    "💬 <b>PDF Q&A Mode Enabled</b>\n"
                    "\n"
                    f"{DIVIDER}\n"
                    "\n"
                    f"📄 <b>{escape_html(lecture['file_name'])}</b>\n"
                    "\n"
                    "Ask any question about this lecture and I'll answer using its content.\n"
                    "\n"
                    f"{DIVIDER}\n"
                    "\n"
                    "💡 <i>Send /reset when you're done.</i>",
                    parse_mode=ParseMode.HTML,
                    reply_markup=ai_actions_keyboard(lecture_id),
                )
                await safe_answer(query, "💬 Ask your question below.")
                return

            await safe_answer(query, "⏳ Running AI...")

            try:
                async with typing_indicator(context.bot, update.effective_chat.id):
                    if action == "sum":
                        await _stream_and_finalize(
                            context.bot,
                            update.effective_chat.id,
                            lambda on_chunk: ai_summary(
                                text, on_chunk=on_chunk, doc_hash=doc_hash
                            ),
                            ai_actions_keyboard(lecture_id),
                            "📝 Generating structured study summary...",
                        )
                        _schedule_eager_mcqs(text, doc_hash=doc_hash)
                    elif action == "flash":
                        await _stream_and_finalize(
                            context.bot,
                            update.effective_chat.id,
                            lambda on_chunk: ai_flashcards(
                                text, on_chunk=on_chunk, doc_hash=doc_hash
                            ),
                            ai_actions_keyboard(lecture_id),
                            "🃏 Generating flashcards...",
                        )
                    elif action == "mcq":
                        progress_msg = await query.message.reply_text(
                            "❓ Generating 5 interactive MCQs..."
                        )

                        last_p = [0.0]

                        async def on_mcq_progress(count: int) -> None:
                            now = time.monotonic()
                            if now - last_p[0] < 1.5:
                                return
                            last_p[0] = now
                            try:
                                await progress_msg.edit_text(
                                    f"❓ Parsing questions... {count}/5 ready"
                                )
                            except BadRequest:
                                pass

                        questions = await _get_mcq_batch_for_user(
                            text, doc_hash=doc_hash, on_progress=on_mcq_progress
                        )
                        try:
                            await progress_msg.delete()
                        except TelegramError:
                            pass
                        if not questions:
                            await context.bot.send_message(
                                chat_id=update.effective_chat.id,
                                text="⚠️ Could not parse the quiz. Please try again.",
                                reply_markup=ai_actions_keyboard(lecture_id),
                            )
                            return
                        await send_quiz(
                            update, context, questions, source_id=f"lec:{lecture_id}"
                        )
                        await _send_task_menu_footer(
                            context.bot,
                            update.effective_chat.id,
                            ai_actions_keyboard(lecture_id),
                            header="📋 <b>Quiz ready — 5 questions above!</b>",
                            hint="👇 <i>Pick your next study tool.</i>",
                        )
                    else:
                        await query.message.reply_text("⚠️ Unknown AI action.")
                        return
            except RuntimeError as exc:
                try:
                    await query.message.reply_text(
                        str(exc), reply_markup=ai_actions_keyboard(lecture_id)
                    )
                except TelegramError:
                    pass
    finally:
        _AI_BUSY.discard(uid)

# ==============================================================================
# 20. HANDLERS — Attachment saving
# ==============================================================================


async def save_attachment(
    update: Update, context: ContextTypes.DEFAULT_TYPE, lecture_id: int
) -> None:
    message = update.effective_message
    user = update.effective_user

    if user is None or not is_activated(user.id):
        reset_nav_modes(context)
        await message.reply_text(ACTIVATION_PROMPT, parse_mode=ParseMode.HTML)
        return

    data = extract_attachment(message)
    if data is None:
        await message.reply_text(
            "⚠️ Unsupported attachment format. Send photos, documents, media, or plain text notes."
        )
        return
    if data.kind == "text" and len(data.text or "") > MAX_NOTE_CHARS:
        await message.reply_text(
            f"⚠️ Text note exceeds max length ({len(data.text or '')}/{MAX_NOTE_CHARS})."
        )
        return

    announce = first_of_album(context, message)
    is_album = bool(message.media_group_id)

    lock = _get_attach_lock(user.id)
    try:
        async with lock:
            status, lecture = await _run_cpu(
                db_add_attachment, lecture_id, data, user.id, user.id
            )
    except sqlite3.Error:
        logger.exception("DB error saving attachment to lecture %s", lecture_id)
        await message.reply_text("⚠️ Database error - item was not saved.")
        return

    if status == ATT_MISSING:
        reset_nav_modes(context)
        await message.reply_text("⚠️ Target lecture no longer exists.")
        return

    if status == ATT_SAVED:
        logger.info("Attachment (%s) added to lecture %s (user %s)", data.kind, lecture_id, user.id)
    if not announce:
        return

    title = escape_html(lecture_title(lecture["number"], lecture["file_name"]))
    if status == ATT_SAVED:
        progress = "" if is_album else f"\n\n📎 <b>{lecture['att_count']} attachment(s)</b> total now."
        text = (
            "✅ <b>Attachment Saved</b>\n"
            "\n"
            f"{DIVIDER}\n"
            "\n"
            f"📄 <b>{title}</b>"
            f"{progress}\n"
            "\n"
            "💡 <i>Send more items or tap Done below.</i>"
        )
    else:
        text = (
            "ℹ️ <b>Already Exists</b>\n"
            "\n"
            f"{DIVIDER}\n"
            "\n"
            f"This attachment is already saved in <b>{title}</b>."
        )
    await message.reply_text(text, reply_markup=done_keyboard(lecture_id), parse_mode=ParseMode.HTML)

# ==============================================================================
# 21. HANDLERS — Messages (documents, media, text)
# ==============================================================================


async def on_document(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.effective_message
    user = update.effective_user
    doc = message.document
    if doc is None or user is None:
        return

    if not is_activated(user.id):
        await message.reply_text(ACTIVATION_PROMPT, parse_mode=ParseMode.HTML)
        return

    attach_to = context.user_data.get(ATTACH_KEY)
    if attach_to is not None:
        await save_attachment(update, context, attach_to)
        return

    file_name = doc.file_name or f"lecture_{doc.file_unique_id}.pdf"
    is_pdf = doc.mime_type == "application/pdf" or file_name.lower().endswith(".pdf")
    if not is_pdf:
        await message.reply_text(
            "⚠️ <b>Only PDF files trigger the study menu.</b>\n"
            "\n"
            "For other file types, first open a lecture and tap ➕ Add attachment.",
            parse_mode=ParseMode.HTML,
        )
        return

    context.user_data[PENDING_PDF_KEY] = {
        "file_id": doc.file_id,
        "file_unique_id": doc.file_unique_id,
        "file_name": file_name,
    }
    context.user_data.pop(PENDING_UPLOAD_MODE_KEY, None)
    context.user_data.pop(PDF_TEXT_KEY, None)
    context.user_data.pop(PDF_NAME_KEY, None)
    context.user_data.pop(PDF_DOC_HASH_KEY, None)
    context.user_data.pop(AI_LECTURE_KEY, None)
    context.user_data.pop(PDF_QA_MODE_KEY, None)
    context.user_data.pop(PDF_QA_SOURCE_KEY, None)

    _fire_task(_eager_download_and_generate(context.bot, doc.file_id))

    await message.reply_text(
        "📄 <b>PDF Detected</b>\n"
        "\n"
        f"{DIVIDER}\n"
        "\n"
        f"<b>{escape_html(file_name)}</b>\n"
        "\n"
        f"{DIVIDER}\n"
        "\n"
        "⭐ <b>Choose an action below:</b>\n"
        "\n"
        "▪️ 📝 Summary\n"
        "\n"
        "▪️ 🤖 MCQs\n"
        "\n"
        "▪️ 📑 Flashcards\n"
        "\n"
        "▪️ 💬 Ask / Infographic\n"
        "\n"
        "▪️ 📂 Upload to Archive\n"
        "\n"
        "👇 <i>Tap a button to begin.</i>",
        reply_markup=pending_pdf_keyboard(),
        parse_mode=ParseMode.HTML,
    )


async def on_attachment_media(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    if user is None or not is_activated(user.id):
        return
    attach_to = context.user_data.get(ATTACH_KEY)
    if attach_to is None:
        return
    await save_attachment(update, context, attach_to)


def _try_glossary_local(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> bool:
    """Short-circuit with a local glossary hit before any AI call.

    Returns True if the message was answered from the local glossary,
    False to let the caller proceed to AI.
    """
    message = update.effective_message
    if message is None or not message.text:
        return False
    if not glossary_db:
        return False

    user_text = message.text.strip().lower()
    if not user_text:
        return False

    matches: list[str] = []
    for key, definition in glossary_db.items():
        definition_text = "" if definition is None else str(definition)
        if user_text in key.lower() or user_text in definition_text.lower():
            matches.append(f"📚 **{key}**\n📖 {definition_text}\n")
            if len(matches) >= GLOSSARY_LOCAL_MAX_MATCHES:
                break

    if not matches:
        return False

    try:
        message.reply_text(
            "⚡ **رد فوري من القاموس المحلي (بدون توكنز):**\n\n"
            + "\n".join(matches),
            parse_mode="Markdown",
        )
    except TelegramError:
        logger.exception("Could not send glossary reply")
        return False
    return True


async def on_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    if user is None:
        return
    user_text = (update.message.text or "").strip()
    if not user_text:
        return

    if not is_activated(user.id):
        if BOT_ACTIVATION_KEY and user_text == BOT_ACTIVATION_KEY:
            db_activate_user(user.id, user.username, user.first_name)
            logger.info("User %s activated successfully (fallback)", user.id)
            await _show_welcome(update, context)
        else:
            await update.message.reply_text(ACTIVATION_PROMPT, parse_mode=ParseMode.HTML)
        return

    attach_to = context.user_data.get(ATTACH_KEY)
    if attach_to is not None:
        await save_attachment(update, context, attach_to)
        return

    # Local glossary short-circuit: answer instantly, skip the AI call entirely.
    if _try_glossary_local(update, context):
        return

    history: list[dict] = context.user_data.setdefault(HISTORY_KEY, [])
    if len(history) > MAX_HISTORY_MESSAGES:
        history[:] = history[-MAX_HISTORY_MESSAGES:]

    pdf_text = context.user_data.get(PDF_TEXT_KEY)
    qa_mode = context.user_data.get(PDF_QA_MODE_KEY, False)

    if pdf_text and qa_mode:
        source = context.user_data.get(PDF_QA_SOURCE_KEY)
        doc_hash = context.user_data.get(PDF_DOC_HASH_KEY)
        if doc_hash:
            index_key = f"doc:{doc_hash}"
        elif source and source[0] == "lecture" and source[1] is not None:
            index_key = f"lec:{source[1]}"
        elif context.user_data.get(PENDING_PDF_KEY):
            index_key = f"pend:{context.user_data[PENDING_PDF_KEY]['file_unique_id']}"
        else:
            index_key = "unknown"

        try:
            index = _get_or_build_index(index_key, pdf_text)
            relevant = index.search(user_text, k=VECTOR_TOP_K)
        except Exception:
            logger.exception("Vector retrieval failed; falling back to full context")
            relevant = []

        if relevant:
            context_text = "\n\n---\n\n".join(relevant)
            logger.info(
                "RAG retrieval: %d/%d chunks selected for query",
                len(relevant), len(index.chunks),
            )
        else:
            context_text = condense_for_context(pdf_text)

        prompt = (
            "Relevant lecture excerpts:\n\n"
            + context_text
            + "\n\n---\n\nStudent question: "
            + user_text
        )
    else:
        prompt = user_text

    try:
        async with typing_indicator(context.bot, update.effective_chat.id):
            answer = await ai_chat(history, prompt)
    except RuntimeError as exc:
        await update.message.reply_text(str(exc))
        return

    history.append({"role": "user", "content": user_text})
    history.append({"role": "assistant", "content": answer})
    if len(history) > MAX_HISTORY_MESSAGES:
        history[:] = history[-MAX_HISTORY_MESSAGES:]

    reply_markup = None
    if qa_mode:
        source = context.user_data.get(PDF_QA_SOURCE_KEY)
        if source and source[0] == "lecture" and source[1] is not None:
            reply_markup = ai_actions_keyboard(source[1])
        elif context.user_data.get(PENDING_PDF_KEY):
            reply_markup = pending_pdf_keyboard()

    await send_formatted(update.message, answer, reply_markup=reply_markup)

# ==============================================================================
# 22. GLOBAL ERROR HANDLER + LIFECYCLE
# ==============================================================================


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    err = context.error

    if isinstance(err, ApplicationHandlerStop):
        return
    if isinstance(err, asyncio.CancelledError):
        return
    if isinstance(err, (BadRequest, Forbidden)):
        logger.warning("Telegram rejected a request: %s", err)
        return
    if isinstance(err, TelegramError):
        logger.warning("Transient Telegram error: %s", err)
        return

    logger.exception("Unhandled exception while processing update", exc_info=err)

    if isinstance(update, Update) and update.effective_message:
        try:
            await update.effective_message.reply_text(
                "Something went wrong. Please send /reset and try again."
            )
        except TelegramError:
            pass


async def post_init(app: Application) -> None:
    await _run_cpu(db_init)

    try:
        await _run_cpu(db_prune_old_mcq_batches, MCQ_BATCH_PRUNE_AGE_DAYS)
    except sqlite3.Error:
        logger.debug("MCQ batch pruning skipped (DB busy)", exc_info=True)

    await app.bot.set_my_commands(
        [
            BotCommand("start", "Open the main menu"),
            BotCommand("help", "User guide"),
            BotCommand("reset", "Clear session and AI state"),
        ]
    )
    logger.info(
        "Bot ready. Model=%s max_tokens=%d(summary=%d/mcq=%d/flash=%d) "
        "activation_required=%s admin_id=%s "
        "mcq_cache_schema=v%d doc_cache=on(precompute) glossary=%d(local_short=%d) "
        "mcq_prefetch_queue=on(max=%d,threshold=%d,delay=%.1fs) "
        "priority_yield=on(poll=%.2fs) premium_summary=on "
        "httpx_pool=20/100 timeouts=%ds "
        "streaming=enabled rag=vector lru=on process_pool=%s(%d,broken=%s) "
        "empty_response_retries=%d chunk_size=%d hard_limit=%d "
        "cache_caps(activation=%d,mcq_locks=%d,attach_locks=%d) mode=%s",
        OPENROUTER_MODEL,
        MAX_OUTPUT_TOKENS,
        SUMMARY_MAX_TOKENS,
        MCQ_MAX_TOKENS,
        FLASHCARD_MAX_TOKENS,
        bool(BOT_ACTIVATION_KEY),
        ADMIN_BYPASS_ID,
        MCQ_CACHE_SCHEMA_VERSION,
        GLOSSARY_MAX_TERMS,
        GLOSSARY_LOCAL_MAX_MATCHES,
        MCQ_PREFETCH_MAX_BATCHES,
        MCQ_PREFETCH_THRESHOLD,
        MCQ_PREFETCH_INTER_BATCH_DELAY,
        _USER_PRIORITY_POLL_INTERVAL,
        REQUEST_TIMEOUT_SECONDS,
        "on" if HAS_PROCESS_POOL else "off",
        PROCESS_POOL_WORKERS,
        _PROCESS_POOL_BROKEN,
        EMPTY_RESPONSE_MAX_ATTEMPTS,
        TELEGRAM_CHUNK_CHARS,
        TELEGRAM_HARD_LIMIT,
        ACTIVATION_CACHE_MAX_ENTRIES,
        MCQ_LOCKS_MAX_ENTRIES,
        ATTACH_LOCKS_MAX_ENTRIES,
        "webhook" if (USE_WEBHOOK and WEBHOOK_URL) else "polling",
    )


async def post_shutdown(app: Application) -> None:
    try:
        await _http_client.aclose()
        logger.info("httpx connection pool closed.")
    except Exception:
        logger.debug("httpx client close failed", exc_info=True)
    try:
        _EXECUTOR.shutdown(wait=False)
        logger.info("CPU thread pool shut down.")
    except Exception:
        logger.debug("Thread executor shutdown failed", exc_info=True)
    try:
        if _PROCESS_POOL is not None:
            _PROCESS_POOL.shutdown(wait=False)
            logger.info("Process pool shut down.")
    except Exception:
        logger.debug("Process executor shutdown failed", exc_info=True)

# ==============================================================================
# 23. MAIN
# ==============================================================================


def _build_application() -> Application:
    return (
        Application.builder()
        .token(TOKEN)
        .concurrent_updates(True)
        .read_timeout(REQUEST_TIMEOUT_SECONDS)
        .write_timeout(REQUEST_TIMEOUT_SECONDS)
        .connect_timeout(REQUEST_TIMEOUT_SECONDS)
        .pool_timeout(REQUEST_TIMEOUT_SECONDS)
        .post_init(post_init)
        .post_shutdown(post_shutdown)
        .build()
    )


def _register_handlers(application: Application) -> None:
    application.add_handler(TypeHandler(Update, activation_gate), group=-1)

    application.add_handler(CommandHandler("start", cmd_start))
    application.add_handler(CommandHandler("help", cmd_help))
    application.add_handler(CommandHandler("reset", cmd_reset))

    application.add_handler(CallbackQueryHandler(cb_home, pattern=f"^{CB_HOME}$"))
    application.add_handler(CallbackQueryHandler(cb_block, pattern=f"^{CB_BLOCK}:"))
    application.add_handler(CallbackQueryHandler(cb_branch, pattern=f"^{CB_BRANCH}:"))
    application.add_handler(CallbackQueryHandler(cb_lecture, pattern=f"^{CB_LECTURE}:"))
    application.add_handler(CallbackQueryHandler(cb_manage, pattern=f"^{CB_MANAGE}:"))

    application.add_handler(CallbackQueryHandler(cb_pending_action, pattern=f"^{CB_PEND}:"))
    application.add_handler(CallbackQueryHandler(cb_quiz_answer, pattern=f"^{CB_QUIZ_ANS}:"))

    application.add_handler(CallbackQueryHandler(cb_panel, pattern=f"^{CB_PANEL}:"))
    application.add_handler(CallbackQueryHandler(cb_attach_start, pattern=f"^{CB_ATTACH}:"))
    application.add_handler(CallbackQueryHandler(cb_attachment_ask, pattern=f"^{CB_ATT_ASK}:"))
    application.add_handler(CallbackQueryHandler(cb_attachment_preview, pattern=f"^{CB_ATT_VIEW}:"))
    application.add_handler(CallbackQueryHandler(cb_attachment_delete, pattern=f"^{CB_ATT_DEL}:"))
    application.add_handler(CallbackQueryHandler(cb_lecture_ask, pattern=f"^{CB_LEC_ASK}:"))
    application.add_handler(CallbackQueryHandler(cb_lecture_delete, pattern=f"^{CB_LEC_DEL}:"))

    application.add_handler(CallbackQueryHandler(cb_ai_action, pattern=f"^{CB_AI}:"))
    application.add_handler(CallbackQueryHandler(cb_upload_new, pattern=f"^{CB_UPLOAD_NEW}$"))

    application.add_handler(CallbackQueryHandler(cb_noop, pattern=f"^{CB_NOOP}$"))
    application.add_handler(CallbackQueryHandler(cb_unknown))

    application.add_handler(MessageHandler(filters.Document.ALL, on_document))
    application.add_handler(
        MessageHandler(
            filters.PHOTO | filters.VIDEO | filters.AUDIO | filters.VOICE,
            on_attachment_media,
        )
    )
    # Single text handler. Local-glossary short-circuit runs inside on_text
    # BEFORE any AI call, so a local hit never burns tokens and never reaches
    # ai_chat. The old duplicate MessageHandler (group 0) was dead code and
    # has been removed.
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_text))
    application.add_error_handler(error_handler)


def main() -> None:
    if not TOKEN or ":" not in TOKEN:
        logger.critical(
            "TELEGRAM_BOT_TOKEN (or TOKEN) is missing or malformed.\n"
            "  • Set it with:  fly secrets set TELEGRAM_BOT_TOKEN=<your-token>\n"
            "  • Or export it locally:  export TELEGRAM_BOT_TOKEN=<your-token>"
        )
        sys.exit(1)
    if not OPENROUTER_API_KEY or not OPENROUTER_API_KEY.startswith("sk-or-"):
        logger.critical(
            "OPENROUTER_API_KEY is missing or malformed.\n"
            "  • Set it with:  fly secrets set OPENROUTER_API_KEY=<your-key>\n"
            "  • Or export it locally:  export OPENROUTER_API_KEY=<your-key>"
        )
        sys.exit(1)
    if not OPENROUTER_MODEL.endswith(":free"):
        logger.warning(
            "OPENROUTER_MODEL=%r does not end with ':free'. You may get 402.",
            OPENROUTER_MODEL,
        )
    if not BOT_ACTIVATION_KEY:
        logger.warning(
            "BOT_ACTIVATION_KEY not set — all users will have unrestricted access."
        )

    application = _build_application()
    _register_handlers(application)

    if USE_WEBHOOK and WEBHOOK_URL:
        logger.info(
            "Starting WEBHOOK mode on %s:%d/%s (public=%s/%s)",
            WEBHOOK_LISTEN, WEBHOOK_PORT, WEBHOOK_PATH,
            WEBHOOK_URL.rstrip("/"), WEBHOOK_PATH,
        )
        application.run_webhook(
            listen=WEBHOOK_LISTEN,
            port=WEBHOOK_PORT,
            url_path=WEBHOOK_PATH,
            webhook_url=f"{WEBHOOK_URL.rstrip('/')}/{WEBHOOK_PATH}",
            secret_token=WEBHOOK_SECRET or None,
            allowed_updates=Update.ALL_TYPES,
            drop_pending_updates=False,
        )
    else:
        logger.info("Polling with model=%s", OPENROUTER_MODEL)
        application.run_polling(allowed_updates=Update.ALL_TYPES, close_loop=False)


if __name__ == "__main__":
    main()