#!/usr/bin/env python3
"""
PDF Translator CLI
  translate.py --language=ta [--translate-workers=N] [--chunk-size=N] file.pdf ...

Artifacts live in .workflows/<sha256_of_resolved_path[:16]>/
All steps are resumable. Translation is parallelized across chunks and PDFs.
"""

import abc
import argparse
import concurrent.futures
import difflib
import hashlib
import json
import logging
import re
import subprocess as _subprocess
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

UTC = timezone.utc

# ---------------------------------------------------------------------------
# Dependencies
# ---------------------------------------------------------------------------
try:
    from pdftext.extraction import paginated_plain_text_output
except ImportError:
    sys.exit("pip install pdftext numpy")

try:
    from googletrans import Translator
except ImportError:
    sys.exit("pip install 'googletrans==4.0.0-rc1'")

try:
    import markdown as _md_lib
except ImportError:
    sys.exit("pip install markdown")

try:
    from rich.console import Console
    from rich.panel import Panel
    from rich.progress import (
        BarColumn, MofNCompleteColumn, Progress,
        SpinnerColumn, TextColumn, TimeRemainingColumn,
    )
    from rich.table import Table
    from rich import box
except ImportError:
    sys.exit("pip install rich")

# ---------------------------------------------------------------------------
# UI  (stderr keeps stdout clean for piping)
# ---------------------------------------------------------------------------
console = Console(stderr=True)
IS_TTY: bool = console.is_terminal


def make_progress() -> Progress:
    if not IS_TTY:
        return Progress(disable=True)
    return Progress(
        SpinnerColumn(),
        TextColumn("[bold]{task.description}"),
        BarColumn(bar_width=38),
        MofNCompleteColumn(),
        TimeRemainingColumn(),
        console=console,
        transient=False,
    )


# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------
CHUNK_SIZE        = 400
TRANSLATE_DELAY   = 0.25
MAX_RETRIES       = 4
TRANSLATE_WORKERS = 4
PDF_WORKERS       = 2
WORKFLOWS_DIR     = Path(".workflows")

# ---------------------------------------------------------------------------
# Workflow identity & layout
# ---------------------------------------------------------------------------

def workflow_id(pdf_path: Path) -> str:
    norm = str(pdf_path.resolve()).lower().replace("\\", "/")
    return hashlib.sha256(norm.encode()).hexdigest()[:16]


def workflow_dir(pdf_path: Path) -> Path:
    wdir = WORKFLOWS_DIR / workflow_id(pdf_path)
    wdir.mkdir(parents=True, exist_ok=True)
    return wdir


def artifact(wdir: Path, name: str) -> Path:
    return wdir / name


# ---------------------------------------------------------------------------
# Per-PDF logger  (logs go to <wdir>/run.log only — not the console)
# ---------------------------------------------------------------------------

def make_logger(wdir: Path, level: int) -> tuple[logging.Logger, logging.FileHandler]:
    logger = logging.getLogger(f"translate.{wdir.name}")
    logger.setLevel(level)
    logger.propagate = False
    # Close any stale handlers from a previous run in the same process
    for h in logger.handlers[:]:
        h.close()
        logger.removeHandler(h)
    fh = logging.FileHandler(wdir / "run.log", encoding="utf-8", mode="a")
    fh.setFormatter(logging.Formatter(
        "%(asctime)s %(levelname)-8s %(message)s", "%Y-%m-%d %H:%M:%S"
    ))
    logger.addHandler(fh)
    return logger, fh


# ---------------------------------------------------------------------------
# Atomic JSON I/O
# ---------------------------------------------------------------------------

def load_json(path: Path, log: logging.Logger):
    if path.exists():
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            log.debug("cache hit: %s", path.name)
            return data
        except (json.JSONDecodeError, OSError) as exc:
            log.warning("corrupt cache %s (%s) - regenerating", path.name, exc)
    return None


def save_json(path: Path, data, log: logging.Logger) -> None:
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)
    log.debug("saved %s", path.name)


def write_meta(wdir: Path, pdf_path: Path, lang: str, chunk_size: int, log: logging.Logger) -> None:
    meta = wdir / "meta.json"
    if meta.exists():
        return
    save_json(meta, {
        "pdf":         str(pdf_path.resolve()),
        "language":    lang,
        "chunk_size":  chunk_size,
        "created_at":  datetime.now(UTC).isoformat(),
        "workflow_id": wdir.name,
    }, log)


# ---------------------------------------------------------------------------
# Step 1 — Extract  (per-page so we can map translations back to source pages)
# ---------------------------------------------------------------------------

def step_extract(
    pdf_path: Path, wdir: Path,
    progress: Progress, log: logging.Logger,
) -> tuple[str, list[str]]:
    """Return (full_text, pages) where pages[i] is the text of page i+1."""
    pages_cache = artifact(wdir, "pages.json")
    text_cache  = artifact(wdir, "extracted.txt")

    if pages_cache.exists() and text_cache.exists():
        pages = load_json(pages_cache, log)
        text  = text_cache.read_text(encoding="utf-8")
        log.info("1/5 extract - cached (%d pages, %s chars)", len(pages), f"{len(text):,}")
        return text, pages

    size_mb = pdf_path.stat().st_size / 1_048_576
    log.info("1/5 extract - starting (%.1f MB)", size_mb)
    t0 = time.monotonic()

    task = progress.add_task(f"extract  {pdf_path.name[:28]}", total=None)
    pages = paginated_plain_text_output(str(pdf_path), sort=True, hyphens=True)
    progress.remove_task(task)

    text = "\n\n".join(pages)
    save_json(pages_cache, pages, log)
    text_cache.write_text(text, encoding="utf-8")
    log.info("1/5 extract - done (%d pages, %s chars, %.1fs)",
             len(pages), f"{len(text):,}", time.monotonic() - t0)
    return text, pages


# ---------------------------------------------------------------------------
# Step 2 — Clean
# ---------------------------------------------------------------------------

def step_clean(raw: str, wdir: Path, log: logging.Logger) -> str:
    cache = artifact(wdir, "cleaned.txt")
    if cache.exists():
        text = cache.read_text(encoding="utf-8")
        log.info("2/5 clean   - cached (%s chars)", f"{len(text):,}")
        return text

    text = re.sub(r"[ \t]+", " ", raw)
    text = re.sub(r"\n{3,}", "\n\n", text)
    text = re.sub(r"(?m)^\s*(Page\s+\d+\s+of\s+\d+|\d+)\s*$", "", text)
    text = re.sub(r"-\n(?=[a-z])", "", text)
    text = text.strip()

    cache.write_text(text, encoding="utf-8")
    log.info("2/5 clean   - done (%s chars, -%s removed)",
             f"{len(text):,}", f"{len(raw) - len(text):,}")
    return text


# ---------------------------------------------------------------------------
# Step 3 — Chunk
# ---------------------------------------------------------------------------

def _split_into_chunks(text: str, chunk_size: int) -> list[str]:
    chunks: list[str] = []
    current = ""
    for para in re.split(r"\n\n+", text):
        para = para.strip()
        if not para:
            continue
        if len(current) + len(para) + 2 <= chunk_size:
            current = (current + "\n\n" + para).strip() if current else para
        else:
            if current:
                chunks.append(current)
            if len(para) > chunk_size:
                buf = ""
                for sent in re.split(r"(?<=[.!?])\s+", para):
                    if len(buf) + len(sent) + 1 <= chunk_size:
                        buf = (buf + " " + sent).strip() if buf else sent
                    else:
                        if buf:
                            chunks.append(buf)
                        buf = sent
                if buf:
                    chunks.append(buf)
                current = ""
            else:
                current = para
    if current:
        chunks.append(current)
    return chunks


def step_chunk(cleaned: str, chunk_size: int, wdir: Path, log: logging.Logger) -> list[str]:
    cache = artifact(wdir, "chunks.json")
    existing = load_json(cache, log)
    if existing is not None:
        log.info("3/5 chunk   - cached (%d chunks)", len(existing))
        return existing

    chunks = _split_into_chunks(cleaned, chunk_size)
    save_json(cache, chunks, log)
    sizes = [len(c) for c in chunks]
    log.info("3/5 chunk   - %d chunks, avg %d chars, max %d chars",
             len(chunks),
             int(sum(sizes) / len(sizes)) if sizes else 0,
             max(sizes, default=0))
    return chunks


# ---------------------------------------------------------------------------
# Step 3b — Map chunks to source pages
# ---------------------------------------------------------------------------

def step_map_pages(
    chunks: list[str],
    pages: list[str],
    wdir: Path,
    log: logging.Logger,
) -> list[int]:
    """Return a 1-based source page number for each chunk (cached)."""
    cache = artifact(wdir, "chunk_pages.json")
    existing = load_json(cache, log)
    if existing is not None:
        log.info("3b/5 page map - cached (%d chunks -> %d pages)", len(existing), len(pages))
        return existing

    log.info("3b/5 page map - building word index over %d pages", len(pages))
    from collections import Counter, defaultdict

    # word -> set of 1-based page numbers
    word_to_pages: dict[str, list[int]] = defaultdict(list)
    for page_num, page_text in enumerate(pages, 1):
        for word in set(page_text.lower().split()):
            word_to_pages[word].append(page_num)

    chunk_pages: list[int] = []
    for chunk in chunks:
        counts: Counter = Counter()
        for word in chunk.lower().split():
            for pg in word_to_pages.get(word, []):
                counts[pg] += 1
        primary = counts.most_common(1)[0][0] if counts else 1
        chunk_pages.append(primary)

    save_json(cache, chunk_pages, log)
    log.info("3b/5 page map - done, pages %d-%d covered",
             min(chunk_pages), max(chunk_pages))
    return chunk_pages


# ---------------------------------------------------------------------------
# Translator backend abstraction
# ---------------------------------------------------------------------------

class TranslatorBackend(abc.ABC):
    """Translation backend protocol. Implement translate_one; translate_batch defaults to loop."""

    @abc.abstractmethod
    def translate_one(self, text: str, src: str, dest: str, log: logging.Logger) -> str: ...

    def translate_batch(
        self, texts: list[str], src: str, dest: str, log: logging.Logger,
    ) -> list[str]:
        return [self.translate_one(t, src, dest, log) for t in texts]


class GoogleTranslateBackend(TranslatorBackend):
    """Unofficial Google Translate via googletrans."""

    def translate_one(self, text: str, src: str, dest: str, log: logging.Logger) -> str:
        return _translate_one(text, src, dest, log)


class SubprocessBackend(TranslatorBackend):
    """Translate via an external process (stdin→stdout JSON protocol).

    stdin:  {"src": "en", "dest": "ta", "texts": ["...", ...]}
    stdout: ["translated...", ...]
    """

    def __init__(self, command: str) -> None:
        self.command = command

    def translate_one(self, text: str, src: str, dest: str, log: logging.Logger) -> str:
        return self.translate_batch([text], src, dest, log)[0]

    def translate_batch(
        self, texts: list[str], src: str, dest: str, log: logging.Logger,
    ) -> list[str]:
        payload = json.dumps({"src": src, "dest": dest, "texts": texts}, ensure_ascii=False)
        log.debug("subprocess backend: sending %d texts to '%s'", len(texts), self.command)
        result = _subprocess.run(
            self.command, shell=True,
            input=payload, capture_output=True, text=True, encoding="utf-8",
            timeout=300,
        )
        if result.returncode != 0:
            raise RuntimeError(
                f"backend '{self.command}' exit {result.returncode}: {result.stderr.strip()[:200]}"
            )
        output = json.loads(result.stdout)
        if not isinstance(output, list) or len(output) != len(texts):
            raise ValueError(f"backend returned {type(output).__name__} len={len(output) if isinstance(output, list) else '?'}, expected list[{len(texts)}]")
        return [str(s) for s in output]


def make_backend(spec: str) -> TranslatorBackend:
    """'googletrans' → GoogleTranslateBackend; anything else → SubprocessBackend(spec)."""
    return GoogleTranslateBackend() if spec.lower() == "googletrans" else SubprocessBackend(spec)


# ---------------------------------------------------------------------------
# Translation workers  (thread-local Translator, used by GoogleTranslateBackend)
# ---------------------------------------------------------------------------

_tls = threading.local()
_write_lock = threading.Lock()


def _get_translator() -> Translator:
    if not hasattr(_tls, "t"):
        _tls.t = Translator()
    return _tls.t


def _translate_one(text: str, src: str, dest: str, log: logging.Logger) -> str:
    tr = _get_translator()
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            result = tr.translate(text, src=src, dest=dest)
            time.sleep(TRANSLATE_DELAY)
            return result.text
        except Exception as exc:
            if attempt == MAX_RETRIES:
                raise RuntimeError(f"failed after {MAX_RETRIES} retries: {exc}") from exc
            wait = attempt * 1.5
            log.warning("translate attempt %d/%d failed (%s) - retry in %.1fs",
                        attempt, MAX_RETRIES, exc, wait)
            time.sleep(wait)
    return ""


# ---------------------------------------------------------------------------
# Step 4 — Translate
# ---------------------------------------------------------------------------

def step_translate(
    chunks: list[str], lang: str, wdir: Path, workers: int,
    progress: Progress, log: logging.Logger,
    backend: TranslatorBackend | None = None,
) -> list[str]:
    if backend is None:
        backend = GoogleTranslateBackend()
    cache = artifact(wdir, f"translated_{lang}.json")
    state: list[str | None] = load_json(cache, log) or [None] * len(chunks)
    while len(state) < len(chunks):
        state.append(None)

    todo = [(i, chunks[i], "en", lang) for i, v in enumerate(state) if v is None]
    if not todo:
        log.info("4/5 translate - cached (%d/%d)", len(chunks), len(chunks))
        return state  # type: ignore[return-value]

    log.info("4/5 translate - %d/%d remaining, %d workers, lang=%s",
             len(todo), len(chunks), workers, lang)
    t0 = time.monotonic()
    task = progress.add_task(f"translate {Path(wdir).name[:12]}…", total=len(todo))

    def _worker(args: tuple) -> tuple[int, str]:
        idx, chunk, src, dest = args
        result = backend.translate_one(chunk, src, dest, log)
        log.debug("translated chunk %d (%d->%d chars)", idx, len(chunk), len(result))
        return idx, result

    with concurrent.futures.ThreadPoolExecutor(max_workers=workers, thread_name_prefix="xl") as pool:
        futures = {pool.submit(_worker, t): t[0] for t in todo}
        for future in concurrent.futures.as_completed(futures):
            try:
                idx, translated = future.result()
            except Exception as exc:
                log.error("chunk %d failed permanently: %s", futures[future], exc)
                raise
            with _write_lock:
                state[idx] = translated
                save_json(cache, state, log)
            progress.advance(task)

    progress.remove_task(task)
    elapsed = time.monotonic() - t0
    log.info("4/5 translate - done in %.1fs (%.1f chunks/s)",
             elapsed, len(todo) / elapsed if elapsed else 0)
    return state  # type: ignore[return-value]


# ---------------------------------------------------------------------------
# Step 5 — Reverse-translate
# ---------------------------------------------------------------------------

def step_reverse(
    translated: list[str], lang: str, wdir: Path, workers: int,
    progress: Progress, log: logging.Logger,
    backend: TranslatorBackend | None = None,
) -> list[str]:
    if backend is None:
        backend = GoogleTranslateBackend()
    cache = artifact(wdir, f"reverse_{lang}.json")
    state: list[str | None] = load_json(cache, log) or [None] * len(translated)
    while len(state) < len(translated):
        state.append(None)

    todo = [(i, translated[i], lang, "en") for i, v in enumerate(state) if v is None]
    if not todo:
        log.info("5/5 reverse  - cached (%d/%d)", len(translated), len(translated))
        return state  # type: ignore[return-value]

    log.info("5/5 reverse  - %d/%d remaining, %d workers", len(todo), len(translated), workers)
    t0 = time.monotonic()
    task = progress.add_task(f"reverse   {Path(wdir).name[:12]}…", total=len(todo))

    def _worker(args: tuple) -> tuple[int, str]:
        idx, chunk, src, dest = args
        result = backend.translate_one(chunk, src, dest, log)
        log.debug("reverse chunk %d (%d->%d chars)", idx, len(chunk), len(result))
        return idx, result

    with concurrent.futures.ThreadPoolExecutor(max_workers=workers, thread_name_prefix="rv") as pool:
        futures = {pool.submit(_worker, t): t[0] for t in todo}
        for future in concurrent.futures.as_completed(futures):
            try:
                idx, back = future.result()
            except Exception as exc:
                log.error("reverse chunk %d failed permanently: %s", futures[future], exc)
                raise
            with _write_lock:
                state[idx] = back
                save_json(cache, state, log)
            progress.advance(task)

    progress.remove_task(task)
    log.info("5/5 reverse  - done in %.1fs", time.monotonic() - t0)
    return state  # type: ignore[return-value]


# ---------------------------------------------------------------------------
# Confidence
# ---------------------------------------------------------------------------

def compute_confidence(originals: list[str], backs: list[str]) -> tuple[float, list[float]]:
    scores = [
        difflib.SequenceMatcher(None, o.lower().split(), b.lower().split()).ratio()
        for o, b in zip(originals, backs)
    ]
    overall = sum(scores) / len(scores) if scores else 0.0
    return overall, scores


# ---------------------------------------------------------------------------
# Output paths
# ---------------------------------------------------------------------------

LANG_NAMES: dict[str, str] = {
    "ta": "Tamil", "hi": "Hindi", "fr": "French", "de": "German",
    "es": "Spanish", "it": "Italian", "pt": "Portuguese", "ru": "Russian",
    "ja": "Japanese", "zh-cn": "Chinese (Simplified)", "zh-tw": "Chinese (Traditional)",
    "ar": "Arabic", "ko": "Korean", "nl": "Dutch", "pl": "Polish",
    "sv": "Swedish", "tr": "Turkish", "vi": "Vietnamese", "th": "Thai",
}

# Fonts that support the script; used in pandoc YAML front matter for xelatex/lualatex.
# These are Noto family fonts — install from https://fonts.google.com/noto
PANDOC_FONTS: dict[str, str] = {
    "ta": "Noto Serif Tamil",
    "hi": "Noto Serif Devanagari",
    "ar": "Noto Naskh Arabic",
    "ja": "Noto Serif CJK JP",
    "zh-cn": "Noto Serif CJK SC",
    "zh-tw": "Noto Serif CJK TC",
    "ko": "Noto Serif CJK KR",
    "th": "Noto Serif Thai",
}


def translated_output_paths(pdf_path: Path, lang: str) -> tuple[Path, Path, Path]:
    """Return (md, html, pdf) placed next to the source file."""
    stem = pdf_path.stem
    parent = pdf_path.parent
    return (
        parent / f"{stem}-Translated.md",
        parent / f"{stem}-Translated.html",
        parent / f"{stem}-Translated.pdf",
    )


def review_output_paths(pdf_path: Path, lang: str) -> tuple[Path, Path, Path]:
    """Return (md, html, pdf) for the side-by-side review document."""
    stem = pdf_path.stem
    parent = pdf_path.parent
    return (
        parent / f"{stem}-ReviewTranslated.md",
        parent / f"{stem}-ReviewTranslated.html",
        parent / f"{stem}-ReviewTranslated.pdf",
    )


# ---------------------------------------------------------------------------
# Workflow detail artifact
# ---------------------------------------------------------------------------

def write_output(
    wdir: Path, pdf_name: str, lang: str,
    translated: list[str], overall: float, scores: list[float],
) -> Path:
    """Write the internal workflow artifact with per-chunk confidence annotations."""
    out = artifact(wdir, f"output_{lang}.txt")
    lines = [
        f"# Source:     {pdf_name}",
        f"# Language:   {lang}",
        f"# Confidence: {overall:.2%}",
        f"# Workflow:   {wdir.name}",
        f"# Generated:  {datetime.now(UTC).isoformat()}",
        "",
    ]
    for i, (chunk, score) in enumerate(zip(translated, scores), 1):
        lines.append(f"<!-- chunk {i}/{len(translated)} | confidence {score:.2%} -->")
        lines.append(chunk)
        lines.append("")
    out.write_text("\n".join(lines), encoding="utf-8")
    return out


# ---------------------------------------------------------------------------
# Page-by-page exchange artifacts  (for external tools / parallelisation)
# ---------------------------------------------------------------------------

def write_pages_input(
    chunks: list[str],
    chunk_pages: list[int],
    pdf_name: str,
    lang: str,
    wdir: Path,
    log: logging.Logger,
) -> Path:
    """Write pages_input.json: source chunks with page mapping, ready for external translators."""
    path = artifact(wdir, "pages_input.json")
    data = {
        "source": pdf_name,
        "target_language": lang,
        "chunks": [
            {"index": i, "page": chunk_pages[i] if i < len(chunk_pages) else None, "text": chunk}
            for i, chunk in enumerate(chunks)
        ],
    }
    save_json(path, data, log)
    log.info("pages_input.json: %d chunks", len(chunks))
    return path


def write_pages_output(
    chunks: list[str],
    translated: list[str],
    back: list[str],
    scores: list[float],
    chunk_pages: list[int],
    lang: str,
    translator_name: str,
    wdir: Path,
    log: logging.Logger,
) -> Path:
    """Write pages_output_<lang>.json: per-chunk results grouped with confidence scores."""
    path = artifact(wdir, f"pages_output_{lang}.json")
    data = {
        "language": lang,
        "translator": translator_name,
        "generated_at": datetime.now(UTC).isoformat(),
        "chunks": [
            {
                "index": i,
                "page": chunk_pages[i] if i < len(chunk_pages) else None,
                "original": chunks[i],
                "translated": translated[i],
                "back_translated": back[i],
                "confidence": round(scores[i], 4),
            }
            for i in range(len(chunks))
        ],
    }
    save_json(path, data, log)
    log.info("pages_output_%s.json: %d chunks", lang, len(chunks))
    return path


# ---------------------------------------------------------------------------
# Pandoc-compatible Markdown output
# ---------------------------------------------------------------------------

def _yaml_str(value: str) -> str:
    """Quote a YAML string value if it contains special characters."""
    if any(c in value for c in ':#{}[]|>&*!,?'):
        return f'"{value}"'
    return value


def write_markdown(
    pdf_path: Path,
    lang: str,
    translated: list[str],
    overall: float,
    scores: list[float],
    chunk_pages: list[int],
    total_pages: int,
    translator: str,
    log: logging.Logger,
) -> Path:
    md_path, _, _ = translated_output_paths(pdf_path, lang)
    lang_name = LANG_NAMES.get(lang, lang.upper())
    font = PANDOC_FONTS.get(lang)
    title = pdf_path.stem.replace("-", " ").replace("_", " ")
    low_idx = {i for i, s in enumerate(scores) if s < 0.4}

    # YAML front matter
    fm: list[str] = [
        "---",
        f"title: {_yaml_str(title)}",
        f"subtitle: {_yaml_str(f'English to {lang_name} Translation')}",
        f"lang: {lang}",
        f"date: {datetime.now(UTC).strftime('%Y-%m-%d')}",
        f"source-language: English",
        f"target-language: {_yaml_str(lang_name)}",
        f"translator: {_yaml_str(translator)}",
        f"translation-confidence: {overall:.1%}",
        f"source-pages: {total_pages}",
        f"chunks: {len(translated)}",
        f"low-confidence-chunks: {len(low_idx)}",
        "geometry: margin=1in",
        "colorlinks: true",
        "linkcolor: blue",
    ]
    if font:
        fm.append(f"mainfont: {_yaml_str(font)}")
    fm.append("---")

    header: list[str] = ["", "---", ""]

    # Body — grouped by source page with page-break markers for back-reference
    body: list[str] = []
    current_page: int | None = None
    for i, (chunk, score) in enumerate(zip(translated, scores)):
        pg = chunk_pages[i] if i < len(chunk_pages) else None
        if pg != current_page:
            if body:          # not the very first page marker
                body.append("")
                body.append("---")
                body.append("")
            body.append(f"**Page {pg} / {total_pages}**")
            body.append("")
            current_page = pg
        if i in low_idx:
            body.append(f"*\\[Low confidence: {score:.0%}\\]*")
            body.append("")
        body.append(chunk)
        body.append("")

    md_path.write_text(
        "\n".join(fm + header + body),
        encoding="utf-8",
    )
    log.info("markdown written: %s", md_path)
    return md_path


# ---------------------------------------------------------------------------
# Review markdown  (side-by-side source / translation table)
# ---------------------------------------------------------------------------

def write_review_markdown(
    pdf_path: Path,
    lang: str,
    chunks: list[str],
    translated: list[str],
    overall: float,
    scores: list[float],
    chunk_pages: list[int],
    total_pages: int,
    translator: str,
    log: logging.Logger,
) -> Path:
    md_path, _, _ = review_output_paths(pdf_path, lang)
    lang_name = LANG_NAMES.get(lang, lang.upper())
    font = PANDOC_FONTS.get(lang)
    title = pdf_path.stem.replace("-", " ").replace("_", " ")
    low_idx = {i for i, s in enumerate(scores) if s < 0.4}

    fm: list[str] = [
        "---",
        f"title: {_yaml_str(title)}",
        f"subtitle: {_yaml_str(f'Review: English to {lang_name} Translation')}",
        f"document-type: Review",
        f"lang: {lang}",
        f"date: {datetime.now(UTC).strftime('%Y-%m-%d')}",
        f"source-language: English",
        f"target-language: {_yaml_str(lang_name)}",
        f"translator: {_yaml_str(translator)}",
        f"translation-confidence: {overall:.1%}",
        f"source-pages: {total_pages}",
        f"chunks: {len(translated)}",
        f"low-confidence-chunks: {len(low_idx)}",
        "geometry: margin=1in",
        "colorlinks: true",
        "linkcolor: blue",
    ]
    if font:
        fm.append(f"mainfont: {_yaml_str(font)}")
    fm.append("---")

    body: list[str] = ["", "---", ""]
    current_page: int | None = None

    for i, (src, tgt, score) in enumerate(zip(chunks, translated, scores)):
        pg = chunk_pages[i] if i < len(chunk_pages) else None
        if pg != current_page:
            if current_page is not None:
                body.append("")
            body.append(f"**Page {pg} / {total_pages}**")
            body.append("")
            body.append("| Source (English) | Translation |")
            body.append("|---|---|")
            current_page = pg

        src_cell = src.replace("\n", " ").replace("|", "\\|")
        tgt_cell = tgt.replace("\n", " ").replace("|", "\\|")
        flag = " ⚠" if i in low_idx else ""
        body.append(f"| {src_cell} | {tgt_cell}{flag} |")

    md_path.write_text("\n".join(fm + body), encoding="utf-8")
    log.info("review markdown written: %s", md_path)
    return md_path


# ---------------------------------------------------------------------------
# HTML rendering  (markdown lib, same approach as ../Resume/Make.py)
# ---------------------------------------------------------------------------

_CSS_PATH = Path(__file__).parent / "translate.css"


def _parse_front_matter(text: str) -> tuple[dict[str, str], str]:
    """Return (fields_dict, body_text) from a YAML-fenced markdown document."""
    if not text.startswith("---\n"):
        return {}, text
    end = text.find("\n---\n", 4)
    if end == -1:
        return {}, text
    fields: dict[str, str] = {}
    for line in text[4:end].splitlines():
        if ": " in line:
            key, _, val = line.partition(": ")
            fields[key.strip()] = val.strip().strip('"')
    return fields, text[end + 5:]


def _render_doc_header(fm: dict[str, str]) -> str:
    title      = fm.get("title", "")
    date       = fm.get("date", "")
    src_lang   = fm.get("source-language", "English")
    tgt_lang   = fm.get("target-language", fm.get("lang", ""))
    translator = fm.get("translator", "")
    confidence = fm.get("translation-confidence", "")
    pages      = fm.get("source-pages", "")
    chunks     = fm.get("chunks", "")
    low        = fm.get("low-confidence-chunks", "")
    doc_type   = fm.get("document-type", "")

    base_subtitle = f"{src_lang} \u2192 {tgt_lang}"
    subtitle = f"Review \u2014 {base_subtitle}" if doc_type == "Review" else base_subtitle
    meta_parts = [p for p in [date, f"Translator: {translator}", f"Confidence: {confidence}"] if p]
    meta = " \u00b7 ".join(meta_parts)

    stats_parts = []
    if pages:
        stats_parts.append(f"Pages: {pages}")
    if chunks:
        stats_parts.append(f"Chunks: {chunks}")
    if low:
        stats_parts.append(f"Low-confidence: {low}")
    stats = " \u00b7 ".join(stats_parts)

    stats_line = f'  <p class="doc-meta">{stats}</p>\n' if stats else ""
    return (
        f'<header class="doc-header">\n'
        f'  <h1 class="doc-title">{title}</h1>\n'
        f'  <p class="doc-subtitle">{subtitle}</p>\n'
        f'  <p class="doc-meta">{meta}</p>\n'
        f'{stats_line}'
        f'</header>\n'
    )


def write_html(
    md_path: Path,
    html_path: Path,
    lang: str,
    log: logging.Logger,
) -> Path:
    fm, body_md = _parse_front_matter(md_path.read_text(encoding="utf-8"))
    css = _CSS_PATH.read_text(encoding="utf-8") if _CSS_PATH.exists() else ""

    header_html = _render_doc_header(fm)
    body_html   = _md_lib.markdown(body_md, extensions=["extra"])

    html = (
        f'<!DOCTYPE html>\n'
        f'<html lang="{lang}">\n'
        f'<head>\n'
        f'<meta charset="utf-8">\n'
        f'<style>{css}</style>\n'
        f'</head>\n'
        f'<body>\n{header_html}{body_html}\n</body>\n</html>\n'
    )
    html_path.write_text(html, encoding="utf-8")
    log.info("HTML written: %s", html_path)
    return html_path


# ---------------------------------------------------------------------------
# PDF rendering  (weasyprint binary, same approach as ../Resume/Make.py)
# ---------------------------------------------------------------------------

def render_pdf(
    html_path: Path,
    out_pdf: Path,
    log: logging.Logger,
) -> bool:
    import shutil
    import subprocess

    binary = "weasyprint"
    if not shutil.which(binary):
        log.warning("weasyprint not found - PDF skipped")
        log.warning("Install: pip install weasyprint  (or see https://weasyprint.org)")
        return False

    log.info("weasyprint: %s -> %s", html_path.name, out_pdf.name)
    try:
        result = subprocess.run(
            [binary, "-e", "utf-8", str(html_path), str(out_pdf)],
            capture_output=True, text=True, timeout=180,
        )
        if result.returncode == 0:
            log.info("PDF written: %s", out_pdf)
            return True
        log.warning("weasyprint failed (exit %d): %s",
                    result.returncode, result.stderr.strip()[:400])
    except FileNotFoundError:
        log.warning("weasyprint not found - PDF skipped")
    except subprocess.TimeoutExpired:
        log.warning("weasyprint timed out")
    except Exception as exc:
        log.warning("weasyprint error: %s", exc)

    return False


# ---------------------------------------------------------------------------
# Rich UI helpers
# ---------------------------------------------------------------------------

def _confidence_bar(score: float, width: int = 20) -> str:
    filled = round(score * width)
    color = "green" if score >= 0.7 else "yellow" if score >= 0.4 else "red"
    bar = "█" * filled + "░" * (width - filled)
    label = "good" if score >= 0.7 else "medium" if score >= 0.4 else "low"
    return f"[{color}]{bar}[/] {score:.1%}  [{color}]{label}[/]"


def print_run_header(entries: list[dict]) -> None:
    """Print a pre-run summary table so the user can tail -f the log immediately."""
    if not IS_TTY:
        for e in entries:
            print(f"log: {e['log']}", file=sys.stderr)
        return

    table = Table(box=box.SIMPLE, show_header=False, padding=(0, 1))
    table.add_column(style="dim")
    table.add_column()
    for e in entries:
        table.add_row("PDF",      f"[bold]{e['pdf']}[/]  [dim]({e['size_mb']:.1f} MB)[/]")
        table.add_row("Language", e["lang"])
        table.add_row("Workflow", f"[dim]{e['wdir']}[/]")
        table.add_row("Log",      f"[cyan]{e['log']}[/]  [dim italic]tail -f {e['log']}[/]")
        if len(entries) > 1:
            table.add_section()

    console.print(Panel(table, title="[bold]PDF Translator[/]", border_style="blue"))


def print_result(
    pdf_name: str, lang: str, overall: float, scores: list[float],
    elapsed: float, wdir: Path, log_file: Path,
    md_path: Path, html_path: Path, out_pdf: Path, pdf_ok: bool,
    rev_md: Path | None = None, rev_html: Path | None = None,
    rev_pdf: Path | None = None, rev_pdf_ok: bool = False,
) -> None:
    low = [(i + 1, s) for i, s in enumerate(scores) if s < 0.4]

    if not IS_TTY:
        print(str(md_path))
        print(str(html_path))
        if pdf_ok:
            print(str(out_pdf))
        if rev_md:
            print(str(rev_md))
            print(str(rev_html))
            if rev_pdf_ok:
                print(str(rev_pdf))
        return

    table = Table(box=box.SIMPLE, show_header=False, padding=(0, 1))
    table.add_column(style="dim", width=14)
    table.add_column()
    table.add_row("PDF",        f"[bold]{pdf_name}[/]")
    table.add_row("Language",   f"{LANG_NAMES.get(lang, lang)} ({lang})")
    table.add_row("Chunks",     f"{len(scores):,}")
    table.add_row("Confidence", _confidence_bar(overall))
    table.add_row("Time",       f"{elapsed:.1f}s")
    table.add_section()
    table.add_row("Markdown",   f"[green]{md_path}[/]")
    table.add_row("HTML",       f"[green]{html_path}[/]")
    if pdf_ok:
        table.add_row("PDF",    f"[green]{out_pdf}[/]")
    else:
        table.add_row("PDF",    "[yellow]skipped — weasyprint not found[/]")
    if rev_md:
        table.add_section()
        table.add_row("Review MD",   f"[cyan]{rev_md}[/]")
        table.add_row("Review HTML", f"[cyan]{rev_html}[/]")
        if rev_pdf_ok:
            table.add_row("Review PDF", f"[cyan]{rev_pdf}[/]")
        else:
            table.add_row("Review PDF", "[yellow]skipped — weasyprint not found[/]")
    table.add_section()
    table.add_row("Workflow",   f"[dim]{wdir}[/]")
    table.add_row("Log",        f"[dim]{log_file}[/]")

    if low:
        table.add_section()
        sample = "  ".join(f"#{c} {s:.0%}" for c, s in low[:8])
        suffix = f"  +{len(low) - 8} more" if len(low) > 8 else ""
        table.add_row(
            "[yellow]warn[/]",
            f"[yellow]{len(low)} low-confidence chunks[/]\n[dim]{sample}{suffix}[/]",
        )

    border = "green" if overall >= 0.7 else "yellow" if overall >= 0.4 else "red"
    console.print(Panel(table, title="[bold]Translation complete[/]", border_style=border))


# ---------------------------------------------------------------------------
# Single-PDF pipeline
# ---------------------------------------------------------------------------

def process_pdf(
    pdf_path: Path,
    lang: str,
    chunk_size: int,
    workers: int,
    translator: str,
    progress: Progress,
    log_level: int,
    backend: TranslatorBackend | None = None,
    review: bool = False,
) -> Path:
    if backend is None:
        backend = GoogleTranslateBackend()
    name = pdf_path.name
    wdir = workflow_dir(pdf_path)
    log, fh = make_logger(wdir, log_level)
    log_file = wdir / "run.log"

    write_meta(wdir, pdf_path, lang, chunk_size, log)
    log.info("=== run start: %s lang=%s workers=%d backend=%s ===",
             name, lang, workers, type(backend).__name__)

    t0 = time.monotonic()
    raw, pages = step_extract(pdf_path, wdir, progress, log)
    cleaned    = step_clean(raw, wdir, log)
    chunks     = step_chunk(cleaned, chunk_size, wdir, log)
    chunk_pages = step_map_pages(chunks, pages, wdir, log)
    write_pages_input(chunks, chunk_pages, name, lang, wdir, log)
    translated = step_translate(chunks, lang, wdir, workers, progress, log, backend)
    back       = step_reverse(translated, lang, wdir, workers, progress, log, backend)

    overall, scores = compute_confidence(chunks, back)
    write_output(wdir, name, lang, translated, overall, scores)
    write_pages_output(chunks, translated, back, scores, chunk_pages, lang, translator, wdir, log)
    md_path, html_path, out_pdf = translated_output_paths(pdf_path, lang)
    write_markdown(pdf_path, lang, translated, overall, scores, chunk_pages, len(pages), translator, log)
    write_html(md_path, html_path, lang, log)
    pdf_ok = render_pdf(html_path, out_pdf, log)

    rev_md_path = rev_html_path = rev_pdf_path = None
    rev_pdf_ok = False
    if review:
        rev_md_path, rev_html_path, rev_pdf_path = review_output_paths(pdf_path, lang)
        write_review_markdown(pdf_path, lang, chunks, translated, overall, scores,
                              chunk_pages, len(pages), translator, log)
        write_html(rev_md_path, rev_html_path, lang, log)
        rev_pdf_ok = render_pdf(rev_html_path, rev_pdf_path, log)

    elapsed = time.monotonic() - t0

    low = [(i + 1, s) for i, s in enumerate(scores) if s < 0.4]
    if low:
        log.warning("%d low-confidence chunks (<40%%): %s",
                    len(low), ", ".join(f"#{c}={s:.0%}" for c, s in low[:10]))
    log.info("done - confidence=%.1f%% chunks=%d elapsed=%.1fs md=%s pdf_ok=%s review=%s",
             overall * 100, len(chunks), elapsed, md_path, pdf_ok, review)

    fh.close()
    log.removeHandler(fh)

    print_result(name, lang, overall, scores, elapsed, wdir, log_file,
                 md_path, html_path, out_pdf, pdf_ok,
                 rev_md_path, rev_html_path, rev_pdf_path, rev_pdf_ok)
    return md_path


# ---------------------------------------------------------------------------
# Multi-PDF driver
# ---------------------------------------------------------------------------

def run(
    pdfs: list[Path],
    lang: str,
    chunk_size: int,
    translate_workers: int,
    pdf_workers: int,
    translator: str,
    log_level: int,
    backend: TranslatorBackend | None = None,
    review: bool = False,
) -> None:
    if backend is None:
        backend = GoogleTranslateBackend()
    WORKFLOWS_DIR.mkdir(exist_ok=True)

    entries = []
    for p in pdfs:
        wdir = workflow_dir(p)
        entries.append({
            "pdf":     p.name,
            "size_mb": p.stat().st_size / 1_048_576,
            "lang":    lang,
            "wdir":    str(wdir),
            "log":     str(wdir / "run.log"),
        })

    print_run_header(entries)

    with make_progress() as progress:
        if len(pdfs) == 1 or pdf_workers == 1:
            for p in pdfs:
                process_pdf(p, lang, chunk_size, translate_workers, translator, progress, log_level, backend, review)
        else:
            def _task(p: Path) -> Path:
                return process_pdf(p, lang, chunk_size, translate_workers, translator, progress, log_level, backend, review)

            with concurrent.futures.ThreadPoolExecutor(
                max_workers=pdf_workers, thread_name_prefix="pdf"
            ) as pool:
                futures = {pool.submit(_task, p): p for p in pdfs}
                for f in concurrent.futures.as_completed(futures):
                    try:
                        f.result()
                    except Exception as exc:
                        console.print(f"[red]Error:[/] {futures[f].name}: {exc}")
                        raise


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    global WORKFLOWS_DIR, IS_TTY

    parser = argparse.ArgumentParser(
        description="Translate PDFs to a target language with confidence scoring.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("pdfs", nargs="+", type=Path, metavar="PDF")
    parser.add_argument("--language",          required=True,
                        help="Target language code (e.g. ta, hi, fr)")
    parser.add_argument("--chunk-size",        type=int, default=CHUNK_SIZE,
                        help="Characters per translation chunk")
    parser.add_argument("--translate-workers", type=int, default=TRANSLATE_WORKERS,
                        help="Parallel translation threads per PDF")
    parser.add_argument("--pdf-workers",       type=int, default=PDF_WORKERS,
                        help="Parallel PDFs processed at once")
    parser.add_argument("--workflows-dir",     type=Path, default=str(WORKFLOWS_DIR),
                        help="Root dir for workflow artifacts")
    parser.add_argument("--log-level",         default="INFO",
                        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
                        help="Verbosity of the per-workflow log file")
    parser.add_argument("--translator",          default="Google Translate (googletrans)",
                        help="Translator label written into the document metadata")
    parser.add_argument("--translator-backend", default="googletrans",
                        help="'googletrans' (default) or a shell command that reads JSON from stdin and writes JSON to stdout")
    parser.add_argument("--review",            action="store_true",
                        help="Also generate a side-by-side source/translation review document (*-ReviewTranslated.*)")
    parser.add_argument("--no-color",          action="store_true",
                        help="Force non-interactive plain output")

    args = parser.parse_args()
    WORKFLOWS_DIR = Path(args.workflows_dir)

    if args.no_color:
        IS_TTY = False

    missing = [p for p in args.pdfs if not p.exists()]
    if missing:
        sys.exit("Not found: " + ", ".join(str(p) for p in missing))
    non_pdf = [p for p in args.pdfs if p.suffix.lower() != ".pdf"]
    if non_pdf:
        sys.exit("Not PDF files: " + ", ".join(str(p) for p in non_pdf))

    run(
        pdfs=args.pdfs,
        lang=args.language,
        chunk_size=args.chunk_size,
        translate_workers=args.translate_workers,
        pdf_workers=args.pdf_workers,
        translator=args.translator,
        log_level=getattr(logging, args.log_level),
        backend=make_backend(args.translator_backend),
        review=args.review,
    )


if __name__ == "__main__":
    main()
