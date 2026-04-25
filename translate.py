#!/usr/bin/env python3
"""
PDF Translator CLI

  translate.py --language=ta File.pdf

  translate.py --language=zh --review File.pdf

  translate.py --language=ta --language=hi File1.pdf File2.pdf

  translate.py --help
"""

import abc
import argparse
import asyncio
import difflib
import hashlib
import json
import logging
import os
import re
import subprocess
import sys
import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Dict, List, Optional, Tuple

UTC = timezone.utc

# Thread-local: each worker thread gets its own GoogleTranslatorLib instance.
_thread_local = threading.local()

# Dependencies
try:
    from pdftext.extraction import paginated_plain_text_output
except ImportError:
    sys.exit("pip install pdftext numpy")

try:
    from googletrans import Translator as GoogleTranslatorLib
    import googletrans
except ImportError:
    sys.exit("pip install 'googletrans==4.0.2'")

try:
    import markdown as mdLib
except ImportError:
    sys.exit("pip install markdown")

try:
    from rich.console import Console
    from rich.panel import Panel
    from rich.progress import (
        BarColumn, MofNCompleteColumn, Progress,
        SpinnerColumn, TextColumn, TimeRemainingColumn,
    )
    from rich.table import Column, Table
    from rich import box
except ImportError:
    sys.exit("pip install rich")

console = Console(stderr=True)
IS_TTY: bool = console.is_terminal

_DESC_WIDTH = 46
_MARKUP_RE = re.compile(r'\[/?[^\]]*\]')

OnChunkDone = Callable[[int, str], None]


def _descPad(text: str, width: int = _DESC_WIDTH) -> str:
    visible = len(_MARKUP_RE.sub('', text))
    return text + ' ' * max(0, width - visible)


def makeProgress() -> Progress:
    if not IS_TTY:
        return Progress(disable=True)
    return Progress(
        SpinnerColumn(),
        TextColumn("{task.description}", table_column=Column(min_width=_DESC_WIDTH, no_wrap=True)),
        BarColumn(bar_width=28),
        MofNCompleteColumn(),
        TimeRemainingColumn(),
        console=console,
        transient=True,          # Clean final output
    )


# ---------------------------------------------------------------------------
# Partial-state helpers
# ---------------------------------------------------------------------------

def _atomicSave(path: Path, data: object) -> None:
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


def _loadPartial(path: Path, expectedLen: int) -> Dict[int, str]:
    if not path.exists():
        return {}
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(raw, list):
            return {i: t for i, t in enumerate(raw) if t is not None}
        if isinstance(raw, dict):
            return {int(k): v for k, v in raw.items() if v is not None}
    except Exception:
        pass
    return {}


# ---------------------------------------------------------------------------
# Sentence splitting
# ---------------------------------------------------------------------------

_ABBREVS = re.compile(
    r'\b(Mr|Mrs|Ms|Dr|Prof|Sr|Jr|St|vs|etc|approx|Dept|Est|Govt|Sec|Fig|No|Vol|pp|al|cf|ca'
    r'|Rev|Gen|Lt|Sgt|Rep|Sen|Gov|Jan|Feb|Mar|Apr|Jun|Jul|Aug|Sep|Oct|Nov|Dec'
    r'|U\.S|C\.F\.R|e\.g|i\.e)\.',
    re.IGNORECASE,
)
_SENTINEL = '\x00'


def _splitSentences(para: str) -> List[str]:
    para = re.sub(r'\n', ' ', para)
    para = re.sub(r' {2,}', ' ', para).strip()
    if not para:
        return []
    text = _ABBREVS.sub(lambda m: m.group(0).replace('.', _SENTINEL), para)
    text = re.sub(r'(\d)\.(\d)', lambda m: m.group(0).replace('.', _SENTINEL), text)
    text = re.sub(
        r'\b([A-Z])\.\s*(?=[A-Z]\.)',
        lambda m: m.group(0).replace('.', _SENTINEL),
        text,
    )
    parts = re.split(r'(?<=[.!?])\s+(?=[A-Z“‘])', text)
    return [p.replace(_SENTINEL, '.').strip() for p in parts if p.strip()]


# ---------------------------------------------------------------------------
# Translators
# ---------------------------------------------------------------------------

class Translator(abc.ABC):
    @abc.abstractmethod
    def translateBatch(self, texts: List[str], src: str, dest: str, onChunkDone: Optional[OnChunkDone] = None) -> List[str]: ...
    def translateOne(self, text: str, src: str, dest: str) -> str:
        return self.translateBatch([text], src, dest)[0]
    @abc.abstractmethod
    def getSupportedLanguages(self) -> Dict[str, str]: ...


class GoogleTranslator(Translator):
    def __init__(self, workers: int = 1):
        self._workers = max(1, workers)

    def translateBatch(self, texts: List[str], src: str, dest: str, onChunkDone: Optional[OnChunkDone] = None) -> List[str]:
        results: List[Optional[str]] = [None] * len(texts)

        def _worker(idx: int, text: str) -> None:
            for attempt in range(1, 5):
                try:
                    async def _do() -> str:
                        async with GoogleTranslatorLib() as tr:
                            result = await tr.translate(text, src=src, dest=dest)
                            return result.text
                    results[idx] = asyncio.run(_do())
                    time.sleep(0.25)
                    break
                except Exception:
                    if attempt == 4:
                        results[idx] = text
                        break
                    time.sleep(attempt * 1.5)
            if onChunkDone:
                onChunkDone(idx, results[idx] or text)

        with ThreadPoolExecutor(max_workers=self._workers) as executor:
            futures = [executor.submit(_worker, i, t) for i, t in enumerate(texts)]
            try:
                for future in as_completed(futures):
                    future.result()
            except KeyboardInterrupt:
                for f in futures:
                    f.cancel()
                raise

        return [r or "" for r in results]

    def getSupportedLanguages(self) -> Dict[str, str]:
        return googletrans.LANGUAGES.copy()


class SubprocessTranslator(Translator):
    def __init__(self, command: str):
        self.command = command

    def translateBatch(self, texts: List[str], src: str, dest: str, onChunkDone: Optional[OnChunkDone] = None) -> List[str]:
        payload = json.dumps({"src": src, "dest": dest, "texts": texts}, ensure_ascii=False)
        result = subprocess.run(self.command, shell=True, input=payload, capture_output=True, text=True, timeout=300)
        if result.returncode != 0:
            raise RuntimeError(f"Translator failed: {result.stderr[:300]}")
        output = json.loads(result.stdout)
        if not isinstance(output, list) or len(output) != len(texts):
            raise ValueError("Invalid translator output")
        translated = [str(x) for x in output]
        if onChunkDone:
            for i, t in enumerate(translated):
                onChunkDone(i, t)
        return translated

    def getSupportedLanguages(self) -> Dict[str, str]:
        return googletrans.LANGUAGES.copy()


class LLMTranslator(Translator):
    _SYSTEM = ("You are a professional translator. "
               "Translate the following text into {lang_name}. "
               "Output only the translation — no explanations, no metadata, no quotes.")

    # Curated subset of languages that modern instruction-tuned LLMs reliably handle.
    # Excludes very low-resource languages where output quality is unpredictable.
    _LANGUAGES: Dict[str, str] = {
        "af": "afrikaans",
        "ar": "arabic",
        "az": "azerbaijani",
        "be": "belarusian",
        "bg": "bulgarian",
        "bn": "bengali",
        "bs": "bosnian",
        "ca": "catalan",
        "cs": "czech",
        "cy": "welsh",
        "da": "danish",
        "de": "german",
        "el": "greek",
        "en": "english",
        "es": "spanish",
        "et": "estonian",
        "fa": "persian",
        "fi": "finnish",
        "fr": "french",
        "gl": "galician",
        "gu": "gujarati",
        "he": "hebrew",
        "hi": "hindi",
        "hr": "croatian",
        "hu": "hungarian",
        "hy": "armenian",
        "id": "indonesian",
        "is": "icelandic",
        "it": "italian",
        "ja": "japanese",
        "ka": "georgian",
        "kk": "kazakh",
        "km": "khmer",
        "kn": "kannada",
        "ko": "korean",
        "la": "latin",
        "lt": "lithuanian",
        "lv": "latvian",
        "mk": "macedonian",
        "ml": "malayalam",
        "mn": "mongolian",
        "mr": "marathi",
        "ms": "malay",
        "my": "myanmar (burmese)",
        "ne": "nepali",
        "nl": "dutch",
        "no": "norwegian",
        "pa": "punjabi",
        "pl": "polish",
        "ps": "pashto",
        "pt": "portuguese",
        "ro": "romanian",
        "ru": "russian",
        "si": "sinhala",
        "sk": "slovak",
        "sl": "slovenian",
        "sq": "albanian",
        "sr": "serbian",
        "sv": "swedish",
        "sw": "swahili",
        "ta": "tamil",
        "te": "telugu",
        "th": "thai",
        "tl": "filipino",
        "tr": "turkish",
        "uk": "ukrainian",
        "ur": "urdu",
        "uz": "uzbek",
        "vi": "vietnamese",
        "zh-cn": "chinese (simplified)",
        "zh-tw": "chinese (traditional)",
    }

    def __init__(self, workers: int = 1):
        try:
            from openai import OpenAI as _OpenAI
        except ImportError:
            sys.exit("pip install openai")
        model = os.environ.get("OPENAI_MODEL", "").strip()
        if not model:
            sys.exit("OPENAI_MODEL environment variable is required")
        self._model = model
        self._workers = max(1, workers)
        self._client = _OpenAI()

    def _translateOne(self, text: str, src: str, dest: str) -> str:
        lang_name = self._LANGUAGES.get(dest, dest)
        resp = self._client.chat.completions.create(
            model=self._model,
            messages=[{"role": "system", "content": self._SYSTEM.format(lang_name=lang_name)},
                      {"role": "user", "content": text}],
            temperature=0.1,
        )
        return (resp.choices[0].message.content or "").strip()

    def translateBatch(self, texts: List[str], src: str, dest: str, onChunkDone: Optional[OnChunkDone] = None) -> List[str]:
        results: List[Optional[str]] = [None] * len(texts)
        def _worker(idx: int, text: str):
            for attempt in range(1, 4):
                try:
                    results[idx] = self._translateOne(text, src, dest)
                    break
                except Exception:
                    if attempt == 3:
                        results[idx] = text
                        break
                    time.sleep(attempt * 2)
            if onChunkDone:
                onChunkDone(idx, results[idx] or text)
        with ThreadPoolExecutor(max_workers=self._workers) as executor:
            futures = [executor.submit(_worker, i, t) for i, t in enumerate(texts)]
            for future in as_completed(futures):
                future.result()
        return [r or "" for r in results]

    def getSupportedLanguages(self) -> Dict[str, str]:
        return self._LANGUAGES.copy()


# ---------------------------------------------------------------------------
# Reviewer + Renderer (unchanged from working version)
# ---------------------------------------------------------------------------

class DefaultReviewer:
    def __init__(self, reverseTranslator: Translator):
        self.reverseTranslator = reverseTranslator

    def reviewAndScore(self, chunks: List[str], translated: List[str], lang: str, wdir: Path, progress: Optional[Progress] = None):
        cache = wdir / f"reverse_{lang}.json"
        partialRev = _loadPartial(cache, len(translated))
        remaining = [(i, translated[i]) for i in range(len(translated)) if i not in partialRev]

        if remaining:
            revTask = _subTask(progress, f"back-translating {lang} → en", total=len(translated))
            if revTask and len(partialRev):
                progress.update(revTask, completed=len(partialRev))

            lock = threading.Lock()
            remIndices = [i for i, _ in remaining]

            def _onDone(localIdx: int, text: str):
                origIdx = remIndices[localIdx]
                with lock:
                    partialRev[origIdx] = text
                    _atomicSave(cache, partialRev)
                if revTask:
                    progress.update(revTask, advance=1)

            self.reverseTranslator.translateBatch([t for _, t in remaining], lang, "en", _onDone)
            if revTask:
                progress.remove_task(revTask)

        back = [partialRev[i] for i in range(len(translated))]
        scores = [difflib.SequenceMatcher(None, o.lower().split(), b.lower().split()).ratio()
                  for o, b in zip(chunks, back)]
        return sum(scores)/len(scores) if scores else 0.0, scores

    def generateReviewDocument(self, pdfPath: Path, lang: str, chunks: List[str], translated: List[str],
                               scores: List[float], chunkPages: List[int], totalPages: int) -> Path:
        stem = pdfPath.stem
        mdPath = pdfPath.parent / f"{stem}-{lang}-ReviewTranslated.md"
        if mdPath.exists():
            return mdPath

        langName = LANG_NAMES.get(lang, lang.upper())
        lowIdx = {i for i, s in enumerate(scores) if s < 0.4}

        fm = ["---",
              f'title: "{stem.replace("-", " ").replace("_", " ")}"',
              f'subtitle: "Review: English to {langName} Translation"',
              "document-type: Review", f"lang: {lang}",
              f"date: {datetime.now(UTC).strftime('%Y-%m-%d')}",
              "source-language: English", f"target-language: {langName}",
              "translator: Custom",
              f"translation-confidence: {sum(scores)/len(scores):.1%}",
              f"source-pages: {totalPages}", f"chunks: {len(translated)}",
              f"low-confidence-chunks: {len(lowIdx)}",
              "geometry: margin=1in", "colorlinks: true", "linkcolor: blue", "---"]

        body: List[str] = ["", "---", ""]
        currentPage = None
        for i, (src, tgt, score) in enumerate(zip(chunks, translated, scores)):
            pg = chunkPages[i] if i < len(chunkPages) else 1
            if pg != currentPage:
                if currentPage is not None:
                    body.append("")
                body.append(f"**Page {pg:03d} / {totalPages}**")
                body.append("")
                body.append("| Source (English) | Translation |")
                body.append("|---|---|")
                currentPage = pg
            flag = " ⚠" if i in lowIdx else ""
            srcCell = src.replace("\n", " ").replace("|", "\\|")
            tgtCell = tgt.replace("\n", " ").replace("|", "\\|")
            body.append(f"| {srcCell} | {tgtCell}{flag} |")

        mdPath.write_text("\n".join(fm + body), encoding="utf-8")
        return mdPath


class DefaultRenderer:
    def render(self, mdPath: Path, lang: str) -> Tuple[Path, Path, bool]:
        htmlPath = mdPath.with_suffix(".html")
        pdfPath = mdPath.with_suffix(".pdf")
        self._writeHtml(mdPath, htmlPath, lang)
        pdfOk = self._renderPdf(htmlPath, pdfPath)
        return htmlPath, pdfPath, pdfOk

    def _writeHtml(self, mdPath: Path, htmlPath: Path, lang: str) -> None:
        text = mdPath.read_text(encoding="utf-8")
        fm: Dict[str, str] = {}
        if text.startswith("---\n"):
            end = text.find("\n---\n", 4)
            if end != -1:
                for line in text[4:end].splitlines():
                    if ": " in line:
                        k, _, v = line.partition(": ")
                        fm[k.strip()] = v.strip().strip('"')
                bodyMd = text[end + 5:]
            else:
                bodyMd = text
        else:
            bodyMd = text

        cssPath = Path(__file__).parent / "translate.css"
        css = cssPath.read_text(encoding="utf-8") if cssPath.exists() else ""

        # Header and markdown rendering logic (same as before)
        headerHtml = ""
        if fm:
            title = fm.get("title", "")
            subtitle = fm.get("subtitle", "")
            metaParts = []
            if fm.get("date"): metaParts.append(fm["date"])
            src, tgt = fm.get("source-language", ""), fm.get("target-language", "")
            if src and tgt: metaParts.append(f"{src} → {tgt}")
            if fm.get("translation-confidence"): metaParts.append(f"Confidence: {fm['translation-confidence']}")
            if fm.get("source-pages"): metaParts.append(f"Pages: {fm['source-pages']}")
            if fm.get("chunks"): metaParts.append(f"Chunks: {fm['chunks']}")
            meta = " · ".join(metaParts)
            parts = ['<div class="doc-header">']
            if title: parts.append(f'<p class="doc-title">{title}</p>')
            if subtitle: parts.append(f'<p class="doc-subtitle">{subtitle}</p>')
            if meta: parts.append(f'<p class="doc-meta">{meta}</p>')
            parts.append("</div>")
            headerHtml = "\n".join(parts) + "\n"

        bodyHtml = mdLib.markdown(bodyMd, extensions=["extra"])

        firstMarker = True
        def _markPageBreak(m: re.Match) -> str:
            nonlocal firstMarker
            label = m.group(1)
            if firstMarker:
                firstMarker = False
                return f"<p><strong>{label}</strong></p>"
            return f'<p class="pb"><strong>{label}</strong></p>'

        bodyHtml = re.sub(r"<p><strong>(Page \d+ / \d+)</strong></p>", _markPageBreak, bodyHtml)

        html = f'<!DOCTYPE html>\n<html lang="{lang}">\n<head>\n<meta charset="utf-8">\n<style>{css}</style>\n</head>\n<body>\n{headerHtml}{bodyHtml}\n</body>\n</html>\n'
        htmlPath.write_text(html, encoding="utf-8")

    def _renderPdf(self, htmlPath: Path, pdfPath: Path) -> bool:
        import shutil
        if not shutil.which("weasyprint"):
            return False
        try:
            subprocess.run(["weasyprint", "-e", "utf-8", str(htmlPath), str(pdfPath)],
                           capture_output=True, timeout=180, check=True)
            return True
        except Exception:
            return False


# ---------------------------------------------------------------------------
# Constants & Helpers
# ---------------------------------------------------------------------------
LANG_NAMES = googletrans.LANGUAGES


def getWorkflowDir(pdfPath: Path) -> Path:
    resolved = pdfPath.resolve()
    norm = str(resolved).lower().replace("\\", "/").replace("//", "/")
    norm = str(Path(norm)).replace("\\", "/").lower()
    wid = hashlib.sha256(norm.encode("utf-8")).hexdigest()[:16]
    wdir = Path(".workflows") / wid
    wdir.mkdir(parents=True, exist_ok=True)
    return wdir


def _subTask(progress: Optional[Progress], label: str, total: Optional[int] = None) -> Optional[int]:
    if progress is None:
        return None
    padded = f"    {label:<{_DESC_WIDTH - 4}}"
    return progress.add_task(padded, total=total)


def _shortName(pdfPath: Path, maxLen: int = 20) -> str:
    n = pdfPath.name
    return n if len(n) <= maxLen else n[:maxLen - 1] + "…"


# ---------------------------------------------------------------------------
# Pipeline Functions (extract, clean, chunk, etc.)
# ---------------------------------------------------------------------------

def extractText(pdfPath: Path, wdir: Path, progress: Optional[Progress], log: logging.Logger):
    pagesCache = wdir / "pages.json"
    textCache = wdir / "extracted.txt"
    if pagesCache.exists() and textCache.exists():
        return textCache.read_text(encoding="utf-8"), json.loads(pagesCache.read_text(encoding="utf-8"))
    task = _subTask(progress, "reading PDF pages")
    pages = paginated_plain_text_output(str(pdfPath), sort=True, hyphens=True)
    if task is not None:
        progress.remove_task(task)
    text = "\n\n".join(pages)
    json.dump(pages, pagesCache.open("w", encoding="utf-8"), ensure_ascii=False, indent=2)
    textCache.write_text(text, encoding="utf-8")
    return text, pages


def cleanText(raw: str, wdir: Path, log: logging.Logger, progress: Optional[Progress] = None) -> str:
    cache = wdir / "cleaned.txt"
    if cache.exists():
        return cache.read_text(encoding="utf-8")
    task = _subTask(progress, "cleaning text")
    text = re.sub(r"[ \t]+", " ", raw)
    text = re.sub(r"\n{3,}", "\n\n", text)
    text = re.sub(r"(?m)^\s*(Page\s+\d+\s+of\s+\d+|\d+)\s*$", "", text)
    text = re.sub(r"-\n(?=[a-z])", "", text)
    text = text.strip()
    if task is not None:
        progress.remove_task(task)
    cache.write_text(text, encoding="utf-8")
    return text


def chunkText(cleaned: str, chunkSize: int, wdir: Path, log: logging.Logger, progress: Optional[Progress] = None) -> List[str]:
    cache = wdir / "chunks.json"
    if cache.exists():
        return json.loads(cache.read_text(encoding="utf-8"))
    task = _subTask(progress, "splitting into sentences")
    sentences: List[str] = []
    for para in re.split(r"\n\n+", cleaned):
        if para.strip():
            sentences.extend(_splitSentences(para.strip()))
    chunks: List[str] = []
    current = ""
    for sent in sentences:
        if not sent: continue
        if not current:
            current = sent
        elif len(current) + 1 + len(sent) <= chunkSize:
            current += " " + sent
        else:
            chunks.append(current)
            current = sent
    if current:
        chunks.append(current)
    if task is not None:
        progress.remove_task(task)
    json.dump(chunks, cache.open("w", encoding="utf-8"), ensure_ascii=False, indent=2)
    return chunks


def mapChunksToPages(chunks: List[str], pages: List[str], wdir: Path, log: logging.Logger, progress: Optional[Progress] = None) -> List[int]:
    cache = wdir / "chunk_pages.json"
    if cache.exists():
        return json.loads(cache.read_text(encoding="utf-8"))
    task = _subTask(progress, "mapping chunks to pages")
    from collections import Counter, defaultdict
    wordToPages: Dict[str, List[int]] = defaultdict(list)
    for pageNum, pageText in enumerate(pages, 1):
        for word in set(pageText.lower().split()):
            wordToPages[word].append(pageNum)
    chunkPages = []
    for chunk in chunks:
        counts = Counter()
        for word in chunk.lower().split():
            for pg in wordToPages.get(word, []):
                counts[pg] += 1
        primary = counts.most_common(1)[0][0] if counts else 1
        chunkPages.append(primary)
    if task is not None:
        progress.remove_task(task)
    json.dump(chunkPages, cache.open("w", encoding="utf-8"), ensure_ascii=False, indent=2)
    return chunkPages


def writeTranslatedMarkdown(pdfPath: Path, lang: str, translated: List[str], overall: float, scores: List[float],
                            chunkPages: List[int], totalPages: int, log: logging.Logger) -> Path:
    stem = pdfPath.stem
    mdPath = pdfPath.parent / f"{stem}-{lang}-Translated.md"
    langName = LANG_NAMES.get(lang, lang.upper())
    lowIdx = {i for i, s in enumerate(scores) if s < 0.4}

    fm = ["---",
          f'title: "{stem.replace("-", " ").replace("_", " ")}"',
          f'subtitle: "English to {langName} Translation"',
          f"lang: {lang}",
          f"date: {datetime.now(UTC).strftime('%Y-%m-%d')}",
          "source-language: English",
          f"target-language: {langName}",
          "translator: Custom",
          f"translation-confidence: {overall:.1%}",
          f"source-pages: {totalPages}",
          f"chunks: {len(translated)}",
          f"low-confidence-chunks: {len(lowIdx)}",
          "geometry: margin=1in", "colorlinks: true", "linkcolor: blue", "---"]

    body: List[str] = ["", "---", ""]
    currentPage = None
    for i, (chunk, score) in enumerate(zip(translated, scores)):
        pg = chunkPages[i] if i < len(chunkPages) else 1
        if pg != currentPage:
            if currentPage is not None:
                body.append("---")
            body.append(f"**Page {pg:03d} / {totalPages}**")
            body.append("")
            currentPage = pg
        if i in lowIdx:
            body.append(f"*\\[Low confidence: {score:.0%}\\]*")
        body.append(chunk)
        body.append("")
    mdPath.write_text("\n".join(fm + body), encoding="utf-8")
    return mdPath


def printResult(pdfName: str, lang: str, overall: float, scores: List[float],
                mdPath: Path, htmlPath: Path, pdfPath: Path, pdfOk: bool,
                review_md: Optional[Path] = None, review_pdf: Optional[Path] = None, review_pdf_ok: bool = False):
    if not IS_TTY:
        print(mdPath)
        if pdfOk: print(pdfPath)
        if review_md:
            print(review_md)
            if review_pdf_ok and review_pdf:
                print(review_pdf)
        return

    table = Table(box=box.SIMPLE, show_header=False)
    table.add_row("PDF", pdfName)
    table.add_row("Language", lang)
    table.add_row("Confidence", f"{overall:.1%}")
    table.add_row("Markdown", str(mdPath))
    if pdfOk:
        table.add_row("PDF", str(pdfPath))
    if review_md:
        table.add_row("Review MD", str(review_md))
        if review_pdf_ok and review_pdf:
            table.add_row("Review PDF", str(review_pdf))

    console.print(Panel(table, title=f"✅ {lang.upper()} Complete", border_style="green"))


# ---------------------------------------------------------------------------
# Main Pipeline
# ---------------------------------------------------------------------------

def prepareChunks(pdfPath: Path, chunkSize: int, progress: Progress) -> Tuple[List[str], List[int], int, Path]:
    wdir = getWorkflowDir(pdfPath)
    log = logging.getLogger(f"translate.{wdir.name}")
    log.setLevel(logging.INFO)

    name = _shortName(pdfPath, 22)
    taskId = progress.add_task(_descPad(f"{name}  1/4 reading PDF"), total=4)

    def _step(n: int, verb: str):
        progress.update(taskId, description=_descPad(f"{name}  {n}/4 {verb}"))

    _step(1, "reading PDF")
    raw, pages = extractText(pdfPath, wdir, progress, log)
    progress.update(taskId, advance=1)

    _step(2, "cleaning text")
    cleaned = cleanText(raw, wdir, log, progress)
    progress.update(taskId, advance=1)

    _step(3, "splitting sentences")
    chunks = chunkText(cleaned, chunkSize, wdir, log, progress)
    progress.update(taskId, advance=1)

    _step(4, "mapping to pages")
    chunkPages = mapChunksToPages(chunks, pages, wdir, log, progress)
    progress.update(taskId, advance=1)

    progress.update(taskId, description=_descPad(f"[green]✓[/] {name}  {len(chunks)} chunks ready"), completed=True)
    return chunks, chunkPages, len(pages), wdir


def translateLang(
    pdfPath: Path, lang: str, chunks: List[str], chunkPages: List[int], totalPages: int, wdir: Path,
    translator: Translator, reviewer: DefaultReviewer, renderer: DefaultRenderer,
    progress: Progress, generateReview: bool
) -> None:
    log = logging.getLogger(f"translate.{wdir.name}.{lang}")
    log.setLevel(logging.INFO)

    main_md = pdfPath.parent / f"{pdfPath.stem}-{lang}-Translated.md"
    review_md = pdfPath.parent / f"{pdfPath.stem}-{lang}-ReviewTranslated.md"

    if main_md.exists() and (not generateReview or review_md.exists()):
        console.print(f"[green]✓ Already complete[/] {main_md.name}")
        if generateReview and review_md.exists():
            console.print(f"[green]✓ Review already exists[/] {review_md.name}")
        return

    langName = LANG_NAMES.get(lang, lang.upper())
    name = _shortName(pdfPath, 18)
    taskId = progress.add_task(_descPad(f"[bold yellow]{lang}[/] {name}  1/2 …"), total=2)

    def _step(n: int, verb: str):
        progress.update(taskId, description=_descPad(f"[bold yellow]{lang}[/] {name}  {n}/2 {verb}"))

    t0 = time.monotonic()

    # Translation
    translatedCache = wdir / f"translated_{lang}.json"
    partialTrans = _loadPartial(translatedCache, len(chunks))
    remaining = [(i, chunks[i]) for i in range(len(chunks)) if i not in partialTrans]

    if not remaining:
        _step(1, f"translate cached ({len(chunks)} chunks)")
    else:
        _step(1, f"translating {len(remaining)}/{len(chunks)} → {langName}")
        trTask = _subTask(progress, f"translating into {langName}", total=len(chunks))
        if trTask and len(partialTrans):
            progress.update(trTask, completed=len(partialTrans))

        lock = threading.Lock()
        remIndices = [i for i, _ in remaining]

        def _onDone(localIdx: int, text: str):
            origIdx = remIndices[localIdx]
            with lock:
                partialTrans[origIdx] = text
                _atomicSave(translatedCache, partialTrans)
            if trTask:
                progress.update(trTask, advance=1)

        translator.translateBatch([t for _, t in remaining], "en", lang, _onDone)
        if trTask:
            progress.remove_task(trTask)

    translated = [partialTrans[i] for i in range(len(chunks))]
    progress.update(taskId, advance=1)

    # Review + Score
    _step(2, "scoring quality")
    overall, scores = reviewer.reviewAndScore(chunks, translated, lang, wdir, progress)
    progress.update(taskId, advance=1)

    mdPath = writeTranslatedMarkdown(pdfPath, lang, translated, overall, scores, chunkPages, totalPages, log)
    htmlPath, outPdf, pdfOk = renderer.render(mdPath, lang)

    review_md_path = None
    review_pdf = None
    review_pdf_ok = False

    if generateReview:
        review_md_path = reviewer.generateReviewDocument(pdfPath, lang, chunks, translated, scores, chunkPages, totalPages)
        review_html, review_pdf, review_pdf_ok = renderer.render(review_md_path, lang)
        if IS_TTY:
            console.print(f"[green]✓ Review document rendered[/] {review_md_path.name}")

    progress.update(taskId, description=_descPad(f"[green]✓[/] {lang} {name}  {overall:.0%} confidence"), completed=True)
    printResult(pdfPath.name, lang, overall, scores, mdPath, htmlPath, outPdf, pdfOk,
                review_md=review_md_path, review_pdf=review_pdf, review_pdf_ok=review_pdf_ok)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="PDF Translator")
    parser.add_argument("pdfs", nargs="*", type=Path)
    parser.add_argument("--language", "-l", action="append")
    parser.add_argument("--chunk-size", type=int, default=400)
    parser.add_argument("--translator", default="googletrans")
    parser.add_argument("--workers", type=int, default=None)
    parser.add_argument("--review", action="store_true")
    parser.add_argument("--list-languages", action="store_true")

    args = parser.parse_args()

    if args.workers is None:
        args.workers = max(1, os.cpu_count() // 2)

    if args.list_languages:
        tmp = LLMTranslator(workers=1) if args.translator == "llm" else GoogleTranslator(workers=1)
        langs = tmp.getSupportedLanguages()
        console.print(f"[bold]Supported Languages ({len(langs)}):[/]")
        for code, name in sorted(langs.items()):
            console.print(f"  {code:8} {name}")
        sys.exit(0)

    if not args.pdfs:
        parser.error("pdfs argument is required")
    if not args.language:
        parser.error("--language/-l is required")

    pdfs = list(args.pdfs)
    langs = list(args.language)

    all_jobs = [(pdf, lang) for pdf in pdfs for lang in langs]
    n_jobs = max(1, len(all_jobs))
    chunkWorkers = max(1, args.workers // n_jobs)

    if args.translator == "googletrans":
        translator: Translator = GoogleTranslator(workers=chunkWorkers)
    elif args.translator == "llm":
        translator = LLMTranslator(workers=chunkWorkers)
    else:
        translator = SubprocessTranslator(args.translator)

    reverseTranslator = GoogleTranslator(workers=chunkWorkers)
    reviewer = DefaultReviewer(reverseTranslator)
    renderer = DefaultRenderer()

    try:
        with makeProgress() as progress:
            with ThreadPoolExecutor(max_workers=n_jobs) as pool:
                # Prepare PDFs
                prep_futs: Dict[Path, Future] = {
                    pdf: pool.submit(prepareChunks, pdf, args.chunk_size, progress)
                    for pdf in pdfs
                }
                for f in as_completed(prep_futs.values()):
                    f.result()

                prepared = {pdf: f.result() for pdf, f in prep_futs.items()}

                # Translate languages
                trans_futs = [
                    pool.submit(translateLang, pdf, lang, *prepared[pdf],
                                translator, reviewer, renderer, progress, args.review)
                    for pdf, lang in all_jobs
                ]
                for f in as_completed(trans_futs):
                    f.result()

        if IS_TTY:
            console.print("\n[bold green]All done![/]")

    except KeyboardInterrupt:
        console.print("\n[yellow]Interrupted.[/] Partial results saved in .workflows/")
        sys.exit(130)


if __name__ == "__main__":
    main()