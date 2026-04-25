# PDF Translator

Translate PDFs into any language via the Google Translate unofficial API.
Fully resumable — kill and restart at any point without losing progress.

## Features

- Text extraction via [pdftext](https://github.com/datalab-to/pdftext)
- Translation via [googletrans](https://pypi.org/project/googletrans/) (unofficial Google Translate API)
- Per-chunk parallelism (configurable threads) and multi-PDF parallelism
- All intermediate state cached under `.workflows/<id>/` — resume after any crash
- Reverse-translation for confidence scoring
- Structured logging with `--log-level` and optional `--log-file`

## Output files

For each translated PDF `data/Foo.pdf`, the tool writes:

| File | Location | Description |
|---|---|---|
| `Foo-Translated.md` | same dir as source PDF | Pandoc-compatible Markdown, ready to render |
| `Foo-Translated.pdf` | same dir as source PDF | Rendered PDF (requires pandoc + a TeX engine) |
| `.workflows/<id>/output_ta.txt` | workflow dir | Internal artifact with per-chunk confidence scores |
| `.workflows/<id>/run.log` | workflow dir | Full run log, tail -f during long runs |

### Pandoc PDF requirements

For PDF generation you need [pandoc](https://pandoc.org/installing.html) and a PDF engine:

```bash
# macOS
brew install pandoc mactex

# Ubuntu / Debian
sudo apt install pandoc texlive-xetex

# Windows (scoop)
scoop install pandoc miktex
```

For non-Latin scripts (Tamil, Hindi, Arabic, Japanese, etc.) the default LaTeX fonts won't work.
Install the [Noto font](https://fonts.google.com/noto) for your target language (e.g. *Noto Serif Tamil*)
and XeLaTeX will pick it up via the `mainfont` field in the Markdown front matter.

If pandoc is not installed the tool still runs completely — it writes the `.md` file and logs a warning
with the exact `pandoc` command you can run manually later.

## Setup

> **Important:** `googletrans==4.0.0-rc1` pins `httpx==0.13.3`.
> Use a virtual environment to avoid breaking other tools.

```bash
python -m venv .venv

# Windows
.venv\Scripts\activate

# macOS / Linux
source .venv/bin/activate

pip install -r requirements.txt
```

## Usage

```bash
# Translate one PDF to Tamil
python translate.py --language=ta data/Freelance.pdf

# Verbose debug output + log file
python translate.py --language=ta --log-level=DEBUG --log-file=run.log data/Freelance.pdf

# Batch: 3 PDFs, 2 PDFs at a time, 6 translation threads each
python translate.py --language=hi --pdf-workers=2 --translate-workers=6 data/*.pdf

# Resume an interrupted run (re-run the exact same command — cached steps are skipped)
python translate.py --language=ta data/Freelance.pdf
```

### All options

| Flag | Default | Description |
|---|---|---|
| `--language` | *(required)* | Target language code (`ta`, `hi`, `fr`, `de`, …) |
| `--chunk-size` | `400` | Characters per translation chunk |
| `--translate-workers` | `4` | Parallel translation threads per PDF |
| `--pdf-workers` | `2` | PDFs processed concurrently |
| `--workflows-dir` | `.workflows` | Root directory for all artifacts |
| `--log-level` | `INFO` | `DEBUG` / `INFO` / `WARNING` / `ERROR` |
| `--log-file` | *(none)* | Also write logs to this file path |

## Workflow layout

Every PDF gets a stable directory derived from a SHA-256 hash of its resolved path:

```
.workflows/
  a3f9c1b2e4d87f60/          ← workflow ID (sha256 of resolved path)[:16]
    meta.json                ← source path, language, chunk_size, created_at
    extracted.txt            ← raw text from pdftext
    cleaned.txt              ← whitespace/page-number cleaned text
    chunks.json              ← array of text chunks
    translated_ta.json       ← per-chunk translations (null = not yet done)
    reverse_ta.json          ← back-translated to English for scoring
    output_ta.txt            ← final output with per-chunk confidence
    translate.log            ← log file (if --log-file used)
```

Deleting a file forces that step to re-run on the next invocation.
Deleting the whole workflow directory starts fresh.

## Output format

```
# Source:     Freelance.pdf
# Language:   ta
# Confidence: 73.41%
# Workflow:   a3f9c1b2e4d87f60
# Generated:  2026-04-24T10:30:00

<!-- chunk 1/142 | confidence 81.20% -->
<translated text>

<!-- chunk 2/142 | confidence 76.44% -->
<translated text>
...
```

Confidence is computed as the token-level sequence similarity between the original
English text and the back-translated (Tamil → English) text.
Low-confidence chunks (<40%) are flagged in the log as warnings.

## Resumability

Each translation step saves state after **every chunk**. If the process is killed
mid-run, re-running the same command picks up from the last saved chunk.
In-flight chunks at crash time (≤ `--translate-workers` chunks) are the only ones
that may need to be re-translated.

## Language codes

Common codes: `ta` (Tamil), `hi` (Hindi), `fr` (French), `de` (German),
`es` (Spanish), `zh-cn` (Chinese Simplified), `ar` (Arabic), `ja` (Japanese).

Full list: https://py-googletrans.readthedocs.io/en/latest/#googletrans-languages

## License

MIT — see [LICENSE](LICENSE.md).

## Thank You and Feedback

All feedback welcome!

* Author: Karthik Kumar Viswanathan
* Web   : [karthikkumar.org](http://karthikkumar.org)
* Email : [me@karthikkumar.org](mailto:me@karthikkumar.org)
