# PDF Translate

[![PyPI](https://img.shields.io/pypi/v/pdf-translate)](https://pypi.org/project/pdf-translate/)
[![GitHub](https://img.shields.io/badge/github-guilt%2Fpdf--translate-blue)](https://github.com/guilt/pdf-translate)

A fast, resumable PDF translator that supports **Google Translate** (free) and **LLM** backends (OpenAI-compatible).

Fully resumable — you can kill the process at any time and resume later without losing progress.

## Features

- Text extraction via [pdftext](https://github.com/datalab-to/pdftext)
- Translation via [googletrans](https://pypi.org/project/googletrans/) (unofficial Google Translate API) or [OpenAI](https://pypi.org/project/openai/)  compatible LLM.
- Smart sentence-based chunking
- **Review mode** with back-translation and confidence scoring
- Automatic generation of clean HTML and PDF via [WeasyPrint CLI](https://weasyprint.org/)
- All intermediate results cached under `.workflows/` and resume on crash

## Installation

```bash
pip install pdf-translate
```

or

```bash
pip install git+https://github.com/guilt/pdf-translate
```

## Setup from Source

```bash
git clone https://github.com/guilt/pdf-translate
cd pdf-translate

python -m venv .venv

# Windows
.venv\Scripts\activate

# macOS / Linux
source .venv/bin/activate

pip install .
```

## Usage

### 1. Google Translate (Free & Simple)

```bash
# Single language
pdf-translate --language ta data/FRF-Interim-Final-Rule-Freelance.pdf

# Multiple languages with review
pdf-translate --language ta --language hi --review data/FRF-Interim-Final-Rule-Freelance.pdf
```

### 2. LLM Mode

```bash
# Set environment variables
export OPENAI_API_KEY="sk-..."
export OPENAI_MODEL="gpt-4o-mini"

# Run translation
pdf-translate --translator llm --language ta --review data/FRF-Interim-Final-Rule-Freelance.pdf
```

#### Popular LLM Provider Examples

### Grok
```bash
export OPENAI_API_KEY="xai-..."
export OPENAI_BASE_URL="https://api.x.ai/v1"
export OPENAI_MODEL="grok-4.20"
```

### OpenRouter
```bash
export OPENAI_API_KEY="sk-..."     # Your Anthropic key
export OPENAI_BASE_URL="https://openrouter.ai/api/v1"
export OPENAI_MODEL="anthropic/claude-opus-4-7"
```

### Claude
```bash
export OPENAI_API_KEY="sk-ant-..."     # Your Anthropic key
export OPENAI_BASE_URL="https://api.anthropic.com/v1"
export OPENAI_MODEL="claude-opus-4-7"
```

### Qwen
```bash
export OPENAI_API_KEY="sk-..."
export OPENAI_BASE_URL="https://dashscope.aliyuncs.com/compatible-mode/v1"
export OPENAI_MODEL="qwen3.5-plus"
```

### OpenAI
```bash
export OPENAI_API_KEY="sk-..."
export OPENAI_MODEL="gpt-5.5"
```

### DeepSeek
```bash
export OPENAI_API_KEY="sk-..."
export OPENAI_BASE_URL="https://api.deepseek.com/v1"
export OPENAI_MODEL="deepseek-chat"
```

### Ollama
```bash
export OPENAI_API_KEY="ollama"
export OPENAI_BASE_URL="http://localhost:11434/v1"
export OPENAI_MODEL="gpt-oss:latest"
```

### All Options

| Flag                  | Default              | Description |
|-----------------------|----------------------|-----------|
| `--language`, `-l`    | **required**         | Target language code (can be used multiple times) |
| `--translator`        | `googletrans`        | `googletrans` or `llm` |
| `--workers`           | `CPU cores / 2`      | Total concurrent translation workers |
| `--chunk-size`        | `400`                | Maximum characters per chunk |
| `--review`            | `false`              | Generate side-by-side review documents |
| `--list-languages`    | —                    | Show all supported languages and exit |

## Output Files

For a file `document.pdf` translated to Tamil (`ta`):

- `document-ta-Translated.md`,`document-ta-Translated.html` and `document-ta-Translated.pdf` (PDF when **WeasyPrint** is available)
- `document-ta-ReviewTranslated.md`, `document-ta-ReviewTranslated.html` and `document-ta-ReviewTranslated.pdf` (Only with `--review`)

All files are saved in the same directory as the source PDF.

## Review Mode

This is by adding an optional flag `--review`

- Performs back-translation (translated text → English)
- Calculates confidence score for each chunk
- Generates a rich side-by-side comparison table
- Flags low-confidence chunks (< 40%) with ⚠

## Language Codes

Run this command to see all supported languages:
```bash
python translate.py --list-languages
```

Examples: `ta` (Tamil), `hi` (Hindi), `zh-cn` (Chinese), `ar` (Arabic), `ja` (Japanese), `fr` (French), `de` (German),
 `es` (Spanish) etc.

## Development

Install with dev and other dependencies:

```bash
pip install -e ".[dev,pdf,llm]"
```

Tests print full translation output by default (`-s -v` is configured in `pyproject.toml`).

## Thank You and Feedback

All feedback welcome!

* Author: Karthik Kumar Viswanathan
* Web   : [karthikkumar.org](http://karthikkumar.org)
* Email : [me@karthikkumar.org](mailto:me@karthikkumar.org)