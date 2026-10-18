# MedEnroll834 PDF Book

Builds a downloadable PDF — styled like an O'Reilly technical book (cover,
table of contents, chapter headers, syntax-highlighted code, repo-tree/
box-drawing diagrams, tables, admonitions) — from the same chapters that
power the [MedEnroll834 documentation site](../docs/index.md).

Intended for sharing as a standalone document (e.g. a LinkedIn portfolio
post), separate from the documentation site itself. Builds in two audience
variants — see [Internal vs. external audience](#internal-vs-external-audience)
below.

## Prerequisites

```bash
pip install -r docs-site/pdfbook/requirements-pdfbook.txt
```

xhtml2pdf **>= 0.2.26** is required — 0.2.23 has a known image-identity bug
where multiple distinct `<img>` sources in one document can collapse onto
the same cached XObject, silently swapping one diagram's picture for
another's. If a diagram looks wrong after a fresh install, check
`pip show xhtml2pdf` and upgrade.

Mermaid diagrams without an existing static asset are rendered through the
public [mermaid.ink](https://mermaid.ink) HTTP API (no local browser/
Puppeteer dependency), so an internet connection is required for the
render step only — most chapters already reference a pre-rendered image
under `docs/assets/img/` directly instead.

Pillow (`PIL`) is used to auto-downscale any oversized screenshot/export
under `docs/assets/img/` before embedding it — see
[Large image handling](#large-image-handling) below.

## Building


```bash
cd docs-site/pdfbook

# 1. Render any ```mermaid fenced block that has no matching static asset
#    to a cached PNG. Only needs to be re-run when a diagram's source
#    changes; results are cached under build/mermaid/ and skipped on
#    subsequent runs.
python render_mermaid.py

# 2. Assemble the cover, table of contents, and all chapters (in the
#    same order as mkdocs.yml's nav) into a PDF for one or both audiences.
python build_pdf.py --audience internal   # default
python build_pdf.py --audience external
python build_pdf.py --audience both
```

Output:

- `docs-site/pdfbook/MedEnroll834-Documentation-Internal.pdf`
- `docs-site/pdfbook/MedEnroll834-Documentation-External.pdf`

## Internal vs. external audience

Chapter 2 (Business Problem) has two source files:

| Audience | Chapter 2 source | Content |
|---|---|---|
| `internal` | `docs/02-business-problem.md` | References the real client this project's requirements are modeled on (AmeriHealth Caritas) and the actual legacy toolchain (EDI, ECA, .NET, TTK, Bash, W2K, Tidal, SSIS, DataStage, FACETS). **Internal use only — do not publish.** |
| `external` | `docs/02-business-problem-external.md` | Fully generalized — no client name, no legacy tool names. Safe for a public portfolio site, LinkedIn, or GitHub README. |

Every other chapter is identical between the two builds; `build_pdf.py`
swaps only this one file based on `--audience`, and labels the cover page
accordingly ("DOCUMENTATION · INTERNAL" vs. "DOCUMENTATION"). The
internal chapter deliberately avoids revealing that a separate external
variant exists — the client/legacy-toolchain context stays behind its own
confidentiality admonition without cross-linking to the public file.

## Large image handling

Several screenshots/exports under `docs/assets/img/` (test evidence,
architecture diagrams) are much larger than needed for a PDF page — some
exceed 8000px on a side. `build_pdf.py` never edits those source files;
instead, any raster image (`.png`/`.jpg`/etc.) wider or taller than 1600px
is downscaled once into a cached copy under `build/img_cache/` and that
copy is what gets embedded. Re-run `build_pdf.py` after replacing an image
and it re-detects staleness via mtime and regenerates the cache entry.

## How it works


| File | Role |
|---|---|
| `render_mermaid.py` | Extracts ` ```mermaid ` fenced blocks that have no matching static asset from `docs/*.md`, renders each to a PNG via mermaid.ink, caches under `build/mermaid/`. |
| `build_pdf.py` | Reads `mkdocs.yml`'s `nav:` for chapter order (swapping chapter 2 per `--audience`), converts each chapter's Markdown to HTML (Python-Markdown + `extra`/`admonition`/`tables`/`codehilite`), swaps any remaining mermaid fence for its cached PNG, resolves `assets/img/*`/`assets/architecture/*` references to absolute paths (downscaling oversized rasters via a cached copy — see above), and renders the whole document to PDF with [xhtml2pdf](https://xhtml2pdf.readthedocs.io/) (pure-Python, no native/browser dependency). |
| `book.css` | O'Reilly-style print stylesheet: a high-contrast crimson accent color, serif-free body text, syntax-highlighted code blocks, striped tables, colored admonition boxes, a running footer with live page numbers (`<pdf:pagenumber/>`/`<pdf:pagecount/>`), and an auto-generated `<pdf:toc leader="dots"/>` with dot leaders. |
| `fonts/` | [DejaVu Sans / DejaVu Sans Mono](https://dejavu-fonts.github.io/) (Bitstream Vera License — see `fonts/LICENSE`), embedded via `@font-face` so arrows (→), box-drawing characters (├─└│) used in repo-tree diagrams, and math comparisons (≤ ≥) all render correctly — xhtml2pdf's built-in Helvetica/Courier aliases don't cover them. |

## Regenerating after a chapter edit

Just re-run `python build_pdf.py --audience both` (step 2) — it always
reads the current `docs/*.md` content. Only re-run `render_mermaid.py` if
you changed a ` ```mermaid ` diagram's source that has no static asset
equivalent.

