"""
docs-site/pdfbook/build_pdf.py
Builds a single, O'Reilly-style PDF "book" from the MkDocs chapters in
docs-site/docs/*.md, driven by mkdocs.yml's nav order. Pre-rendered static
images under docs/assets/ are used directly wherever one already exists for
a diagram; any remaining ```mermaid fenced block without a matching static
asset is swapped for the cached PNG from render_mermaid.py. Everything else
(headings, tables, code, images, admonitions) converts through Python-Markdown
to HTML and is rendered to PDF with xhtml2pdf (pure-Python, no native deps).

Usage:
    python docs-site/pdfbook/render_mermaid.py          # once, or whenever diagrams change
    python docs-site/pdfbook/build_pdf.py --audience internal   # default
    python docs-site/pdfbook/build_pdf.py --audience external   # client-free, public-safe
Output:
    docs-site/pdfbook/MedEnroll834-Documentation-Internal.pdf
    docs-site/pdfbook/MedEnroll834-Documentation-External.pdf
"""
import argparse
import re
import sys
from pathlib import Path

import markdown
import yaml
from PIL import Image
from xhtml2pdf import pisa
from xhtml2pdf.config.resources import ResourceAccessPolicy

ROOT = Path(__file__).resolve().parent.parent.parent
DOCS_SITE = ROOT / "docs-site"
DOCS_DIR = DOCS_SITE / "docs"
PDFBOOK_DIR = DOCS_SITE / "pdfbook"
MERMAID_DIR = PDFBOOK_DIR / "build" / "mermaid"
IMG_CACHE_DIR = PDFBOOK_DIR / "build" / "img_cache"
# Chapters that discuss the legacy system/client context have a sensitive
# variant that must NEVER live under docs/ (docs/ is served verbatim by the
# public GitHub Pages site and is the public repo's working tree - anything
# there is public, nav-listed or not). The sensitive content instead lives
# only under pdfbook/internal_source/, which is read solely by this script
# for --audience internal, and is itself git-ignored (see .gitignore) so it
# never reaches the public GitHub repository at all.
INTERNAL_SOURCE_DIR = PDFBOOK_DIR / "internal_source"


# Several screenshots/exports under docs/assets/img/ exceed xhtml2pdf's
# comfortable raster size (some are 8000+ px on a side). Rather than feeding
# those multi-megapixel originals straight into the PDF (slow to rasterize,
# bloats file size, no visual benefit since the page is a few inches wide),
# any source image wider or taller than this is downscaled once into a
# cached copy under build/img_cache/ and that copy is embedded instead -
# the original file on disk is never modified.
MAX_IMG_DIMENSION = 1600

BOOK_TITLE = "MedEnroll834"
BOOK_AUTHOR = "SIVABALA SELVAM"

# docs/*.md is the single, public source of truth read by both the live
# GitHub Pages site (mkdocs.yml's nav) and the external PDF build - it must
# never contain a client name or a specific legacy tool name. The internal
# PDF build additionally overlays a handful of chapters with sensitive
# content (client context, real legacy toolchain) from INTERNAL_SOURCE_DIR,
# which lives outside docs/ and is git-ignored, so it is never present in
# the public repository or public site regardless of mkdocs.yml's nav.
INTERNAL_ONLY_CHAPTERS = {
    "02-business-problem.md",
    "03-legacy-architecture.md",
    "34-legacy-vs-modern-benchmark.md",
    "35-business-impact.md",
}
BOOK_SUBTITLE = {
    "internal": "Documentation (Internal) \u2014 A Modern Distributed Data Platform for Growing Data",
    "external": "Documentation \u2014 A Modern Distributed Data Platform for Growing Data",
}
OUT_PDF_NAME = {
    "internal": "MedEnroll834-Documentation-Internal.pdf",
    "external": "MedEnroll834-Documentation-External.pdf",
}

MERMAID_FENCE = re.compile(r"```mermaid\n(.*?)\n```", re.DOTALL)
IMG_TAG = re.compile(r'<img\s+[^>]*src="(?P<src>[^"]+)"[^>]*/?>')



def load_nav_chapters(audience: str) -> list[tuple[str, str]]:
    """Read mkdocs.yml's nav to get (section, chapter-file) pairs in reading
    order. The file uses mkdocs-material's custom !!python/name: YAML tags
    for emoji extensions, which yaml.safe_load() can't parse - so extract
    just the `nav:` block as a standalone YAML document instead of loading
    the whole file. Filenames are returned unchanged here regardless of
    audience; build_chapter_html() decides whether to read a given filename
    from docs/ (always, for external) or from INTERNAL_SOURCE_DIR (for
    internal, only for INTERNAL_ONLY_CHAPTERS)."""
    mkdocs_yml_text = (DOCS_SITE / "mkdocs.yml").read_text(encoding="utf-8")
    nav_match = re.search(r"^nav:\n((?:.*\n?)*)", mkdocs_yml_text, re.MULTILINE)
    if not nav_match:
        raise RuntimeError("Could not find 'nav:' section in mkdocs.yml")
    nav_yaml = "nav:\n" + nav_match.group(1)
    cfg = yaml.safe_load(nav_yaml)
    chapters: list[tuple[str, str]] = []
    for entry in cfg["nav"]:
        for section_title, pages in entry.items():
            if section_title == "Home":
                continue
            for page in pages:
                for _title, filename in page.items():
                    chapters.append((section_title, filename))
    return chapters






def replace_mermaid_with_images(md_text: str, stem: str) -> str:
    """Swap any remaining ```mermaid fenced block for an <img> tag pointing
    at its pre-rendered PNG (render_mermaid.py). Most chapters already
    reference a static diagram under docs/assets/img/ directly in Markdown
    (![...](assets/img/...)) instead of an inline mermaid fence, so this
    only applies to the few chapters that still have a live fence with no
    matching static asset."""
    counter = {"n": 0}

    def _sub(match: re.Match) -> str:
        counter["n"] += 1
        png = MERMAID_DIR / f"{stem}-{counter['n']}.png"
        if not png.exists():
            return match.group(0)
        return f'\n<img src="{png.as_posix()}" class="diagram" />\n'

    return MERMAID_FENCE.sub(_sub, md_text)


def _cached_compressed_image(abs_path: Path) -> Path:
    """Return a path safe to hand to xhtml2pdf: the original file untouched
    if it's already a reasonable size, or a cached downscaled copy under
    build/img_cache/ if either dimension exceeds MAX_IMG_DIMENSION (some
    screenshots/exports under docs/assets/img/ are 8000+ px on a side).
    Only raster formats (PNG/JPEG/etc.) are touched - SVGs pass through
    unchanged since xhtml2pdf rasterizes them separately and they have no
    fixed pixel dimension to downscale."""
    if abs_path.suffix.lower() not in (".png", ".jpg", ".jpeg", ".bmp", ".gif", ".tiff"):
        return abs_path
    if not abs_path.exists():
        return abs_path

    IMG_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    cache_path = IMG_CACHE_DIR / f"{abs_path.stem}{abs_path.suffix}"
    if cache_path.exists() and cache_path.stat().st_mtime >= abs_path.stat().st_mtime:
        return cache_path

    with Image.open(abs_path) as img:
        width, height = img.size
        if max(width, height) <= MAX_IMG_DIMENSION:
            return abs_path  # already small enough, embed the original directly
        scale = MAX_IMG_DIMENSION / max(width, height)
        new_size = (max(1, int(width * scale)), max(1, int(height * scale)))
        resized = img.convert("RGB") if img.mode in ("P", "RGBA") and abs_path.suffix.lower() in (".jpg", ".jpeg") else img
        resized = resized.resize(new_size, Image.LANCZOS)
        resized.save(cache_path)
        print(f"  compressed {abs_path.name}: {width}x{height} -> {new_size[0]}x{new_size[1]}")
    return cache_path


def resolve_relative_images(md_text: str, chapter_dir: Path) -> str:
    """Rewrite markdown image refs (![alt](assets/img/x.svg)) to absolute file
    paths so xhtml2pdf can load them regardless of its own working directory,
    swapping in a downscaled cached copy for any oversized raster image (see
    _cached_compressed_image) while leaving the original file on disk as-is."""
    def _sub(match: re.Match) -> str:
        alt, rel = match.group(1), match.group(2)
        if rel.startswith(("http://", "https://", "/")):
            return match.group(0)
        abs_path = (chapter_dir / rel).resolve()
        abs_path = _cached_compressed_image(abs_path)
        return f"![{alt}]({abs_path.as_posix()})"

    return re.sub(r"!\[([^\]]*)\]\(([^)]+)\)", _sub, md_text)


def strip_mkdocs_only_syntax(md_text: str) -> str:
    """Remove/convert MkDocs-Material-only syntax that plain Python-Markdown
    doesn't understand (admonition content blocks use a different extension
    name but the same '!!! type "title"' syntax, so no change needed there)."""
    # mkdocs-material attribute lists like {: .class} are rare here; left as-is.
    return md_text


def build_cover_html(audience: str) -> str:
    kicker = "DOCUMENTATION" if audience == "external" else "DOCUMENTATION \u00b7 INTERNAL"
    return f"""
<div class="cover">
  <div class="cover-kicker">{kicker}</div>
  <h1 class="cover-title">{BOOK_TITLE}</h1>
  <div class="cover-subtitle">{BOOK_SUBTITLE[audience]}</div>
  <div class="cover-rule"></div>
  <div class="cover-meta">
    <div>PySpark &middot; Delta Lake &middot; Databricks &middot; Apache Airflow &middot; PostgreSQL</div>
    <div class="cover-author">{BOOK_AUTHOR}</div>
  </div>
</div>
<pdf:nextpage />
"""


def build_toc_page() -> str:
    return (
        '<div class="toc-page">'
        '<h1 class="toc-heading">Table of Contents</h1>'
        '<pdf:toc leader="dots" />'
        "</div>"
        "<pdf:nextpage />"
    )


def markdown_to_html(md_text: str) -> str:
    return markdown.markdown(
        md_text,
        extensions=[
            "extra",
            "admonition",
            "toc",
            "tables",
            "fenced_code",
            "codehilite",
            "sane_lists",
        ],
        extension_configs={
            "codehilite": {"guess_lang": False, "noclasses": False, "pygments_style": "friendly"},
        },
    )


def build_chapter_html(section_title: str, filename: str, is_first_in_section: bool, audience: str) -> str:
    # For audience=internal, chapters in INTERNAL_ONLY_CHAPTERS are read from
    # pdfbook/internal_source/ (sensitive client/legacy-toolchain content,
    # git-ignored, never in docs/). Every other chapter, and every chapter
    # for audience=external, is read from docs/ - the single public source
    # of truth also served by the live GitHub Pages site.
    if audience == "internal" and filename in INTERNAL_ONLY_CHAPTERS:
        source_dir = INTERNAL_SOURCE_DIR
        if not (source_dir / filename).exists():
            raise FileNotFoundError(
                f"Internal PDF build requires {source_dir / filename} - "
                f"see docs-site/pdfbook/README.md for how to populate "
                f"internal_source/ (never commit this directory's contents)."
            )
    else:
        source_dir = DOCS_DIR
    chapter_path = source_dir / filename
    stem = chapter_path.stem
    md_text = chapter_path.read_text(encoding="utf-8")
    md_text = replace_mermaid_with_images(md_text, stem)
    md_text = resolve_relative_images(md_text, DOCS_DIR)
    body_html = markdown_to_html(md_text)

    section_banner = ""
    if is_first_in_section:
        section_banner = f'<div class="part-banner">{section_title}</div>'

    return (
        f'<div class="chapter" id="{stem}">'
        f"{section_banner}"
        f"{body_html}"
        "</div>"
        "<pdf:nextpage />"
    )


def build_full_html(audience: str) -> str:
    chapters = load_nav_chapters(audience)
    footer_html = (
        '<div id="footer_content">MedEnroll834 Documentation &nbsp;|&nbsp; '
        'Page <pdf:pagenumber /> of <pdf:pagecount /></div>'
    )
    parts = [footer_html, build_cover_html(audience), build_toc_page()]
    seen_sections: set[str] = set()

    for section_title, filename in chapters:
        is_first = section_title not in seen_sections
        seen_sections.add(section_title)
        parts.append(build_chapter_html(section_title, filename, is_first, audience))
    body = "\n".join(parts)
    css = (PDFBOOK_DIR / "book.css").read_text(encoding="utf-8")
    fonts_dir = (PDFBOOK_DIR / "fonts").resolve().as_posix()
    css = css.replace('url("fonts/', f'url("{fonts_dir}/')
    return f"""<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8" />
<title>{BOOK_TITLE} — {BOOK_SUBTITLE[audience]}</title>
<style>
{css}

</style>
</head>
<body>
{body}
</body>
</html>"""


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--audience", choices=["internal", "external", "both"], default="internal",
        help="internal: names the client + legacy toolchain. external: fully "
             "generalized, safe for public/portfolio use. both: build both PDFs.",
    )
    args = parser.parse_args()
    audiences = ["internal", "external"] if args.audience == "both" else [args.audience]

    if not MERMAID_DIR.exists() or not any(MERMAID_DIR.glob("*.png")):
        print("WARNING: no pre-rendered mermaid PNGs found - run render_mermaid.py first.")

    exit_code = 0
    for audience in audiences:
        print(f"\n=== Building {audience} PDF ===")
        html = build_full_html(audience)
        debug_html_path = PDFBOOK_DIR / "build" / f"book-{audience}.html"
        debug_html_path.parent.mkdir(parents=True, exist_ok=True)
        debug_html_path.write_text(html, encoding="utf-8")
        print(f"Intermediate HTML written to {debug_html_path}")

        out_pdf = PDFBOOK_DIR / OUT_PDF_NAME[audience]
        with open(out_pdf, "wb") as pdf_file:
            policy = ResourceAccessPolicy(base_dir=ROOT, max_local_bytes=50 * 1024 * 1024)
            result = pisa.CreatePDF(html, dest=pdf_file, encoding="utf-8", resource_policy=policy)
        if result.err:
            print(f"xhtml2pdf reported {result.err} error(s) for {audience}")
            exit_code = 1
            continue
        print(f"PDF written to {out_pdf} ({out_pdf.stat().st_size / 1024:.0f} KB)")
    return exit_code


if __name__ == "__main__":
    sys.exit(main())

