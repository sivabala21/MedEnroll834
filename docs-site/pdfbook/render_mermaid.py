"""
docs-site/pdfbook/render_mermaid.py
Extracts every ```mermaid fenced block from docs-site/docs/*.md and renders
it to a standalone PNG via the mermaid.ink HTTP rendering service (no local
browser/Puppeteer dependency, which is unreliable in sandboxed/offline CI
environments) so the PDF book can embed a static raster image instead of
live JS.

Output: docs-site/pdfbook/build/mermaid/<chapter-stem>-<n>.png
"""
import base64
import json
import re
import sys
import time
from pathlib import Path

import requests

DOCS_DIR = Path(__file__).resolve().parent.parent / "docs"
OUT_DIR = Path(__file__).resolve().parent / "build" / "mermaid"
MERMAID_FENCE = re.compile(r"```mermaid\n(.*?)\n```", re.DOTALL)

MERMAID_INK_URL = "https://mermaid.ink/img/{b64}"
THEME = {"theme": "neutral"}


def _encode(diagram_src: str) -> str:
    state = {"code": diagram_src, "mermaid": THEME}
    return base64.urlsafe_b64encode(json.dumps(state).encode("utf-8")).decode("ascii")


def render_all() -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    rendered = 0
    for md_file in sorted(DOCS_DIR.glob("*.md")):
        text = md_file.read_text(encoding="utf-8")
        for i, match in enumerate(MERMAID_FENCE.finditer(text), start=1):
            diagram_src = match.group(1)
            stem = md_file.stem
            png_path = OUT_DIR / f"{stem}-{i}.png"
            if png_path.exists() and png_path.stat().st_size > 0:
                print(f"skipping {stem}-{i} (already rendered)")
                rendered += 1
                continue
            b64 = _encode(diagram_src)
            url = MERMAID_INK_URL.format(b64=b64) + "?type=png&width=2000"

            print(f"rendering {stem}-{i} ...")
            for attempt in range(3):
                try:
                    resp = requests.get(url, timeout=30)
                    if resp.status_code == 200 and resp.content:
                        png_path.write_bytes(resp.content)
                        rendered += 1
                        break
                    print(f"  attempt {attempt + 1} failed: HTTP {resp.status_code}")
                except requests.RequestException as exc:
                    print(f"  attempt {attempt + 1} error: {exc}")
                time.sleep(2)
            else:
                print(f"  FAILED to render {stem}-{i} after 3 attempts")
                return 1
    print(f"\nRendered {rendered} mermaid diagram(s) to {OUT_DIR}")
    return 0


if __name__ == "__main__":
    sys.exit(render_all())
