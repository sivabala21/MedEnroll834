# MedEnroll834 Documentation (docs-site)

This folder contains the **GitBook-style documentation site** for the
MedEnroll834 project, built with [MkDocs](https://www.mkdocs.org/) +
[Material for MkDocs](https://squidfunk.github.io/mkdocs-material/) and the
[mermaid2](https://github.com/fralau/mkdocs-mermaid2-plugin) plugin for
native rendering of the project's architecture diagrams.

It is **free to host** on GitHub Pages — no GitBook.com account or paid
plan required.

## 37 chapters, organized docs-as-code

All 37 chapters live under `docs/` as plain Markdown files, numbered
`01-project-overview.md` through `37-future-improvements.md`, grouped into
6 sections in `mkdocs.yml`'s `nav`. Every chapter is written from, and
links back to, the actual source code in this repository — see the root
[`README.md`](../README.md) for the project overview this documentation
expands on.


## Running locally

```bash
pip install -r docs-site/requirements-docs.txt
cd docs-site
mkdocs serve
# -> http://127.0.0.1:8000
```

`mkdocs serve` live-reloads on file changes, so editing a chapter and
saving immediately refreshes the browser preview.

## Building a static site

```bash
cd docs-site
mkdocs build --strict
# output: docs-site/site/ (git-ignored, not committed)
```

`--strict` turns broken internal links/anchors into build failures — the
same check the GitHub Actions deploy workflow runs.

## Deploying to GitHub Pages

Deployment is automated by
[`.github/workflows/docs.yml`](../.github/workflows/docs.yml): any push to
`main` that touches `docs-site/**` triggers `mkdocs gh-deploy`, which
builds the site and force-pushes it to the `gh-pages` branch. GitHub Pages
then serves that branch automatically once enabled in the repository's
**Settings → Pages → Source → Deploy from a branch → `gh-pages`**.

To deploy manually instead:

```bash
cd docs-site
mkdocs gh-deploy --force
```

## Editing an existing chapter

Each rendered page has an "Edit this page" pencil icon (via `edit_uri` in
`mkdocs.yml`) linking straight back to the chapter's source file on GitHub
for quick corrections.

## Adding a new chapter

1. Create `docs/NN-chapter-slug.md`.
2. Add it to the `nav:` section of `mkdocs.yml` under the appropriate group.
3. Run `mkdocs build --strict` locally to confirm no broken links before
   pushing.
