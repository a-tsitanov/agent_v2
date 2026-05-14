# kb-llamaindex — Conference Decks

Two Marp Markdown decks for the project conference talks.

- `kb-llamaindex-conf-A.md` — Tech / ML conference (22 slides, ~35 min).
- `kb-llamaindex-conf-D.md` — Internal project defense (23 slides, ~35 min, w/ speaker notes).

Design spec: `../superpowers/specs/2026-05-14-kb-llamaindex-conf-deck-design.md`.

## Build

```bash
# PDF
npx -y @marp-team/marp-cli kb-llamaindex-conf-A.md -o A.pdf
npx -y @marp-team/marp-cli kb-llamaindex-conf-D.md -o D.pdf

# PPTX
npx -y @marp-team/marp-cli kb-llamaindex-conf-A.md --pptx -o A.pptx
npx -y @marp-team/marp-cli kb-llamaindex-conf-D.md --pptx -o D.pptx

# HTML preview with auto-reload
npx -y @marp-team/marp-cli -w kb-llamaindex-conf-A.md
```

Output files (`*.pdf`, `*.pptx`) are gitignored.
