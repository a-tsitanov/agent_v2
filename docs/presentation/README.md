# kb-llamaindex — Conference Decks

Two Marp Markdown decks for the project conference talks.

- `kb-llamaindex-conf-A.md` — Tech / ML conference (22 slides, ~35 min).
- `kb-llamaindex-conf-D.md` — Internal project defense (23 slides, ~35 min, w/ speaker notes).

Design spec: [2026-05-14-kb-llamaindex-conf-deck-design.md](../superpowers/specs/2026-05-14-kb-llamaindex-conf-deck-design.md).

## Prerequisites

Marp uses Puppeteer to render PDF/PPTX, which needs Chrome, Edge or Firefox.

- **System install (recommended):** install Chrome from <https://www.google.com/chrome/>. No further setup.
- **Puppeteer-managed Chrome:** if no system browser, run once:

  ```bash
  npx -y puppeteer browsers install chrome
  ```

  Then export `CHROME_PATH` to its location before every build, e.g. on macOS arm64:

  ```bash
  export CHROME_PATH="$HOME/.cache/puppeteer/chrome/mac_arm-148.0.7778.97/chrome-mac-arm64/Google Chrome for Testing.app/Contents/MacOS/Google Chrome for Testing"
  ```

  (The exact version in the path may differ on your machine — adjust accordingly.)

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
