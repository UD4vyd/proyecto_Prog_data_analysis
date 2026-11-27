"""Full-page screenshots of both dashboards for the Workshop 4 evidence (1920 x 1080 viewport).

Start the dashboards first (docker compose up -d, or poetry run python dashboard/<app>.py), then run from the
repository root:  python workshop_4/capture_screenshots.py
Writes workshop_4/evidence/06_governance_full.png and 07_storytelling_full.png. Uses Edge, Chrome or the
Playwright Chromium, whichever is installed (pip install playwright).
"""
from pathlib import Path

from playwright.sync_api import sync_playwright

EVIDENCE = Path(__file__).parent / "evidence"
PAGES = {"06_governance_full.png": "http://localhost:8050", "07_storytelling_full.png": "http://localhost:8051"}


def launch(playwright):
    for channel in ["msedge", "chrome", None]:
        try:
            return playwright.chromium.launch(channel=channel) if channel else playwright.chromium.launch()
        except Exception:  # browser not installed, try the next one
            continue
    raise RuntimeError("No Edge, Chrome or Playwright Chromium found (python -m playwright install chromium)")


def main():
    EVIDENCE.mkdir(exist_ok=True)
    with sync_playwright() as playwright:
        browser = launch(playwright)
        page = browser.new_page(viewport={"width": 1920, "height": 1080})
        for name, url in PAGES.items():
            page.goto(url, wait_until="networkidle")
            page.wait_for_selector(".js-plotly-plot .main-svg", timeout=60000)
            page.wait_for_timeout(4000)  # let every callback finish drawing
            page.screenshot(path=str(EVIDENCE / name), full_page=True)
            print("written", EVIDENCE / name)
        browser.close()


if __name__ == "__main__":
    main()
