"""Convert Workshop4_Report.md to workshop_4/Workshop4_Report.pdf.

The Markdown is converted to HTML and printed to PDF with Microsoft Edge (installed with Windows), or Chrome /
Chromium when Edge is not there. A screenshot that does not exist yet is drawn as a dashed "pending" box, so
a draft can be built before the evidence is captured.
Needs two libraries that are not part of the pipeline:  pip install markdown playwright
Run from the repository root:  python workshop_4/report/build_pdf.py
"""
import re
from pathlib import Path

import markdown
from playwright.sync_api import sync_playwright

HERE = Path(__file__).parent
SOURCE = HERE / "Workshop4_Report.md"
OUTPUT = HERE.parent / "Workshop4_Report.pdf"

CSS = """
body { font-family: Arial, Helvetica, sans-serif; font-size: 10pt; line-height: 1.38; color: #111; }
h1 { font-size: 18pt; color: #14285a; margin-bottom: 6px; }
h2 { font-size: 13pt; color: #14285a; margin-top: 18px; border-bottom: 1px solid #bbb; padding-bottom: 2px; }
h3 { font-size: 11pt; color: #14285a; margin-top: 12px; }
table { border-collapse: collapse; width: 100%; margin: 8px 0; font-size: 8.6pt; }
th, td { border: 1px solid #999; padding: 3px 5px; text-align: left; vertical-align: top; }
th { background: #e8edf5; }
tr { break-inside: avoid; }
code { font-family: Consolas, monospace; font-size: 8.6pt; background: #f2f2f2; padding: 0 2px; }
pre { background: #f2f2f2; padding: 6px 8px; font-size: 7.6pt; white-space: pre-wrap; word-break: break-all;
      break-inside: avoid; }
pre code { background: none; padding: 0; font-size: 7.6pt; }
blockquote { border-left: 4px solid #1a9e77; margin: 8px 0; padding: 2px 10px; background: #f4faf8; }
img { max-width: 100%; max-height: 560px; display: block; margin: 8px auto 2px auto; border: 1px solid #ccc; }
.pending { border: 2px dashed #c98f00; color: #7a4f00; padding: 28px; text-align: center; margin: 8px 0 2px 0; }
h2, h3 { break-after: avoid; }
p:has(> img), p:has(> .pending) { break-inside: avoid; break-after: avoid; }
p > em { font-size: 8.8pt; color: #333; }
"""
FOOTER = ('<div style="font-size:8px; width:100%; text-align:center; color:#555;">'
          'Team 5, Workshop 4 report. Page <span class="pageNumber"></span> of <span class="totalPages"></span></div>')


def mark_missing_images(text):
    """Replace ![alt](path) of a file that does not exist with a visible placeholder."""
    def replace(match):
        alt, path = match.group(1), match.group(2)
        if (HERE / path).exists():
            return match.group(0)
        return f'<div class="pending">Screenshot pending: {alt} ({Path(path).name})</div>'
    return re.sub(r"!\[([^\]]*)\]\(([^)]+)\)", replace, text)


def launch(playwright):
    for channel in ["msedge", "chrome", None]:
        try:
            return playwright.chromium.launch(channel=channel) if channel else playwright.chromium.launch()
        except Exception:  # browser not installed, try the next one
            continue
    raise RuntimeError("No Edge, Chrome or Playwright Chromium found (python -m playwright install chromium)")


def main():
    text = mark_missing_images(SOURCE.read_text(encoding="utf-8"))
    body = markdown.markdown(text, extensions=["tables", "fenced_code"])
    html = f"<!doctype html><html><head><meta charset='utf-8'><style>{CSS}</style></head><body>{body}</body></html>"
    page_file = HERE / "_report.html"     # next to the Markdown file, so relative image paths work
    page_file.write_text(html, encoding="utf-8")
    try:
        with sync_playwright() as playwright:
            browser = launch(playwright)
            page = browser.new_page()
            page.goto(page_file.as_uri(), wait_until="networkidle")
            page.pdf(path=str(OUTPUT), format="A4", print_background=True, display_header_footer=True,
                     header_template="<span></span>", footer_template=FOOTER,
                     margin={"top": "16mm", "bottom": "18mm", "left": "16mm", "right": "16mm"})
            browser.close()
    finally:
        page_file.unlink()
    print("written", OUTPUT)


if __name__ == "__main__":
    main()
