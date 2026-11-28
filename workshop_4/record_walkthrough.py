"""Screen-recorded walkthrough of both dashboards for the Workshop 4 evidence (no audio, on-screen captions).

Start the dashboards first (docker compose up -d dashboard-governance dashboard-storytelling), then run from the
repository root:  python workshop_4/record_walkthrough.py
Writes workshop_4/evidence/09_dashboards_walkthrough.webm. Needs Playwright (pip install playwright, then
python -m playwright install ffmpeg) and Edge, Chrome or the Playwright Chromium.
"""
from pathlib import Path

from playwright.sync_api import sync_playwright

EVIDENCE = Path(__file__).parent / "evidence"
OUTPUT = EVIDENCE / "09_dashboards_walkthrough.webm"
SIZE = {"width": 1600, "height": 900}

CAPTION_JS = """text => {
    let box = document.getElementById('walkthrough-caption');
    if (!box) {
        box = document.createElement('div');
        box.id = 'walkthrough-caption';
        Object.assign(box.style, {position: 'fixed', left: '50%', bottom: '28px', transform: 'translateX(-50%)',
            maxWidth: '1100px', padding: '12px 22px', borderRadius: '8px', background: 'rgba(20, 20, 20, 0.85)',
            color: '#fff', font: '20px/1.4 system-ui, sans-serif', textAlign: 'center', zIndex: 99999,
            pointerEvents: 'none'});
        document.body.appendChild(box);
    }
    box.textContent = text;
}"""


def launch(playwright):
    for channel in ["msedge", "chrome", None]:
        try:
            return playwright.chromium.launch(channel=channel) if channel else playwright.chromium.launch()
        except Exception:  # browser not installed, try the next one
            continue
    raise RuntimeError("No Edge, Chrome or Playwright Chromium found (python -m playwright install chromium)")


class Tour:
    def __init__(self, page):
        self.page = page

    def caption(self, text, seconds=3.5):
        self.page.evaluate(CAPTION_JS, text)
        self.page.wait_for_timeout(int(seconds * 1000))

    def open(self, url):
        self.page.goto(url, wait_until="networkidle")
        self.page.wait_for_selector(".js-plotly-plot .main-svg", timeout=60000)
        self.page.wait_for_timeout(4000)  # let every callback finish drawing

    def scroll_to(self, selector, block="start"):
        self.page.locator(selector).first.evaluate(f"e => e.scrollIntoView({{behavior: 'smooth', block: '{block}'}})")
        self.page.wait_for_timeout(1500)

    def hover_across(self, chart_id, steps=6):
        """Move the mouse over a chart from left to right so its tooltips show."""
        plot_area = self.page.locator(f"#{chart_id} .nsewdrag")  # cartesian charts; maps and donuts have none
        target = plot_area if plot_area.count() else self.page.locator(f"#{chart_id} .main-svg")
        box = target.first.bounding_box()
        if not box:
            return
        y = box["y"] + box["height"] * 0.5
        for i in range(1, steps + 1):
            self.page.mouse.move(box["x"] + box["width"] * i / (steps + 1), y, steps=12)
            self.page.wait_for_timeout(700)
        self.page.mouse.move(5, 5)

    def pick(self, dropdown_id, downs=1):
        """Open a dropdown and choose the option `downs` places below the current one."""
        self.page.click(f"#{dropdown_id}")
        self.page.wait_for_timeout(800)
        for _ in range(downs):
            self.page.keyboard.press("ArrowDown")
            self.page.wait_for_timeout(300)
        self.page.keyboard.press("Enter")
        self.page.wait_for_timeout(2500)

    def click_label(self, text):
        self.page.get_by_text(text, exact=True).first.click()
        self.page.wait_for_timeout(2500)


def governance(tour):
    tour.open("http://localhost:8050")
    tour.caption("Governance dashboard (port 8050): quality, volume and freshness of the pipeline, read from Gold")
    tour.caption("KPI cards: total records, null rate, duplicate rate and schema compliance, each with its threshold")
    tour.scroll_to("#null-chart", block="center")
    tour.caption("Completeness and validity: null rate per field and outlier checks", 2.5)
    tour.hover_across("null-chart", steps=4)
    tour.pick("null-topic", downs=1)
    tour.caption("The topic filter narrows the null-rate chart to one source", 2.5)
    tour.click_label("Show fields at 0 %")
    tour.caption("The switch also shows the fields that have no nulls", 2.5)
    tour.scroll_to("#volume-chart", block="center")
    tour.caption("Volume and timeliness: records per ingestion day and time since the last run", 2.5)
    tour.hover_across("volume-chart")
    tour.click_label("Linear scale")
    tour.caption("Linear scale: the big historical load hides the small daily runs ...", 2.5)
    tour.click_label("Log scale")
    tour.caption("... so the log scale is the default", 2)
    tour.scroll_to("#completeness-chart", block="center")
    tour.caption("Station completeness per year: days present and nulls of TMAX, TMIN and PRCP", 2.5)
    tour.pick("completeness-metric", downs=1)
    tour.caption("The metric selector changes the heatmap", 2.5)
    tour.scroll_to("#kpi-table", block="center")
    tour.caption("Data quality summary: every KPI with its value and threshold, worst first", 4)


def storytelling(tour):
    tour.open("http://localhost:8051")
    tour.caption("Storytelling dashboard (port 8051): how unusual is the weather in Texas, and is anyone warned?")
    tour.caption("Step 1: key findings in plain words and a map of the stations with today's label", 3)
    tour.hover_across("map-chart", steps=5)
    tour.scroll_to("#station-cards", block="center")
    tour.caption("One card per station with its latest observation and label", 3)
    tour.scroll_to("#donut-chart", block="center")
    tour.caption("Step 2: share of station-days above, within and below the 1991-2020 usual range, and the trend", 3)
    tour.hover_across("trend-chart")
    tour.pick("story-station", downs=1)
    tour.caption("The station and measure filters apply to both charts of this step", 2.5)
    tour.pick("story-element", downs=1)
    tour.scroll_to("#band-chart", block="center")
    tour.caption("Step 3: one station day by day against its usual band; zoom with the buttons or by dragging", 3)
    tour.hover_across("band-chart")
    tour.pick("band-station", downs=1)
    tour.pick("band-element", downs=2)
    tour.caption("Here: another station and its precipitation", 2.5)
    tour.scroll_to("#alert-volume-chart", block="center")
    tour.caption("Step 4: official NWS alerts for Texas, their volume and their severity", 3)
    tour.hover_across("severity-chart", steps=4)
    tour.scroll_to("#keyword-chart", block="center")
    tour.caption("Keywords of the alert texts; the colour shows how serious the alerts using each word are", 3)
    tour.click_label("Two-word phrases in NWS alert texts")
    tour.click_label("Words in NOAA storm reports (2021-2025)")
    tour.caption("The same view for the NOAA storm reports", 2.5)
    tour.scroll_to("#storms-chart", block="center")
    tour.caption("Step 5: past EXTREME days next to NOAA storm reports of the same county", 3.5)
    tour.scroll_to("#source-severity-chart", block="center")
    tour.caption("Step 6: where the data come from, on a common Low / Medium / High scale for API and scraped sources",
                 4.5)
    tour.caption("End of the walkthrough. Both dashboards read only the Gold Parquet files and refresh after each "
                 "Gold DAG run", 4)


def main():
    EVIDENCE.mkdir(exist_ok=True)
    with sync_playwright() as playwright:
        browser = launch(playwright)
        context = browser.new_context(viewport=SIZE, record_video_dir=str(EVIDENCE / "_video_tmp"),
                                      record_video_size=SIZE)
        page = context.new_page()
        tour = Tour(page)
        governance(tour)
        storytelling(tour)
        context.close()  # the video is written when the context closes
        page.video.save_as(str(OUTPUT))
        page.video.delete()
        (EVIDENCE / "_video_tmp").rmdir()
        browser.close()
    print("written", OUTPUT)


if __name__ == "__main__":
    main()
