"""Convert Workshop1_Report.md to docs/Workshop1_Report.pdf (pandoc is not installed on our machines)."""
import re
from pathlib import Path

import markdown
from fpdf import FPDF, FontFace

HERE = Path(__file__).parent
SRC = HERE / "Workshop1_Report.md"
OUT = HERE.parent / "Workshop1_Report.pdf"
FONTS = Path("/usr/share/fonts/truetype")


class Report(FPDF):
    def footer(self):
        self.set_y(-12)
        self.set_font("Sans", size=8)
        self.cell(0, 8, f"Team 5, Workshop 1 report. Page {self.page_no()}", align="C")


def set_widths(match):
    """Give each column a width proportional to its longest cell (capped), so short columns stay narrow."""
    table = match.group(0)
    rows = re.findall(r"<tr>(.*?)</tr>", table, flags=re.S)
    cells = [re.findall(r"<t[hd](?:\s[^>]*)?>(.*?)</t[hd]>", r, flags=re.S) for r in rows]
    n = len(cells[0])
    longest = [max(len(re.sub(r"<[^>]+>", "", row[i])) for row in cells if i < len(row)) for i in range(n)]
    weights = [min(max(length, 12), 45) for length in longest]
    widths = [round(100 * w / sum(weights)) for w in weights]
    heads = iter(widths)
    return re.sub(r"<th(\s[^>]*)?>", lambda m: f'<th width="{next(heads)}%">', table, count=n)


def main():
    html = markdown.markdown(SRC.read_text(encoding="utf-8"), extensions=["tables", "fenced_code"])
    html = html.replace('src="figures/', f'src="{HERE / "figures"}/')
    html = html.replace("<img ", '<img width="560" ')
    # fpdf2 does not accept inline tags inside table cells, so we drop them there.
    html = re.sub(r"<table>.*?</table>", lambda m: re.sub(r"</?(code|strong|em)>", "", m.group(0)), html, flags=re.S)
    html = re.sub(r"<table>.*?</table>", set_widths, html, flags=re.S)
    html = html.replace("<table>", '<table border="1" cellpadding="2">')
    html = re.sub(r"<td( style=\"[^\"]*\")?>", '<td align="left">', html)

    pdf = Report(format="A4")
    pdf.set_margins(18, 15, 18)
    pdf.set_auto_page_break(True, margin=16)
    pdf.add_font("Sans", "", FONTS / "liberation/LiberationSans-Regular.ttf")
    pdf.add_font("Sans", "B", FONTS / "liberation/LiberationSans-Bold.ttf")
    pdf.add_font("Sans", "I", FONTS / "liberation/LiberationSans-Italic.ttf")
    pdf.add_font("Sans", "BI", FONTS / "liberation/LiberationSans-BoldItalic.ttf")
    pdf.add_font("Mono", "", FONTS / "dejavu/DejaVuSansMono.ttf")
    pdf.add_page()
    pdf.set_font("Sans", size=9.5)
    pdf.write_html(html, font_family="Sans",
                   tag_styles={"h1": FontFace(size_pt=17, emphasis="BOLD", color=(20, 40, 90)),
                               "h2": FontFace(size_pt=12.5, emphasis="BOLD", color=(20, 40, 90)),
                               "code": FontFace(family="Mono", size_pt=7.5),
                               "pre": FontFace(family="Mono", size_pt=7.5)},
                   table_line_separators=True)
    pdf.output(str(OUT))
    print("pages:", pdf.page_no(), "->", OUT)


if __name__ == "__main__":
    main()
