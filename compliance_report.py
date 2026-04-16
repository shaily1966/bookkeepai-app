"""
BookKeep AI Pro — PDF Compliance Report Generator
==================================================
Professional PDF a bookkeeper emails to their client after processing.

Integration into app.py results section:
    from compliance_report import generate_compliance_pdf, render_pdf_download_button
    render_pdf_download_button(compliance_result, txns, business_name, period, industry, province, st)
"""

import io
from datetime import datetime
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.units import inch
from reportlab.lib import colors
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, HRFlowable
)

# Colour palette
DARK_BLUE  = colors.HexColor("#1A365D")
MID_BLUE   = colors.HexColor("#2B6CB0")
LIGHT_BLUE = colors.HexColor("#EBF8FF")
LIGHT_GRAY = colors.HexColor("#F7FAFC")
MID_GRAY   = colors.HexColor("#718096")
BORDER     = colors.HexColor("#E2E8F0")
RED        = colors.HexColor("#C53030")
ORANGE     = colors.HexColor("#C05621")
GREEN      = colors.HexColor("#276749")
WHITE      = colors.white
BLACK      = colors.black
RED_BG     = colors.HexColor("#FFF5F5")
ORANGE_BG  = colors.HexColor("#FFFAF0")
GREEN_BG   = colors.HexColor("#F0FFF4")

PAGE_W = 7.0 * inch  # usable width


def _p(text, size=10, bold=False, color=BLACK, align="LEFT",
       leading=None, after=4, indent=0):
    fn = "Helvetica-Bold" if bold else "Helvetica"
    al = {"LEFT": 0, "CENTER": 1, "RIGHT": 2}.get(align, 0)
    return Paragraph(str(text), ParagraphStyle("_",
        fontName=fn, fontSize=size, textColor=color,
        alignment=al, leading=leading or size * 1.45,
        spaceAfter=after, leftIndent=indent))


def _score_color(s):
    return GREEN if s >= 85 else (ORANGE if s >= 70 else RED)

def _grade(s):
    if s >= 90: return "A — CRA-Ready"
    if s >= 80: return "B — Minor Issues"
    if s >= 70: return "C — Needs Attention"
    if s >= 55: return "D — Filing Risk"
    return "F — Do Not File"


def generate_compliance_pdf(
    compliance_result, transactions,
    business_name="Client", period="", industry="", province="ON",
    bookkeeper_name="", bookkeeper_firm="", bookkeeper_email="",
):
    """Returns PDF bytes ready for st.download_button()."""
    buf  = io.BytesIO()
    doc  = SimpleDocTemplate(buf, pagesize=letter,
        leftMargin=0.75*inch, rightMargin=0.75*inch,
        topMargin=0.75*inch, bottomMargin=0.75*inch)

    score   = compliance_result.get("score", 0)
    pillars = compliance_result.get("pillar_scores", {})
    actions = compliance_result.get("action_items", [])
    today   = datetime.now().strftime("%B %d, %Y")
    sc      = _score_color(score)
    story   = []

    PILLAR_LABELS = {
        "categorization": "Categorization",
        "cra_regulatory": "CRA Regulatory",
        "itc_accuracy":   "HST/ITC Accuracy",
        "reconciliation": "Reconciliation",
        "documentation":  "Documentation",
    }

    # ── HEADER ──────────────────────────────────────────────────────
    hdr_data = [[
        _p("BookKeep AI Pro", bold=True, color=MID_BLUE, size=11),
        _p(f"{bookkeeper_firm}  |  {bookkeeper_name}  |  {bookkeeper_email}"
           if bookkeeper_firm else "",
           size=8, color=MID_GRAY, align="RIGHT"),
    ]]
    hdr = Table(hdr_data, colWidths=[PAGE_W * 0.5, PAGE_W * 0.5])
    hdr.setStyle(TableStyle([
        ("VALIGN",        (0,0), (-1,-1), "MIDDLE"),
        ("BOTTOMPADDING", (0,0), (-1,-1), 8),
        ("LINEBELOW",     (0,0), (-1,-1), 0.5, BORDER),
    ]))
    story += [hdr, Spacer(1, 12)]

    # ── TITLE ───────────────────────────────────────────────────────
    story += [
        _p("CRA Compliance Report", bold=True, size=22, color=DARK_BLUE, after=4),
        _p(f"<b>{business_name}</b>  |  {period}  |  {industry}  |  "
           f"{province}  |  {today}", size=10, color=MID_GRAY, after=14),
    ]

    # ── SCORE BANNER (single wide row) ──────────────────────────────
    score_data = [[
        _p(str(score), bold=True, size=52, color=sc, align="CENTER", leading=56),
        _p(f"out of 100\n{_grade(score)}", bold=True, size=13,
           color=sc, leading=20, after=0),
    ]]
    score_tbl = Table(score_data, colWidths=[PAGE_W * 0.25, PAGE_W * 0.75])
    score_tbl.setStyle(TableStyle([
        ("BACKGROUND",    (0,0), (-1,-1),
         GREEN_BG if score >= 85 else (ORANGE_BG if score >= 70 else RED_BG)),
        ("TOPPADDING",    (0,0), (-1,-1), 14),
        ("BOTTOMPADDING", (0,0), (-1,-1), 14),
        ("LEFTPADDING",   (0,0), (-1,-1), 16),
        ("RIGHTPADDING",  (0,0), (-1,-1), 16),
        ("VALIGN",        (0,0), (-1,-1), "MIDDLE"),
    ]))
    story += [score_tbl, Spacer(1, 8)]

    # ── PILLAR BREAKDOWN (simple table) ─────────────────────────────
    pillar_rows = [[
        _p("Pillar",  bold=True, size=9, color=WHITE),
        _p("Score",   bold=True, size=9, color=WHITE, align="CENTER"),
        _p("Percent", bold=True, size=9, color=WHITE, align="RIGHT"),
    ]]
    for key, label in PILLAR_LABELS.items():
        if key in pillars:
            d   = pillars[key]
            pct = round(d["score"] / d["max"] * 100)
            pillar_rows.append([
                _p(label, size=9),
                _p(f"{d['score']}/{d['max']}", size=9, align="CENTER"),
                _p(f"{pct}%", size=9, bold=True,
                   color=_score_color(pct), align="RIGHT"),
            ])
    ptbl = Table(pillar_rows,
        colWidths=[PAGE_W * 0.60, PAGE_W * 0.20, PAGE_W * 0.20])
    ptbl.setStyle(TableStyle([
        ("BACKGROUND",    (0,0), (-1,0), DARK_BLUE),
        ("ROWBACKGROUNDS",(0,1), (-1,-1), [LIGHT_GRAY, WHITE]),
        ("TOPPADDING",    (0,0), (-1,-1), 5),
        ("BOTTOMPADDING", (0,0), (-1,-1), 5),
        ("LEFTPADDING",   (0,0), (-1,-1), 10),
        ("RIGHTPADDING",  (0,0), (-1,-1), 10),
        ("LINEBELOW",     (0,0), (-1,-1), 0.3, BORDER),
        ("VALIGN",        (0,0), (-1,-1), "MIDDLE"),
    ]))
    story += [ptbl, Spacer(1, 18)]

    # ── ACTION ITEMS ─────────────────────────────────────────────────
    story.append(_p("Action Items Before Filing", bold=True,
                    size=14, color=DARK_BLUE, after=8))

    criticals = [a for a in actions if a.get("severity") == "critical"]
    warnings  = [a for a in actions if a.get("severity") == "warning"]

    if not actions:
        t = Table([[_p(
            "No action items — these books are CRA-ready. "
            "Proceed with T2125 and GST34 filing.", size=10, color=GREEN)]],
            colWidths=[PAGE_W])
        t.setStyle(TableStyle([
            ("BACKGROUND",    (0,0), (-1,-1), GREEN_BG),
            ("TOPPADDING",    (0,0), (-1,-1), 10),
            ("BOTTOMPADDING", (0,0), (-1,-1), 10),
            ("LEFTPADDING",   (0,0), (-1,-1), 14),
        ]))
        story += [t, Spacer(1, 14)]
    else:
        def _action_table(item, bg, line_color, label):
            t = Table([[
                _p(label, bold=True, size=11,
                   color=RED if label == "!" else ORANGE, align="CENTER"),
                _p(f"<b>{item['title']}</b><br/>"
                   f"<font size='8'>{item.get('action','')}</font>",
                   size=9, leading=14, after=0),
            ]], colWidths=[0.32*inch, PAGE_W - 0.34*inch])
            t.setStyle(TableStyle([
                ("BACKGROUND",    (0,0), (-1,-1), bg),
                ("TOPPADDING",    (0,0), (-1,-1), 8),
                ("BOTTOMPADDING", (0,0), (-1,-1), 8),
                ("LEFTPADDING",   (0,0), (-1,-1), 8),
                ("RIGHTPADDING",  (0,0), (-1,-1), 8),
                ("VALIGN",        (0,0), (-1,-1), "TOP"),
                ("LINEBELOW",     (0,0), (-1,-1), 0.5, line_color),
            ]))
            return t

        if criticals:
            story.append(_p(f"Critical — Must Resolve ({len(criticals)})",
                            bold=True, size=11, color=RED, after=4))
            for a in criticals:
                story += [_action_table(a, RED_BG,
                    colors.HexColor("#FC8181"), "!"), Spacer(1, 4)]

        if warnings:
            story += [Spacer(1, 6),
                      _p(f"Warnings ({len(warnings)})",
                         bold=True, size=11, color=ORANGE, after=4)]
            for a in warnings:
                story += [_action_table(a, ORANGE_BG,
                    colors.HexColor("#F6E05E"), "~"), Spacer(1, 4)]

    story.append(Spacer(1, 14))

    # ── EXPENSE SUMMARY ──────────────────────────────────────────────
    story.append(_p("Expense Summary", bold=True, size=14,
                    color=DARK_BLUE, after=8))

    cat_totals = {}
    for t in transactions:
        cat = t.get("category", "")
        if not cat or t.get("type") in ("PAYMENT", "FEE_REBATE"):
            continue
        if t.get("debit", 0) > 0:
            cat_totals.setdefault(cat, {"amount": 0.0, "itc": 0.0, "count": 0})
            cat_totals[cat]["amount"] += t.get("debit", 0)
            cat_totals[cat]["itc"]    += t.get("itc_amount", 0)
            cat_totals[cat]["count"]  += 1

    if cat_totals:
        total_exp = sum(v["amount"] for v in cat_totals.values())
        total_itc = sum(v["itc"]    for v in cat_totals.values())

        exp_rows = [[
            _p("Category", bold=True, size=9, color=WHITE),
            _p("Txns",     bold=True, size=9, color=WHITE, align="CENTER"),
            _p("Amount",   bold=True, size=9, color=WHITE, align="RIGHT"),
            _p("ITC",      bold=True, size=9, color=WHITE, align="RIGHT"),
        ]]
        for cat, v in sorted(cat_totals.items(), key=lambda x: -x[1]["amount"]):
            cc = RED if "Uncategorized" in cat else BLACK
            exp_rows.append([
                _p(cat, size=9, color=cc),
                _p(str(v["count"]), size=9, align="CENTER"),
                _p(f"${v['amount']:,.2f}", size=9, align="RIGHT"),
                _p(f"${v['itc']:,.2f}" if v["itc"] else "-",
                   size=9, color=GREEN if v["itc"] else MID_GRAY,
                   align="RIGHT"),
            ])
        exp_rows.append([
            _p("TOTAL", bold=True, size=9, color=DARK_BLUE),
            _p(str(sum(v["count"] for v in cat_totals.values())),
               bold=True, size=9, align="CENTER"),
            _p(f"${total_exp:,.2f}", bold=True, size=9,
               color=DARK_BLUE, align="RIGHT"),
            _p(f"${total_itc:,.2f}", bold=True, size=9,
               color=GREEN, align="RIGHT"),
        ])

        etbl = Table(exp_rows,
            colWidths=[PAGE_W*0.50, PAGE_W*0.14,
                       PAGE_W*0.19, PAGE_W*0.17])
        etbl.setStyle(TableStyle([
            ("BACKGROUND",    (0,0), (-1,0),  DARK_BLUE),
            ("ROWBACKGROUNDS",(0,1), (-1,-2), [LIGHT_GRAY, WHITE]),
            ("BACKGROUND",    (0,-1),(-1,-1), LIGHT_BLUE),
            ("TOPPADDING",    (0,0), (-1,-1), 5),
            ("BOTTOMPADDING", (0,0), (-1,-1), 5),
            ("LEFTPADDING",   (0,0), (-1,-1), 8),
            ("RIGHTPADDING",  (0,0), (-1,-1), 8),
            ("LINEBELOW",     (0,0), (-1,-1), 0.3, BORDER),
            ("LINEABOVE",     (0,-1),(-1,-1), 1.0, MID_BLUE),
            ("VALIGN",        (0,0), (-1,-1), "MIDDLE"),
        ]))
        story += [etbl, Spacer(1, 18)]

    # ── PRE-FILING CHECKLIST ─────────────────────────────────────────
    story.append(_p("Pre-Filing Checklist", bold=True, size=14,
                    color=DARK_BLUE, after=8))

    checklist = [
        ("T2125 Business Income", [
            "All transactions categorized — 0 Uncategorized",
            "CCA assets on Fixed Assets schedule, NOT expensed directly",
            "Meals & Entertainment reduced by 50%",
            "Personal expenses removed from deductions",
        ]),
        ("GST34 HST/GST Return", [
            "Total ITC reviewed and approved",
            "ITC = $0 on insurance and bank charges",
            "Province-correct tax rates verified",
        ]),
        ("Record Keeping — CRA requires 6 years", [
            "Original bank statement PDFs saved",
            "Receipts for all expenses over $30",
            "This report and Excel file archived",
        ]),
    ]
    for section, items in checklist:
        story.append(_p(section, bold=True, size=10, color=DARK_BLUE, after=4))
        ck_rows = [[_p("[ ]", size=9, color=MID_GRAY), _p(item, size=9)]
                   for item in items]
        ctbl = Table(ck_rows, colWidths=[0.28*inch, PAGE_W - 0.28*inch])
        ctbl.setStyle(TableStyle([
            ("VALIGN",        (0,0), (-1,-1), "TOP"),
            ("TOPPADDING",    (0,0), (-1,-1), 3),
            ("BOTTOMPADDING", (0,0), (-1,-1), 3),
        ]))
        story += [ctbl, Spacer(1, 8)]

    # ── FOOTER ───────────────────────────────────────────────────────
    story += [Spacer(1, 10),
              HRFlowable(width=PAGE_W, thickness=0.5, color=BORDER),
              Spacer(1, 6)]
    footer = (f"Generated by BookKeep AI Pro on {today}. "
              "For informational purposes only — not tax or legal advice. "
              "Always have a licensed Canadian CPA review before filing with the CRA.")
    if bookkeeper_name:
        footer += f"  Prepared by {bookkeeper_name}"
        if bookkeeper_firm:  footer += f", {bookkeeper_firm}"
        if bookkeeper_email: footer += f" — {bookkeeper_email}"
        footer += "."
    story.append(_p(footer, size=8, color=MID_GRAY, align="CENTER", after=0))

    doc.build(story)
    buf.seek(0)
    return buf.read()


# ── STREAMLIT INTEGRATION ────────────────────────────────────────────

def render_pdf_download_button(
    compliance_result, transactions, business_name,
    period, industry, province, st
):
    """Add to app.py after render_compliance_report()."""
    with st.expander("📄 Download Client PDF Report", expanded=False):
        st.caption(
            "Professional PDF compliance report to email to your client. "
            "Add your name and firm for branded output."
        )
        c1, c2, c3 = st.columns(3)
        bk_name  = c1.text_input("Your Name",  key="pdf_bk_name",
                                  placeholder="Jane Smith CPA")
        bk_firm  = c2.text_input("Your Firm",  key="pdf_bk_firm",
                                  placeholder="Smith Accounting")
        bk_email = c3.text_input("Your Email", key="pdf_bk_email",
                                  placeholder="jane@smithaccounting.ca")

        if st.button("Generate PDF Report", key="gen_pdf",
                     type="primary", use_container_width=True):
            with st.spinner("Generating PDF..."):
                try:
                    pdf_bytes = generate_compliance_pdf(
                        compliance_result=compliance_result,
                        transactions=transactions,
                        business_name=business_name,
                        period=period, industry=industry, province=province,
                        bookkeeper_name=bk_name,
                        bookkeeper_firm=bk_firm,
                        bookkeeper_email=bk_email,
                    )
                    safe  = "".join(c for c in business_name
                                    if c.isalnum() or c in " -_").strip()
                    fname = f"{safe}_CRA_Report_{period.replace(' ','_')}.pdf"
                    st.download_button(
                        "⬇️ Download PDF Report",
                        data=pdf_bytes, file_name=fname,
                        mime="application/pdf", type="primary",
                        use_container_width=True, key="dl_pdf_final",
                    )
                except Exception as e:
                    st.error(f"PDF generation failed: {e}")
