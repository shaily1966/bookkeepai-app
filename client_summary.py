"""
BookKeep AI Pro — Client Summary & GST34 Guide Generator
=========================================================
Two outputs designed for the business owner, not the accountant.

1. Plain English Summary  — bookkeeper emails/texts this to their client.
   No jargon. No Excel. Business owner reads it in 2 minutes and
   knows exactly what receipts to send and what they owe CRA.

2. GST34 Field Guide — shows exactly which number goes in which box
   on the CRA GST34 return. Bookkeeper prints or emails this.
   Client opens CRA My Business Account and fills in the boxes.

Integration into app.py — add after render_compliance_report():
──────────────────────────────────────────────────────────────
from client_summary import render_client_summary_section
render_client_summary_section(txns, business_name, period, industry, province, st)
"""

from datetime import datetime


# ═══════════════════════════════════════════════════════════════════
# HELPER — build category totals from transactions
# ═══════════════════════════════════════════════════════════════════

def _build_totals(transactions):
    """Aggregate transactions into summary numbers."""
    cat_totals   = {}
    total_exp    = 0.0
    total_itc    = 0.0
    total_income = 0.0
    payments     = 0.0
    needs_review = []
    meals_total  = 0.0
    personal_total = 0.0

    for t in transactions:
        txn_type = t.get("type", "")
        debit    = t.get("debit", 0) or 0
        credit   = t.get("credit", 0) or 0
        cat      = t.get("category", "")
        itc      = t.get("itc_amount", 0) or 0
        conf     = t.get("confidence", "95")
        desc     = t.get("description", "")
        date     = t.get("date", "")

        if txn_type == "PAYMENT":
            payments += credit
            continue

        if txn_type == "FEE_REBATE":
            continue

        if debit > 0:
            total_exp += debit
            total_itc += itc

            if cat and "Uncategorized" not in cat and "Personal" not in cat:
                cat_totals[cat] = cat_totals.get(cat, 0) + debit

            if cat == "Meals & Entertainment":
                meals_total += debit

            if cat in ("Owner Draw / Personal", "Shareholder Loan (Debit)"):
                personal_total += debit

            # Flag for needs review
            if ("Uncategorized" in cat or
                (str(conf).isdigit() and int(conf) < 70)):
                needs_review.append({
                    "date":   date,
                    "desc":   desc,
                    "amount": debit,
                    "reason": (
                        "Receipt needed — can't tell if business or personal"
                        if "Uncategorized" in cat
                        else "AI uncertain — please confirm this is a business expense"
                    )
                })

        if credit > 0 and txn_type not in ("PAYMENT", "REFUND", "FEE_REBATE"):
            total_income += credit

    return {
        "cat_totals":     cat_totals,
        "total_exp":      round(total_exp, 2),
        "total_itc":      round(total_itc, 2),
        "total_income":   round(total_income, 2),
        "payments":       round(payments, 2),
        "needs_review":   needs_review[:10],    # Cap at 10 for readability
        "meals_total":    round(meals_total, 2),
        "meals_deductible": round(meals_total * 0.5, 2),
        "personal_total": round(personal_total, 2),
        "txn_count":      len([t for t in transactions
                               if t.get("debit", 0) > 0]),
    }


# ═══════════════════════════════════════════════════════════════════
# 1. PLAIN ENGLISH CLIENT SUMMARY
# ═══════════════════════════════════════════════════════════════════

def generate_client_summary(
    transactions,
    business_name  = "Your Business",
    period         = "",
    industry       = "",
    province       = "ON",
    bookkeeper_name= "",
    compliance_score = None,
):
    """
    Generate a plain English summary the bookkeeper emails to their client.
    Returns a string — ready to copy and paste into an email.

    No jargon. No Excel. No accounting terms.
    Business owner reads this in 2 minutes.
    """
    d     = _build_totals(transactions)
    today = datetime.now().strftime("%B %d, %Y")

    # Province tax label
    hst_label = {
        "ON": "HST (13%)", "BC": "GST (5%)", "AB": "GST (5%)",
        "QC": "GST (5%)", "NS": "HST (15%)", "NB": "HST (15%)",
        "NL": "HST (15%)", "PE": "HST (15%)", "MB": "GST (5%)",
        "SK": "GST (5%)",
    }.get(province, "HST/GST")

    # Deductible business expenses (exclude personal)
    deductible = d["total_exp"] - d["personal_total"]

    lines = []

    # ── Subject line ─────────────────────────────────────────────
    lines.append(f"SUBJECT: Your Books for {period} — {business_name}")
    lines.append("")

    # ── Greeting ─────────────────────────────────────────────────
    lines.append(f"Hi,")
    lines.append("")
    lines.append(
        f"I've finished processing your bank statements for {period}. "
        f"Here's a plain English summary of where things stand."
    )
    lines.append("")

    # ── The numbers ──────────────────────────────────────────────
    lines.append("─" * 50)
    lines.append(f"YOUR {period.upper()} SUMMARY")
    lines.append("─" * 50)
    lines.append("")

    lines.append(f"Total business expenses:       ${deductible:>10,.2f}")
    if d["meals_total"] > 0:
        lines.append(
            f"  (includes meals — only 50%    ${d['meals_deductible']:>10,.2f}  is tax deductible)"
        )
    if d["personal_total"] > 0:
        lines.append(
            f"Personal expenses removed:     ${d['personal_total']:>10,.2f}  (not deductible)"
        )
    lines.append(f"{hst_label} you can claim back:   ${d['total_itc']:>10,.2f}")
    lines.append("")

    if d["total_income"] > 0:
        lines.append(f"Income recorded:               ${d['total_income']:>10,.2f}")
        lines.append("")

    # ── Top expense categories in plain English ───────────────────
    if d["cat_totals"]:
        lines.append("WHERE YOUR MONEY WENT:")
        lines.append("")
        sorted_cats = sorted(d["cat_totals"].items(), key=lambda x: -x[1])

        # Plain English category names
        plain_names = {
            "Motor Vehicle Expense":  "Vehicle & fuel costs",
            "Meals & Entertainment":  "Meals & client entertainment",
            "Office Supplies":        "Office & software expenses",
            "Utilities":              "Phone & internet",
            "Materials & Supplies":   "Materials & supplies",
            "Rent":                   "Rent & lease payments",
            "Insurance":              "Insurance premiums",
            "Advertising":            "Advertising & marketing",
            "Travel":                 "Travel expenses",
            "Professional Fees":      "Professional fees (legal/accounting)",
            "Subcontracts":           "Subcontractor payments",
            "Cost of Goods":          "Cost of goods sold",
            "Repairs & Maintenance":  "Repairs & maintenance",
            "Delivery & Shipping":    "Shipping & delivery",
            "Government Remittances": "Government remittances (WSIB/CPP/EI)",
            "Bank Charges":           "Bank fees & charges",
        }

        for cat, amount in sorted_cats[:8]:
            label = plain_names.get(cat, cat)
            lines.append(f"  {label:<38} ${amount:>10,.2f}")
        lines.append("")

    # ── Items needing client input ────────────────────────────────
    if d["needs_review"]:
        lines.append("─" * 50)
        lines.append(f"I NEED YOUR HELP WITH {len(d['needs_review'])} ITEM(S):")
        lines.append("─" * 50)
        lines.append("")
        lines.append(
            "I couldn't categorize these transactions from the bank "
            "description alone. Please reply with what each one was for:"
        )
        lines.append("")
        for i, item in enumerate(d["needs_review"], 1):
            lines.append(
                f"  {i}. {item['date']}  |  {item['desc'][:35]:<35}  "
                f"|  ${item['amount']:,.2f}"
            )
            lines.append(f"     → {item['reason']}")
            lines.append("")
    else:
        lines.append("✅ Everything was categorized — no questions from me this month.")
        lines.append("")

    # ── What happens next ─────────────────────────────────────────
    lines.append("─" * 50)
    lines.append("WHAT HAPPENS NEXT:")
    lines.append("─" * 50)
    lines.append("")

    if d["needs_review"]:
        lines.append(
            f"1. Reply to this email with answers to the {len(d['needs_review'])} "
            f"item(s) above."
        )
        lines.append(
            "2. Once I have those, I'll finalize your books and send your "
            "GST/HST filing summary."
        )
    else:
        lines.append(
            "1. Your books are complete for this period — no action needed from you."
        )
        lines.append(
            f"2. I'll use the ${d['total_itc']:,.2f} {hst_label} figure for "
            f"your next GST/HST return."
        )
    lines.append("")

    # ── Compliance note ───────────────────────────────────────────
    if compliance_score is not None:
        if compliance_score >= 85:
            lines.append(
                f"📊 Compliance Score: {compliance_score}/100 — "
                f"your books are in great shape."
            )
        elif compliance_score >= 70:
            lines.append(
                f"📊 Compliance Score: {compliance_score}/100 — "
                f"a few items to tidy up before filing."
            )
        else:
            lines.append(
                f"📊 Compliance Score: {compliance_score}/100 — "
                f"please reply to my questions above before we file anything."
            )
        lines.append("")

    # ── Sign off ─────────────────────────────────────────────────
    lines.append(
        f"Questions? Just reply to this email."
    )
    lines.append("")
    if bookkeeper_name:
        lines.append(bookkeeper_name)
    lines.append("")
    lines.append(
        "─" * 50
        + f"\nGenerated by BookKeep AI Pro | {today}"
    )

    return "\n".join(lines)


# ═══════════════════════════════════════════════════════════════════
# 2. GST34 FIELD GUIDE
# ═══════════════════════════════════════════════════════════════════

# CRA GST34 form boxes and what goes in them
# Source: CRA GST/HST Return Working Copy (form GST34-2)
GST34_FIELDS = [
    # (box_number, label, description, how_to_calculate)
    ("101", "Total Sales and Revenue",
     "Total revenue your business earned this period, including HST collected.",
     "Add up all deposits and payments received for services/goods sold."),

    ("103", "HST/GST Collected",
     "The HST/GST you collected FROM your customers on your sales.",
     "Multiply your taxable sales by the HST rate for your province."),

    ("106", "Input Tax Credits (ITC)",
     "The HST/GST you paid ON your business purchases — this is what you claim back.",
     "Use the ITC total from your BookKeep AI statement."),

    ("108", "Total ITC and Adjustments",
     "Same as Box 106 unless you have adjustments. Usually identical.",
     "Same as Box 106 for most small businesses."),

    ("109", "Net Tax",
     "What you OWE CRA (if positive) or CRA OWES YOU (if negative).",
     "Box 103 minus Box 106. If negative, you get a refund."),

    ("111", "Rebates",
     "Special rebates — most small businesses leave this blank.",
     "Leave blank unless your accountant tells you otherwise."),

    ("205", "GST/HST Due",
     "Final amount you pay CRA this period.",
     "Same as Box 109 if positive. Zero if Box 109 is negative."),

    ("405", "Refund",
     "Amount CRA owes you if your ITC exceeds the HST collected.",
     "Same as Box 109 (as positive number) if Box 109 was negative."),
]


def generate_gst34_guide(
    transactions,
    business_name   = "Your Business",
    period          = "",
    province        = "ON",
    filing_period   = "quarterly",
    total_sales     = None,   # Optional — if bookkeeper knows their client's revenue
    hst_collected   = None,   # Optional — if known
    bookkeeper_name = "",
):
    """
    Generate a GST34 filing guide showing exactly which number
    goes in which box on the CRA GST34 return.

    Returns a string — ready to print or email to client.

    total_sales and hst_collected are optional.
    If not provided, guide shows placeholders with instructions.
    """
    d     = _build_totals(transactions)
    today = datetime.now().strftime("%B %d, %Y")

    # Province-specific details
    province_info = {
        "ON": {"name": "Ontario",            "rate": "13%", "type": "HST"},
        "BC": {"name": "British Columbia",   "rate": "5%",  "type": "GST"},
        "AB": {"name": "Alberta",            "rate": "5%",  "type": "GST"},
        "QC": {"name": "Quebec",             "rate": "5%",  "type": "GST + QST"},
        "SK": {"name": "Saskatchewan",       "rate": "5%",  "type": "GST"},
        "MB": {"name": "Manitoba",           "rate": "5%",  "type": "GST"},
        "NS": {"name": "Nova Scotia",        "rate": "15%", "type": "HST"},
        "NB": {"name": "New Brunswick",      "rate": "15%", "type": "HST"},
        "NL": {"name": "Newfoundland",       "rate": "15%", "type": "HST"},
        "PE": {"name": "PEI",                "rate": "15%", "type": "HST"},
    }
    prov = province_info.get(province, {"name": province, "rate": "13%", "type": "HST"})

    # Calculate net tax
    itc = d["total_itc"]
    net_tax = None
    if hst_collected is not None:
        net_tax = round(hst_collected - itc, 2)

    lines = []

    # ── Header ───────────────────────────────────────────────────
    lines.append("=" * 55)
    lines.append("  CRA GST/HST RETURN — FILING GUIDE")
    lines.append(f"  Form GST34")
    lines.append("=" * 55)
    lines.append("")
    lines.append(f"  Business:       {business_name}")
    lines.append(f"  Period:         {period}")
    lines.append(f"  Province:       {prov['name']} ({prov['type']} {prov['rate']})")
    lines.append(f"  Filing type:    {filing_period.title()}")
    lines.append(f"  Prepared:       {today}")
    if bookkeeper_name:
        lines.append(f"  Prepared by:    {bookkeeper_name}")
    lines.append("")
    lines.append(
        "  HOW TO USE THIS GUIDE:"
        "\n  1. Log into CRA My Business Account"
        "\n  2. Go to: File a return → GST/HST Return (GST34)"
        "\n  3. Enter the numbers below into the matching boxes"
        "\n  4. Submit"
    )
    lines.append("")

    # ── The boxes ────────────────────────────────────────────────
    lines.append("=" * 55)
    lines.append("  WHAT TO ENTER IN EACH BOX")
    lines.append("=" * 55)
    lines.append("")

    # Box 101 — Sales
    lines.append(f"  BOX 101 — Total Sales and Revenue")
    if total_sales is not None:
        lines.append(f"  Enter:   ${total_sales:,.2f}")
    else:
        lines.append(f"  Enter:   [Your total revenue for {period}]")
        lines.append(f"  Tip:     Add up all payments received from clients/customers.")
    lines.append("")

    # Box 103 — HST Collected
    lines.append(f"  BOX 103 — {prov['type']} Collected on Sales")
    if hst_collected is not None:
        lines.append(f"  Enter:   ${hst_collected:,.2f}")
    else:
        lines.append(f"  Enter:   [The {prov['type']} you charged your customers]")
        lines.append(
            f"  Tip:     If you charged {prov['rate']} {prov['type']} on all sales,"
            f"\n           multiply Box 101 × {prov['rate'].replace('%', '')} / 100"
        )
    lines.append("")

    # Box 106 — ITC (THIS IS THE KEY NUMBER FROM BOOKKEEP AI)
    lines.append(f"  BOX 106 — Input Tax Credits (ITC)")
    lines.append(f"  *** THIS IS THE NUMBER FROM YOUR BOOKKEEP AI STATEMENT ***")
    lines.append(f"  Enter:   ${itc:,.2f}")
    lines.append(
        f"  This is the {prov['type']} you paid on your business expenses.\n"
        f"  Claiming this reduces what you owe CRA."
    )
    lines.append("")

    # Box 108 — Total ITC
    lines.append(f"  BOX 108 — Total ITC and Adjustments")
    lines.append(f"  Enter:   ${itc:,.2f}  (same as Box 106)")
    lines.append(f"  Most small businesses have no adjustments.")
    lines.append("")

    # Box 109 — Net Tax
    lines.append(f"  BOX 109 — Net Tax (what you owe or get back)")
    if net_tax is not None:
        if net_tax > 0:
            lines.append(f"  Enter:   ${net_tax:,.2f}")
            lines.append(f"  You OWE CRA this amount.")
        elif net_tax < 0:
            lines.append(f"  Enter:   $0.00")
            lines.append(
                f"  Your ITC exceeds what you collected — CRA owes you "
                f"${abs(net_tax):,.2f}. See Box 405."
            )
        else:
            lines.append(f"  Enter:   $0.00")
            lines.append(f"  Perfectly balanced — nothing owed either way.")
    else:
        lines.append(f"  Enter:   Box 103 minus Box 106")
        lines.append(
            f"  If positive → you owe CRA that amount.\n"
            f"  If negative → CRA owes you a refund."
        )
    lines.append("")

    # Box 205 — Amount owing
    lines.append(f"  BOX 205 — Payment Due to CRA")
    if net_tax is not None and net_tax > 0:
        lines.append(f"  Enter:   ${net_tax:,.2f}")
        lines.append(f"  Pay this by your filing deadline.")
    elif net_tax is not None and net_tax <= 0:
        lines.append(f"  Enter:   $0.00  (you are getting a refund — see Box 405)")
    else:
        lines.append(f"  Enter:   Amount from Box 109 (if positive)")
    lines.append("")

    # Box 405 — Refund
    lines.append(f"  BOX 405 — Refund Claimed")
    if net_tax is not None and net_tax < 0:
        lines.append(f"  Enter:   ${abs(net_tax):,.2f}")
        lines.append(f"  CRA will refund this to your bank account.")
    else:
        lines.append(f"  Enter:   $0.00  (leave blank if you owe CRA)")
    lines.append("")

    # ── ITC Breakdown ─────────────────────────────────────────────
    lines.append("=" * 55)
    lines.append("  HOW YOUR ITC WAS CALCULATED")
    lines.append("  (Box 106 breakdown — keep this for your records)")
    lines.append("=" * 55)
    lines.append("")

    d_cat = d["cat_totals"]
    # ITC-eligible categories
    itc_cats = {
        "Motor Vehicle Expense", "Office Supplies", "Materials & Supplies",
        "Rent", "Utilities", "Advertising", "Travel", "Professional Fees",
        "Subcontracts", "Cost of Goods", "Repairs & Maintenance",
        "Delivery & Shipping", "Meals & Entertainment",
    }
    # Recalculate per category
    cat_itc = {}
    for t in transactions:
        cat = t.get("category", "")
        itc_amt = t.get("itc_amount", 0) or 0
        if cat in itc_cats and itc_amt > 0:
            cat_itc[cat] = cat_itc.get(cat, 0) + itc_amt

    if cat_itc:
        for cat, itc_amt in sorted(cat_itc.items(), key=lambda x: -x[1]):
            note = " (50% rule applied)" if cat == "Meals & Entertainment" else ""
            lines.append(f"  {cat:<35} ${itc_amt:>8,.2f}{note}")
        lines.append("")
        lines.append(f"  {'TOTAL ITC (Box 106)':<35} ${itc:>8,.2f}")
        lines.append("")

    # ── Important notes ────────────────────────────────────────────
    lines.append("=" * 55)
    lines.append("  IMPORTANT NOTES")
    lines.append("=" * 55)
    lines.append("")
    lines.append(
        "  • Keep all receipts for 6 years after filing."
    )
    lines.append(
        "  • Meals & Entertainment: CRA only allows 50% deduction."
        "\n    This has already been applied to your ITC calculation."
    )
    if province == "QC":
        lines.append(
            "  • Quebec: You must also file a separate QST return with"
            "\n    Revenu Quebec. Contact your accountant for QST amounts."
        )
    lines.append(
        "  • Insurance premiums and bank charges are exempt from ITC."
        "\n    These are correctly excluded from Box 106."
    )
    lines.append("")

    # ── Filing deadlines ──────────────────────────────────────────
    lines.append("=" * 55)
    lines.append("  FILING DEADLINES")
    lines.append("=" * 55)
    lines.append("")
    deadlines = {
        "quarterly": "One month after the end of your quarter.",
        "monthly":   "One month after the end of the reporting period.",
        "annual":    "Three months after your fiscal year-end.",
    }
    lines.append(f"  Your filing frequency: {filing_period.title()}")
    lines.append(f"  Deadline: {deadlines.get(filing_period, 'Check with CRA')}")
    lines.append("")
    lines.append(
        "  Late filing penalty: 1% of balance owing + 0.25% per month."
        "\n  File on time even if you cannot pay — reduces penalties."
    )
    lines.append("")

    # ── Footer ────────────────────────────────────────────────────
    lines.append("─" * 55)
    if bookkeeper_name:
        lines.append(f"  Prepared by {bookkeeper_name}")
    lines.append(
        f"  Generated by BookKeep AI Pro | {today}"
        "\n  For informational purposes only."
        "\n  Confirm figures with your accountant before filing."
    )
    lines.append("─" * 55)

    return "\n".join(lines)


# ═══════════════════════════════════════════════════════════════════
# STREAMLIT INTEGRATION
# Add to app.py results section after the download button
# ═══════════════════════════════════════════════════════════════════

def render_client_summary_section(
    transactions, business_name, period,
    industry, province, compliance_score, st
):
    """
    Renders both the client summary email and GST34 guide
    inside Streamlit expanders. Call from app.py results section.

    from client_summary import render_client_summary_section
    render_client_summary_section(
        txns, business_name, period,
        industry, prov_display,
        compliance_result.get('score', 0), st
    )
    """
    st.divider()
    st.subheader("📨 Client Communication Tools")
    st.caption(
        "Ready-to-send outputs for your client — "
        "no accounting jargon, no Excel."
    )

    col1, col2 = st.columns(2)

    # ── Plain English Email ──────────────────────────────────────
    with col1:
        with st.expander("✉️ Client Summary Email", expanded=False):
            st.caption(
                "Copy and paste this into an email to your client. "
                "Tells them what you found, what you need from them, "
                "and what their HST refund is."
            )
            bk_name = st.text_input(
                "Your name (for sign-off)",
                key="summary_bk_name",
                placeholder="Jane Smith CPA"
            )
            if st.button("Generate Email",
                         key="gen_summary", use_container_width=True):
                summary = generate_client_summary(
                    transactions     = transactions,
                    business_name    = business_name,
                    period           = period,
                    industry         = industry,
                    province         = province,
                    bookkeeper_name  = bk_name,
                    compliance_score = compliance_score,
                )
                st.text_area(
                    "Copy this and paste into your email:",
                    value=summary,
                    height=500,
                    key="summary_output"
                )
                st.download_button(
                    "⬇️ Download as .txt",
                    data=summary.encode("utf-8"),
                    file_name=f"{business_name}_{period}_Summary.txt",
                    mime="text/plain",
                    key="dl_summary",
                )

    # ── GST34 Guide ──────────────────────────────────────────────
    with col2:
        with st.expander("📋 GST34 Filing Guide", expanded=False):
            st.caption(
                "Shows your client exactly which number goes in "
                "which box on the CRA GST34 form. "
                "They open CRA My Business Account and fill in the boxes."
            )

            g_col1, g_col2 = st.columns(2)
            with g_col1:
                bk_name2 = st.text_input(
                    "Your name",
                    key="gst_bk_name",
                    placeholder="Jane Smith CPA"
                )
                filing_freq = st.selectbox(
                    "Filing frequency",
                    ["quarterly", "monthly", "annual"],
                    key="gst_freq"
                )
            with g_col2:
                total_sales_str = st.text_input(
                    "Client's total sales (optional)",
                    key="gst_sales",
                    placeholder="e.g. 45000"
                )
                hst_collected_str = st.text_input(
                    "HST collected from clients (optional)",
                    key="gst_hst",
                    placeholder="e.g. 5200"
                )

            try:
                total_sales   = float(total_sales_str.replace(",","").replace("$","")) if total_sales_str.strip() else None
                hst_collected = float(hst_collected_str.replace(",","").replace("$","")) if hst_collected_str.strip() else None
            except ValueError:
                total_sales = hst_collected = None

            if st.button("Generate GST34 Guide",
                         key="gen_gst34", use_container_width=True):
                guide = generate_gst34_guide(
                    transactions    = transactions,
                    business_name   = business_name,
                    period          = period,
                    province        = province,
                    filing_period   = filing_freq,
                    total_sales     = total_sales,
                    hst_collected   = hst_collected,
                    bookkeeper_name = bk_name2,
                )
                st.text_area(
                    "Print this or email to your client:",
                    value=guide,
                    height=500,
                    key="gst34_output"
                )
                st.download_button(
                    "⬇️ Download as .txt",
                    data=guide.encode("utf-8"),
                    file_name=f"{business_name}_{period}_GST34_Guide.txt",
                    mime="text/plain",
                    key="dl_gst34",
                )

            # Show the ITC total prominently
            d = _build_totals(transactions)
            if d["total_itc"] > 0:
                st.metric(
                    "ITC for Box 106",
                    f"${d['total_itc']:,.2f}",
                    help="This is the number that goes in Box 106 of the GST34 form"
                )
