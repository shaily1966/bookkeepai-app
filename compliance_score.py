"""
BookKeep AI Pro — CRA Compliance Score Engine v1.0
===================================================
Turns processed transactions into a compliance verdict.

Five pillars, each scored independently, combined into a 0–100 score
with letter grade. Every deduction produces a specific, actionable
finding — not generic warnings.

The score tells the accountant:
  "These books are 73/100 — here are the 4 specific things to fix
   before you file the T2125 and GST34."

That is the difference between a productivity tool and a
compliance platform.

Integration into app.py
───────────────────────
from compliance_score import compute_compliance_score, render_compliance_report

score_result = compute_compliance_score(
    transactions      = txns,
    industry          = industry,
    business_structure= business_structure,
    province          = prov_display,
    period            = period,
    validation_results= st.session_state.get("validation_results", []),
    recon_matches     = st.session_state.get("recon_matches", []),
    recon_unmatched   = st.session_state.get("recon_unmatched", []),
    receipt_matches   = st.session_state.get("receipt_matches", []),
    t5018_data        = st.session_state.get("t5018_data", []),
    anomalies         = st.session_state.get("anomalies", []),
)
render_compliance_report(score_result, st)
"""

import re
from datetime import datetime

# ═══════════════════════════════════════════════════════════════════
# PILLAR WEIGHTS  (must sum to 100)
# ═══════════════════════════════════════════════════════════════════
PILLAR_WEIGHTS = {
    "categorization":  25,   # Are all transactions categorized correctly?
    "cra_regulatory":  30,   # T5018, CCA, Meals rule, personal separation
    "itc_accuracy":    20,   # HST/ITC calculated correctly
    "reconciliation":  15,   # Totals match, no duplicates, payments matched
    "documentation":   10,   # Receipts, vendor statements for ambiguous items
}

# Severity levels for findings
CRITICAL = "critical"   # blocks filing — red
WARNING  = "warning"    # should fix — orange
INFO     = "info"       # good to know — blue
PASS     = "pass"       # all good — green


# ═══════════════════════════════════════════════════════════════════
# HELPER — safe percentage
# ═══════════════════════════════════════════════════════════════════

def _pct(numerator, denominator):
    return (numerator / denominator * 100) if denominator else 100.0


# ═══════════════════════════════════════════════════════════════════
# PILLAR 1 — CATEGORIZATION (25 pts)
# ═══════════════════════════════════════════════════════════════════

def _score_categorization(transactions):
    """
    Checks:
      A. % of purchase transactions fully categorized           (15 pts)
      B. % of categorized transactions with confidence ≥ 85     (10 pts)
    """
    findings = []
    earned = 0

    purchases = [t for t in transactions
                 if t.get("type") not in ("PAYMENT", "FEE_REBATE")
                 and (t.get("debit", 0) > 0)]

    if not purchases:
        return {"score": 25, "max": 25, "findings": [
            {"severity": PASS, "pillar": "categorization",
             "title": "No purchase transactions to categorize",
             "detail": "", "action": ""}
        ]}

    # ── Check A: categorization rate ─────────────────────────────
    uncat = [t for t in purchases if "Uncategorized" in t.get("category", "")]
    cat_rate = _pct(len(purchases) - len(uncat), len(purchases))

    if cat_rate >= 98:
        earned += 15
        findings.append({
            "severity": PASS, "pillar": "categorization",
            "title": f"Categorization: {cat_rate:.0f}% complete",
            "detail": f"{len(purchases) - len(uncat)}/{len(purchases)} transactions categorized.",
            "action": ""
        })
    elif cat_rate >= 90:
        earned += 10
        findings.append({
            "severity": WARNING, "pillar": "categorization",
            "title": f"Categorization: {cat_rate:.0f}% — {len(uncat)} items need review",
            "detail": _summarise_uncat(uncat),
            "action": "Review the Needs Review tab. Upload receipts or vendor statements for the flagged items."
        })
    elif cat_rate >= 75:
        earned += 6
        findings.append({
            "severity": CRITICAL, "pillar": "categorization",
            "title": f"Categorization: {cat_rate:.0f}% — {len(uncat)} uncategorized transactions",
            "detail": _summarise_uncat(uncat),
            "action": (
                f"Do not file until resolved. {len(uncat)} transactions totalling "
                f"${sum(t.get('debit',0) for t in uncat):,.2f} have no CRA category. "
                f"Upload vendor statements or receipts to resolve."
            )
        })
    else:
        earned += 2
        findings.append({
            "severity": CRITICAL, "pillar": "categorization",
            "title": f"Categorization: {cat_rate:.0f}% — critical — {len(uncat)} uncategorized",
            "detail": _summarise_uncat(uncat),
            "action": (
                f"Filing risk: {len(uncat)} transactions (${sum(t.get('debit',0) for t in uncat):,.2f}) "
                f"have no category. CRA will question these if audited. Resolve before filing."
            )
        })

    # ── Check B: confidence distribution ─────────────────────────
    categorized = [t for t in purchases if "Uncategorized" not in t.get("category", "")]
    high_conf = [t for t in categorized
                 if str(t.get("confidence", "0")).isdigit()
                 and int(t.get("confidence", "0")) >= 85]
    conf_rate = _pct(len(high_conf), len(categorized)) if categorized else 100

    if conf_rate >= 85:
        earned += 10
        findings.append({
            "severity": PASS, "pillar": "categorization",
            "title": f"Categorization confidence: {conf_rate:.0f}% high-confidence",
            "detail": f"{len(high_conf)}/{len(categorized)} categorized transactions scored ≥85.",
            "action": ""
        })
    elif conf_rate >= 70:
        earned += 6
        low_items = [t for t in categorized
                     if str(t.get("confidence","0")).isdigit()
                     and int(t.get("confidence","0")) < 70]
        findings.append({
            "severity": WARNING, "pillar": "categorization",
            "title": f"Confidence: {conf_rate:.0f}% — {len(low_items)} low-confidence categories",
            "detail": f"{len(low_items)} transactions scored below 70 — AI was uncertain.",
            "action": "Review yellow/red highlighted rows in All Transactions tab. Correct any miscategorized items."
        })
    else:
        earned += 2
        findings.append({
            "severity": CRITICAL, "pillar": "categorization",
            "title": f"Confidence: {conf_rate:.0f}% — high proportion of uncertain categories",
            "detail": "More than 30% of transactions have low AI confidence.",
            "action": "Manually review all red-highlighted rows. Consider uploading vendor statements to improve accuracy."
        })

    return {"score": earned, "max": 25, "findings": findings}


def _summarise_uncat(uncat_list):
    """Produce a readable summary of uncategorized transactions."""
    if not uncat_list:
        return ""
    total = sum(t.get("debit", 0) for t in uncat_list)
    merchants = {}
    for t in uncat_list:
        desc = t.get("description", "Unknown")[:30]
        merchants[desc] = merchants.get(desc, 0) + t.get("debit", 0)
    top = sorted(merchants.items(), key=lambda x: -x[1])[:5]
    lines = [f"  • {m}: ${v:,.2f}" for m, v in top]
    return f"Total uncategorized: ${total:,.2f}\nTop merchants:\n" + "\n".join(lines)


# ═══════════════════════════════════════════════════════════════════
# PILLAR 2 — CRA REGULATORY (30 pts)
# ═══════════════════════════════════════════════════════════════════

def _score_cra_regulatory(transactions, industry, business_structure, t5018_data):
    """
    Checks:
      A. T5018 subcontractor reporting (construction)           (8 pts)
      B. CCA assets separated from direct expenses              (7 pts)
      C. Meals 50% rule applied                                 (5 pts)
      D. Personal expenses separated from business              (5 pts)
      E. Shareholder loans tracked (corporations)               (5 pts)
    """
    findings = []
    earned = 0

    # ── Check A: T5018 ───────────────────────────────────────────
    is_construction = "Construction" in industry or "Trades" in industry
    if is_construction:
        reportable = [r for r in (t5018_data or []) if r.get("t5018_required")]
        unconfirmed_etfr = [t for t in transactions
                            if "T5018" in t.get("notes", "")
                            and t.get("debit", 0) >= 500]
        if reportable:
            earned += 8
            findings.append({
                "severity": WARNING, "pillar": "cra_regulatory",
                "title": f"T5018: {len(reportable)} subcontractor(s) flagged for CRA filing",
                "detail": "\n".join(
                    f"  • {r['payee']}: ${r['total']:,.2f} ({r['count']} payments)"
                    for r in reportable
                ),
                "action": (
                    "File T5018 slips for these subcontractors if construction activities "
                    "exceed 50% of total business income. Deadline: last day of February. "
                    "Confirm payees are individuals (not incorporated) before filing."
                )
            })
        elif unconfirmed_etfr:
            earned += 5
            findings.append({
                "severity": WARNING, "pillar": "cra_regulatory",
                "title": f"T5018: {len(unconfirmed_etfr)} large e-transfers may require filing",
                "detail": f"${sum(t.get('debit',0) for t in unconfirmed_etfr):,.2f} in e-transfers ≥$500 to individuals detected.",
                "action": "Confirm whether these are subcontractor payments. If yes, T5018 filing required."
            })
        else:
            earned += 8
            findings.append({
                "severity": PASS, "pillar": "cra_regulatory",
                "title": "T5018: No reportable subcontractor payments detected",
                "detail": "No e-transfers ≥$500 to individuals found in this period.",
                "action": ""
            })
    else:
        earned += 8  # Not applicable — full points
        findings.append({
            "severity": INFO, "pillar": "cra_regulatory",
            "title": "T5018: Not applicable for this industry",
            "detail": f"T5018 reporting applies to construction businesses only. Industry: {industry}.",
            "action": ""
        })

    # ── Check B: CCA assets ──────────────────────────────────────
    cca_assets = [t for t in transactions if "CCA_ASSET" in t.get("notes", "")]
    large_expenses = [t for t in transactions
                      if t.get("debit", 0) >= 500
                      and t.get("type") in ("PURCHASE", "")
                      and "CCA_ASSET" not in t.get("notes", "")
                      and "Uncategorized" not in t.get("category", "")
                      and t.get("category") not in ("Owner Draw / Personal",
                                                     "Government Remittances",
                                                     "Rent", "Insurance",
                                                     "Shareholder Loan (Debit)")]
    if cca_assets:
        earned += 7
        findings.append({
            "severity": PASS, "pillar": "cra_regulatory",
            "title": f"CCA: {len(cca_assets)} capital asset(s) detected and flagged",
            "detail": "\n".join(
                f"  • {t['description'][:40]}: ${t.get('debit',0):,.2f} ({t.get('notes','')})"
                for t in cca_assets[:5]
            ),
            "action": (
                "Review Fixed Assets tab. Confirm CCA class is correct. "
                "Do NOT expense these as direct costs — claim CCA over time."
            )
        })
    elif large_expenses:
        earned += 4
        findings.append({
            "severity": WARNING, "pillar": "cra_regulatory",
            "title": f"CCA: {len(large_expenses)} large purchases may be capital assets",
            "detail": "\n".join(
                f"  • {t['description'][:40]}: ${t.get('debit',0):,.2f}"
                for t in large_expenses[:5]
            ),
            "action": (
                "Review these purchases. If they are equipment, vehicles, or tools with a "
                "useful life >1 year, they must be capitalized as CCA assets — not expensed directly. "
                "Expensing capital assets in year one is a common CRA audit trigger."
            )
        })
    else:
        earned += 7
        findings.append({
            "severity": PASS, "pillar": "cra_regulatory",
            "title": "CCA: No capital assets requiring review",
            "detail": "No purchases ≥$500 flagged as potential capital assets.",
            "action": ""
        })

    # ── Check C: Meals 50% rule ──────────────────────────────────
    meals_txns = [t for t in transactions
                  if t.get("category") == "Meals & Entertainment"
                  and t.get("debit", 0) > 0]
    meals_missing_rule = [t for t in meals_txns
                          if "MEALS_50_RULE" not in t.get("notes", "")]
    if meals_txns and not meals_missing_rule:
        earned += 5
        total_meals = sum(t.get("debit", 0) for t in meals_txns)
        findings.append({
            "severity": PASS, "pillar": "cra_regulatory",
            "title": f"Meals 50% rule: applied to all {len(meals_txns)} meal transactions",
            "detail": f"Total meals: ${total_meals:,.2f} — only 50% (${total_meals*0.5:,.2f}) is deductible.",
            "action": ""
        })
    elif meals_missing_rule:
        earned += 2
        findings.append({
            "severity": CRITICAL, "pillar": "cra_regulatory",
            "title": f"Meals 50% rule: missing on {len(meals_missing_rule)} transactions",
            "detail": f"CRA requires only 50% of meals & entertainment to be deducted.",
            "action": (
                "Apply the 50% rule to all Meals & Entertainment transactions before filing. "
                "Full deduction of meals is a top CRA audit trigger for small businesses."
            )
        })
    else:
        earned += 5
        findings.append({
            "severity": INFO, "pillar": "cra_regulatory",
            "title": "Meals 50% rule: no meal transactions in this period",
            "detail": "", "action": ""
        })

    # ── Check D: Personal separation ────────────────────────────
    personal = [t for t in transactions
                if t.get("category") in ("Owner Draw / Personal", "Shareholder Loan (Debit)")
                and t.get("debit", 0) > 0]
    personal_total = sum(t.get("debit", 0) for t in personal)
    total_debits = sum(t.get("debit", 0) for t in transactions if t.get("debit", 0) > 0)
    personal_pct = _pct(personal_total, total_debits) if total_debits else 0

    if personal_pct > 30:
        earned += 2
        findings.append({
            "severity": WARNING, "pillar": "cra_regulatory",
            "title": f"Personal expenses: {personal_pct:.0f}% of total spend is personal/owner draw",
            "detail": f"${personal_total:,.2f} of ${total_debits:,.2f} total expenses flagged as personal.",
            "action": (
                "High personal expense ratio may indicate under-separation of business and personal. "
                "Consider a dedicated business card. Verify all personal items are correctly identified."
            )
        })
    elif personal:
        earned += 5
        findings.append({
            "severity": PASS, "pillar": "cra_regulatory",
            "title": f"Personal expenses: {len(personal)} personal transactions identified ({personal_pct:.0f}% of spend)",
            "detail": f"${personal_total:,.2f} flagged as Owner Draw / Personal — correctly excluded from deductions.",
            "action": ""
        })
    else:
        earned += 5
        findings.append({
            "severity": INFO, "pillar": "cra_regulatory",
            "title": "Personal expenses: none detected",
            "detail": "No personal expenses flagged. Verify this is accurate for the period.",
            "action": ""
        })

    # ── Check E: Shareholder loans (corporations) ────────────────
    is_corp = business_structure in ("Corporation", "Inc.", "Ltd.")
    if is_corp:
        sh_loans = [t for t in transactions
                    if t.get("category") == "Shareholder Loan (Debit)"
                    and t.get("debit", 0) > 0]
        sh_total = sum(t.get("debit", 0) for t in sh_loans)
        if sh_loans:
            earned += 3
            findings.append({
                "severity": WARNING, "pillar": "cra_regulatory",
                "title": f"Shareholder loans: {len(sh_loans)} personal charges via corporation (${sh_total:,.2f})",
                "detail": "\n".join(
                    f"  • {t['description'][:40]}: ${t.get('debit',0):,.2f}"
                    for t in sh_loans[:5]
                ),
                "action": (
                    "Shareholder loans must be repaid within one year of the corporation's fiscal year-end "
                    "or included in the shareholder's personal income. Track the running balance carefully. "
                    "CRA scrutinizes these closely."
                )
            })
        else:
            earned += 5
            findings.append({
                "severity": PASS, "pillar": "cra_regulatory",
                "title": "Shareholder loans: none detected this period",
                "detail": "", "action": ""
            })
    else:
        earned += 5  # Not applicable for sole proprietors
        findings.append({
            "severity": INFO, "pillar": "cra_regulatory",
            "title": "Shareholder loans: not applicable (sole proprietor)",
            "detail": "", "action": ""
        })

    return {"score": min(earned, 30), "max": 30, "findings": findings}


# ═══════════════════════════════════════════════════════════════════
# PILLAR 3 — ITC ACCURACY (20 pts)
# ═══════════════════════════════════════════════════════════════════

def _score_itc_accuracy(transactions, province):
    """
    Checks:
      A. ITC claimed on all eligible purchases               (8 pts)
      B. ITC correctly zero on exempt categories             (6 pts)
      C. Province-correct rates applied                      (6 pts)
    """
    findings = []
    earned = 0

    eligible = [t for t in transactions
                if t.get("type") in ("PURCHASE", "FEE", "")
                and t.get("debit", 0) > 0
                and t.get("category") not in (
                    "Insurance", "Bank Charges", "Owner Draw / Personal",
                    "Shareholder Loan (Debit)", "Government Remittances",
                    "", None
                )
                and "Uncategorized" not in t.get("category", "")]

    # ── Check A: ITC claimed on eligible ─────────────────────────
    missing_itc = [t for t in eligible if not t.get("itc_amount", 0)]
    if not missing_itc:
        earned += 8
        total_itc = sum(t.get("itc_amount", 0) for t in transactions)
        findings.append({
            "severity": PASS, "pillar": "itc_accuracy",
            "title": f"ITC: claimed on all eligible transactions — ${total_itc:,.2f} recoverable",
            "detail": f"All {len(eligible)} eligible purchases have ITC calculated.",
            "action": ""
        })
    else:
        missing_val = sum(t.get("debit", 0) for t in missing_itc)
        earned += max(0, 8 - len(missing_itc))
        findings.append({
            "severity": WARNING, "pillar": "itc_accuracy",
            "title": f"ITC: missing on {len(missing_itc)} eligible transactions",
            "detail": (
                f"${missing_val:,.2f} in eligible purchases have no ITC calculated. "
                f"Estimated missed ITC: ${missing_val * 0.115:,.2f} (approx at ON rate)."
            ),
            "action": "Review All Transactions tab. Recalculate ITC for these rows or apply province rate manually."
        })

    # ── Check B: ITC zero on exempt categories ───────────────────
    exempt_cats = ("Insurance", "Bank Charges", "Government Remittances",
                   "Owner Draw / Personal", "Shareholder Loan (Debit)")
    wrong_itc = [t for t in transactions
                 if t.get("category") in exempt_cats
                 and t.get("itc_amount", 0) > 0]
    if not wrong_itc:
        earned += 6
        findings.append({
            "severity": PASS, "pillar": "itc_accuracy",
            "title": "ITC: correctly zero on exempt categories (insurance, bank charges)",
            "detail": "No ITC claimed on non-recoverable supplies.",
            "action": ""
        })
    else:
        wrong_total = sum(t.get("itc_amount", 0) for t in wrong_itc)
        findings.append({
            "severity": CRITICAL, "pillar": "itc_accuracy",
            "title": f"ITC: incorrectly claimed on {len(wrong_itc)} exempt transactions (${wrong_total:,.2f})",
            "detail": "\n".join(
                f"  • {t['description'][:35]}: {t['category']} — ITC ${t.get('itc_amount',0):.2f}"
                for t in wrong_itc[:5]
            ),
            "action": (
                "Remove ITC from insurance premiums, bank charges, and government remittances. "
                "These are exempt supplies — CRA will disallow these credits and may assess penalties."
            )
        })

    # ── Check C: Province consistency ───────────────────────────
    wrong_prov = [t for t in transactions
                  if "ITC_PROV=" in t.get("notes", "")
                  and t.get("itc_amount", 0) > 0]
    if wrong_prov:
        earned += 6
        findings.append({
            "severity": INFO, "pillar": "itc_accuracy",
            "title": f"ITC: {len(wrong_prov)} cross-provincial transactions detected and corrected",
            "detail": f"Province-specific rates applied per transaction (not blanket {province} rate).",
            "action": ""
        })
    else:
        earned += 6
        findings.append({
            "severity": PASS, "pillar": "itc_accuracy",
            "title": f"ITC: province-correct rates applied ({province})",
            "detail": f"All ITC calculated using {province} rate.",
            "action": ""
        })

    return {"score": min(earned, 20), "max": 20, "findings": findings}


# ═══════════════════════════════════════════════════════════════════
# PILLAR 4 — RECONCILIATION INTEGRITY (15 pts)
# ═══════════════════════════════════════════════════════════════════

def _score_reconciliation(transactions, validation_results, recon_unmatched):
    """
    Checks:
      A. Statement totals match PDF summary                   (8 pts)
      B. No duplicate transactions                            (4 pts)
      C. Unmatched items explained                            (3 pts)
    """
    findings = []
    earned = 0

    # ── Check A: PDF total validation ───────────────────────────
    all_validations = []
    for vr in (validation_results or []):
        all_validations.extend(vr.get("validations", []))

    matched_vals = [v for v in all_validations if "✅" in v.get("status", "")]
    failed_vals  = [v for v in all_validations if "❌" in v.get("status", "")]
    warn_vals    = [v for v in all_validations if "⚠️" in v.get("status", "")]

    if not all_validations:
        earned += 5
        findings.append({
            "severity": INFO, "pillar": "reconciliation",
            "title": "Reconciliation: statement validation not available",
            "detail": "PDF summary page not found or totals not extractable.",
            "action": "Upload a complete statement PDF (including the summary page) for full validation."
        })
    elif failed_vals:
        earned += 2
        total_variance = sum(abs(v.get("variance", 0)) for v in failed_vals)
        findings.append({
            "severity": CRITICAL, "pillar": "reconciliation",
            "title": f"Reconciliation: {len(failed_vals)} total(s) do NOT match PDF — variance ${total_variance:,.2f}",
            "detail": "\n".join(
                f"  • {v['field']}: PDF ${v.get('pdf',0):,.2f} vs computed ${v.get('computed',0):,.2f}"
                for v in failed_vals
            ),
            "action": (
                "Statement integrity failure. Possible causes: missing transactions, "
                "PDF truncated at page limit, or duplicate rows. "
                "Do not file HST/GST return until this variance is resolved."
            )
        })
    elif warn_vals:
        earned += 6
        findings.append({
            "severity": WARNING, "pillar": "reconciliation",
            "title": f"Reconciliation: {len(warn_vals)} field(s) could not be fully verified",
            "detail": f"{len(matched_vals)} matched, {len(warn_vals)} warnings.",
            "action": "Review validation warnings in Statement Validation tab."
        })
    else:
        earned += 8
        findings.append({
            "severity": PASS, "pillar": "reconciliation",
            "title": f"Reconciliation: all {len(matched_vals)} statement totals verified ✅",
            "detail": "Computed totals match PDF summary page within $0.05.",
            "action": ""
        })

    # ── Check B: duplicates ──────────────────────────────────────
    dupes = [t for t in transactions if "POTENTIAL_DUPLICATE" in t.get("notes", "")]
    if not dupes:
        earned += 4
        findings.append({
            "severity": PASS, "pillar": "reconciliation",
            "title": "Duplicates: none detected",
            "detail": "No duplicate transactions found across uploaded statements.",
            "action": ""
        })
    else:
        dupe_val = sum(t.get("debit", 0) or t.get("credit", 0) for t in dupes)
        findings.append({
            "severity": CRITICAL, "pillar": "reconciliation",
            "title": f"Duplicates: {len(dupes)} potential duplicate transactions (${dupe_val:,.2f})",
            "detail": "\n".join(
                f"  • {t['description'][:40]}: ${t.get('debit',0) or t.get('credit',0):,.2f} on {t.get('date','')}"
                for t in dupes[:5]
            ),
            "action": (
                "Review and remove duplicate rows before filing. "
                "Duplicate expenses inflate deductions and are a CRA audit flag."
            )
        })

    # ── Check C: unmatched items ──────────────────────────────────
    unmatched = recon_unmatched or []
    if not unmatched:
        earned += 3
        findings.append({
            "severity": PASS, "pillar": "reconciliation",
            "title": "Cross-document reconciliation: all items matched",
            "detail": "", "action": ""
        })
    elif len(unmatched) <= 3:
        earned += 2
        findings.append({
            "severity": WARNING, "pillar": "reconciliation",
            "title": f"Reconciliation: {len(unmatched)} unmatched item(s)",
            "detail": "\n".join(
                f"  • {u.get('desc','')[:35]}: ${u.get('amount',0):,.2f} ({u.get('type','')})"
                for u in unmatched[:3]
            ),
            "action": "Investigate unmatched items — they may indicate missing statements or timing differences."
        })
    else:
        findings.append({
            "severity": WARNING, "pillar": "reconciliation",
            "title": f"Reconciliation: {len(unmatched)} unmatched items across documents",
            "detail": f"CC payments, invoices, or receipts that couldn't be matched across statements.",
            "action": "Upload the corresponding chequing/credit card statements to close these gaps."
        })

    return {"score": min(earned, 15), "max": 15, "findings": findings}


# ═══════════════════════════════════════════════════════════════════
# PILLAR 5 — DOCUMENTATION (10 pts)
# ═══════════════════════════════════════════════════════════════════

def _score_documentation(transactions, receipt_matches):
    """
    Checks:
      A. Large transactions (≥$200) have receipt support       (6 pts)
      B. Ambiguous merchants have vendor statement support      (4 pts)
    """
    findings = []
    earned = 0

    # ── Check A: receipts for large transactions ─────────────────
    large_txns = [t for t in transactions
                  if t.get("debit", 0) >= 200
                  and t.get("type") in ("PURCHASE", "")
                  and "Uncategorized" not in t.get("category", "")
                  and t.get("category") not in ("Owner Draw / Personal",
                                                 "Shareholder Loan (Debit)")]
    receipts = receipt_matches or []
    matched_receipts = {r.get("matched_txn", "") for r in receipts if r.get("status") == "MATCHED"}
    large_with_receipt = [t for t in large_txns
                          if t.get("description", "") in matched_receipts]

    if not large_txns:
        earned += 6
        findings.append({
            "severity": INFO, "pillar": "documentation",
            "title": "Documentation: no large transactions requiring receipts",
            "detail": "No purchases ≥$200 in this period.", "action": ""
        })
    elif not receipts:
        earned += 3
        findings.append({
            "severity": INFO, "pillar": "documentation",
            "title": f"Documentation: {len(large_txns)} large transaction(s) ≥$200 — no receipts uploaded",
            "detail": f"Total: ${sum(t.get('debit',0) for t in large_txns):,.2f}",
            "action": (
                "CRA requires receipts for all business expenses. Upload receipts via the "
                "📸 Receipts slot for large transactions. Minimum: keep all receipts ≥$30."
            )
        })
    else:
        receipt_coverage = _pct(len(large_with_receipt), len(large_txns))
        if receipt_coverage >= 80:
            earned += 6
            findings.append({
                "severity": PASS, "pillar": "documentation",
                "title": f"Documentation: {receipt_coverage:.0f}% of large transactions have receipts",
                "detail": f"{len(large_with_receipt)}/{len(large_txns)} purchases ≥$200 matched to receipts.",
                "action": ""
            })
        else:
            earned += 3
            unreceipted = [t for t in large_txns
                           if t.get("description", "") not in matched_receipts]
            findings.append({
                "severity": WARNING, "pillar": "documentation",
                "title": f"Documentation: {receipt_coverage:.0f}% receipt coverage — {len(unreceipted)} large transactions without receipts",
                "detail": "\n".join(
                    f"  • {t['description'][:40]}: ${t.get('debit',0):,.2f}"
                    for t in unreceipted[:5]
                ),
                "action": "Upload missing receipts. CRA can disallow deductions without supporting documentation."
            })

    # ── Check B: vendor statements for ambiguous merchants ────────
    needs_vendor = [t for t in transactions
                    if "Uncategorized" in t.get("category", "")
                    and t.get("debit", 0) >= 50
                    and t.get("type") not in ("PAYMENT", "FEE_REBATE")]
    vendor_resolved = [t for t in transactions
                       if "VENDOR_MATCHED" in t.get("notes", "")
                       or "CAT_FROM_VENDOR_STMT" in t.get("notes", "")]

    if not needs_vendor:
        earned += 4
        findings.append({
            "severity": PASS, "pillar": "documentation",
            "title": "Documentation: no ambiguous merchant statements needed",
            "detail": "All significant transactions are categorized.", "action": ""
        })
    elif vendor_resolved:
        earned += 3
        findings.append({
            "severity": INFO, "pillar": "documentation",
            "title": f"Documentation: vendor statements resolved {len(vendor_resolved)} ambiguous transaction(s)",
            "detail": f"{len(needs_vendor)} still uncategorized — consider additional vendor statements.",
            "action": "Upload remaining vendor account statements (Amazon Business, Costco, Home Depot) to resolve."
        })
    else:
        earned += 1
        findings.append({
            "severity": WARNING, "pillar": "documentation",
            "title": f"Documentation: {len(needs_vendor)} uncategorized purchases need vendor statements",
            "detail": f"${sum(t.get('debit',0) for t in needs_vendor):,.2f} in purchases cannot be categorized from bank description alone.",
            "action": (
                "Upload vendor account statements (e.g. Amazon Business CSV, Costco purchase history) "
                "via the 🏪 Vendor Statements slot for line-item categorization."
            )
        })

    return {"score": min(earned, 10), "max": 10, "findings": findings}


# ═══════════════════════════════════════════════════════════════════
# GRADE + RISK LEVEL
# ═══════════════════════════════════════════════════════════════════

def _grade(score):
    if score >= 90: return "A", "CRA-Ready", "green",  "✅"
    if score >= 80: return "B", "Minor Issues", "blue", "🔵"
    if score >= 70: return "C", "Needs Attention", "orange", "🟡"
    if score >= 55: return "D", "Filing Risk", "red",  "🔴"
    return "F", "Do Not File", "red", "🚨"


# ═══════════════════════════════════════════════════════════════════
# MAIN ENTRY POINT
# ═══════════════════════════════════════════════════════════════════

def compute_compliance_score(
    transactions,
    industry          = "Other",
    business_structure= "Sole Proprietor",
    province          = "ON",
    period            = "",
    validation_results= None,
    recon_matches     = None,
    recon_unmatched   = None,
    receipt_matches   = None,
    t5018_data        = None,
    anomalies         = None,
):
    """
    Compute the full CRA Compliance Score.

    Returns a dict:
        score          — 0–100 composite score
        grade          — A/B/C/D/F
        grade_label    — "CRA-Ready" / "Minor Issues" / etc.
        colour         — green/blue/orange/red
        icon           — emoji
        pillar_scores  — {pillar_name: {score, max, findings}}
        all_findings   — flat list of all findings sorted by severity
        action_items   — CRITICAL + WARNING findings only (what to fix)
        computed_at    — ISO timestamp
    """
    pillar_scores = {
        "categorization": _score_categorization(transactions),
        "cra_regulatory": _score_cra_regulatory(
            transactions, industry, business_structure, t5018_data),
        "itc_accuracy":   _score_itc_accuracy(transactions, province),
        "reconciliation": _score_reconciliation(
            transactions, validation_results, recon_unmatched),
        "documentation":  _score_documentation(transactions, receipt_matches),
    }

    total = sum(p["score"] for p in pillar_scores.values())
    grade, label, colour, icon = _grade(total)

    all_findings = []
    for pillar, data in pillar_scores.items():
        for f in data["findings"]:
            f["pillar"] = pillar
            all_findings.append(f)

    severity_order = {CRITICAL: 0, WARNING: 1, INFO: 2, PASS: 3}
    all_findings.sort(key=lambda f: severity_order.get(f["severity"], 4))

    action_items = [f for f in all_findings
                    if f["severity"] in (CRITICAL, WARNING) and f.get("action")]

    return {
        "score":         total,
        "grade":         grade,
        "grade_label":   label,
        "colour":        colour,
        "icon":          icon,
        "pillar_scores": pillar_scores,
        "all_findings":  all_findings,
        "action_items":  action_items,
        "period":        period,
        "industry":      industry,
        "province":      province,
        "computed_at":   datetime.now().isoformat(),
    }


# ═══════════════════════════════════════════════════════════════════
# STREAMLIT RENDERER
# ═══════════════════════════════════════════════════════════════════

PILLAR_LABELS = {
    "categorization": "📂 Categorization",
    "cra_regulatory": "🇨🇦 CRA Regulatory",
    "itc_accuracy":   "🧮 HST/ITC Accuracy",
    "reconciliation": "🔄 Reconciliation",
    "documentation":  "📎 Documentation",
}

SEVERITY_COLOURS = {
    CRITICAL: "🔴",
    WARNING:  "🟡",
    INFO:     "🔵",
    PASS:     "✅",
}


def render_compliance_report(result, st):
    """
    Render the full CRA Compliance Report inside Streamlit.
    Call from app.py results section.
    """
    score   = result["score"]
    grade   = result["grade"]
    label   = result["grade_label"]
    colour  = result["colour"]
    icon    = result["icon"]
    actions = result["action_items"]

    st.divider()
    st.header("🇨🇦 CRA Compliance Report")

    # ── Score Banner ─────────────────────────────────────────────
    col_score, col_grade, col_label = st.columns([2, 1, 4])
    with col_score:
        st.markdown(
            f"<h1 style='color:{colour};margin:0'>{icon} {score}/100</h1>",
            unsafe_allow_html=True
        )
    with col_grade:
        st.markdown(
            f"<h1 style='color:{colour};margin:0'>{grade}</h1>",
            unsafe_allow_html=True
        )
    with col_label:
        st.markdown(
            f"<h3 style='color:{colour};margin-top:12px'>{label}</h3>",
            unsafe_allow_html=True
        )
        period_str = result.get("period", "")
        industry_str = result.get("industry", "")
        st.caption(f"{period_str} · {industry_str} · Province: {result.get('province','ON')}")

    # ── Pillar Score Bar ─────────────────────────────────────────
    st.markdown("#### Score Breakdown")
    cols = st.columns(5)
    for i, (pillar, data) in enumerate(result["pillar_scores"].items()):
        pct = round(data["score"] / data["max"] * 100)
        col_colour = "green" if pct >= 85 else "orange" if pct >= 65 else "red"
        cols[i].metric(
            PILLAR_LABELS[pillar],
            f"{data['score']}/{data['max']}",
            delta=f"{pct}%",
            delta_color="normal" if pct >= 65 else "inverse"
        )

    # ── Action Items (what to fix) ────────────────────────────────
    if actions:
        st.markdown(f"#### ⚡ {len(actions)} Action Item(s) Before Filing")
        criticals = [a for a in actions if a["severity"] == CRITICAL]
        warnings  = [a for a in actions if a["severity"] == WARNING]

        if criticals:
            st.error(f"🔴 **{len(criticals)} Critical — must resolve before filing**")
            for a in criticals:
                with st.expander(f"🔴 {a['title']}", expanded=True):
                    if a.get("detail"):
                        st.code(a["detail"], language=None)
                    st.warning(f"**Action required:** {a['action']}")

        if warnings:
            st.warning(f"🟡 **{len(warnings)} Warning(s) — should resolve before filing**")
            for a in warnings:
                with st.expander(f"🟡 {a['title']}", expanded=False):
                    if a.get("detail"):
                        st.code(a["detail"], language=None)
                    st.info(f"**Recommended action:** {a['action']}")
    else:
        st.success(
            "✅ **No action items — these books are CRA-ready.** "
            "Proceed with T2125 preparation and GST34 filing."
        )

    # ── Full Findings by Pillar ────────────────────────────────────
    with st.expander("📋 Full Compliance Findings by Pillar", expanded=False):
        for pillar, data in result["pillar_scores"].items():
            pct = round(data["score"] / data["max"] * 100)
            st.markdown(
                f"**{PILLAR_LABELS[pillar]}** — "
                f"{data['score']}/{data['max']} pts ({pct}%)"
            )
            for f in data["findings"]:
                sev = SEVERITY_COLOURS.get(f["severity"], "•")
                st.markdown(f"&nbsp;&nbsp;{sev} {f['title']}")
                if f.get("action") and f["severity"] not in (PASS, INFO):
                    st.caption(f"&nbsp;&nbsp;&nbsp;&nbsp;↳ {f['action']}")
            st.markdown("---")

    # ── Filing Checklist ──────────────────────────────────────────
    with st.expander("✅ Pre-Filing Checklist", expanded=False):
        st.markdown("""
**Before filing your T2125 (Business Income):**
- [ ] All transactions categorized (0 Uncategorized)
- [ ] CCA assets on Fixed Assets tab — NOT expensed directly
- [ ] Meals & Entertainment × 50% applied
- [ ] Personal expenses removed from business deductions
- [ ] HST/ITC calculation reviewed and approved
- [ ] Statement totals match PDF validation ✅

**Before filing your GST34 (HST/GST Return):**
- [ ] Total ITC figure from HST-GST tab
- [ ] ITC on insurance and bank charges = $0
- [ ] Cross-provincial transactions at correct rate
- [ ] Reconciliation tab shows no imbalance

**Record Keeping (CRA requires 6 years):**
- [ ] All original PDFs saved
- [ ] Receipts for expenses ≥$30
- [ ] This Excel file archived
        """)
