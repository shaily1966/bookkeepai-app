# BookKeep AI Pro v3.11

## What Changed (v3.10 → v3.11)

### Code Review Fixes (23 issues resolved)

**Critical (Financial Accuracy):**
- Costco WHOLESALE/GAS keywords preserved during merchant normalization — eliminates 32+ uncategorized rows
- Amazon transactions get confidence penalty (cap 70) for Construction/Trades — flags for receipt verification
- PAYMENT rows get explicit empty category instead of Python None

**High (Code Reliability):**
- Text/PDF batches now use retry helper (was raw stream — transient errors silently dropped batches)
- Dead duplicate `build_excel()` removed from exports.py
- 4x duplicate `import csv` blocks removed
- All 30+ bare `except:` replaced with typed exceptions + logging
- Confidence thresholds standardized: ≥85 green, 70-84 yellow, <70 red
- VERSION constant used everywhere (was 3 different strings)
- Ghost pattern regex uses `^...$` anchors — no more false positives on merchant names
- Government Remittances + Repairs & Maintenance added to VALID_CATEGORIES

**Medium (Robustness):**
- Receipt matching uses ±2% tolerance (was fixed $0.05)
- Dedup engine guards against removing legitimate recurring monthly charges
- Prompt injection protection: business name sanitized before API calls
- Period auto-detected from PDF summary when user doesn't enter one
- Audit trail split into FINANCIAL vs COSMETIC changes
- Cost estimate shows first-request vs cached pricing
- normalize_date falls back to existing transaction years instead of 2026

---

## What Changed (v1 → v2)

### The Real Problem Was Never the Instructions
v1 gutted the 17K-token system prompt to 500 tokens, losing 90% of the intelligence.
But with prompt caching, the full system prompt costs **$0.0005 per request**.

**The actual cost bottleneck was the OUTPUT format** — asking Claude to return verbose
JSON with every field quoted and bracketed wastes ~60% of output tokens.

### v2 Fix: Full Brain + Compact Output

| | v1 (Token Optimized) | v2 (Pro) |
|--|---------------------|----------|
| System prompt | 500 tokens (stripped) | 17K tokens (full, cached) |
| Accuracy | ~60% (missing vendor keywords, industry rules, business/personal logic) | ~95% (full BookKeepAI intelligence) |
| Output format | Verbose JSON (~4500 tokens) | Compact TSV (~1800 tokens) |
| Output cost | $0.067/request | $0.027/request |
| Total cost | ~$0.069/stmt | ~$0.035/stmt |
| Speed | ~45 sec | ~20 sec |
| Excel tabs | 2 (Transactions, Summary) | 5 (Transactions, Expense Summary, HST ITC, Needs Review, Summary) |
| Receipt upload | ❌ | ✅ (photos + PDFs) |
| Pass 2 workflow | ❌ | ✅ (answer flags, re-process) |
| Flagged questions | Generic | Specific (with amounts, dates, options) |
| Confidence colors | None | 🟢 green / 🟡 yellow / 🔴 red row highlighting |

### Monthly Cost Comparison (440 statements)

| Setup | Monthly Cost |
|-------|-------------|
| v1 Full instructions, no caching | $52.49 |
| v1 Condensed, no caching | $30.36 |
| v1 Condensed + caching | $29.74 |
| **v2 Full instructions + caching + TSV output** | **$15.40** |

**v2 is cheaper AND smarter.** The trick was optimizing the right thing (output) instead of the wrong thing (instructions).

## Setup

```bash
pip install -r requirements.txt

# Add your API key (one of these):
echo "sk-ant-..." > api_key.txt
# OR
export ANTHROPIC_API_KEY=sk-ant-...

# Run
streamlit run app.py
```

## For Accountants

Upload your client's bank statement (PDF) → get a categorized Excel file in ~20 seconds.

The Excel has 5 tabs:
1. **All Transactions** — every transaction categorized with T2125 lines, ITC amounts, confidence scores
2. **Expense Summary** — totals by CRA T2125 category
3. **HST-GST ITC** — input tax credit calculation
4. **Needs Review** — flagged items with specific questions to ask your client
5. **Summary** — income, expenses, net cash flow, total ITC

Optional: upload receipt photos alongside the statement for receipt-matched verification.
