"""
BookKeep AI Pro — Export Utilities
Standalone module (no Streamlit dependency) for QuickBooks/Xero CSV generation.
Importable by tests and by app.py alike.

NOTE: build_excel() lives in app.py — it was previously duplicated here
but removed in v3.11 to eliminate drift between the two copies.
"""


def build_quickbooks_csv(transactions, biz_name, period_str):
    """
    Export transactions in QuickBooks Online CSV import format.
    Columns: Date, Description, Amount, Account, Tax Code, Memo
    Negatives = expenses, positives = income/credits.
    """
    import io, csv
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["Date", "Description", "Amount", "Account", "Tax Code", "Memo"])

    # QBO account name mapping from T2125 categories
    ACCOUNT_MAP = {
        "Motor Vehicle Expense": "Vehicle Expenses",
        "Meals & Entertainment": "Meals and Entertainment",
        "Office Supplies": "Office Expenses",
        "Utilities": "Utilities",
        "Bank Charges": "Bank Service Charges",
        "Insurance": "Insurance Expense",
        "Materials & Supplies": "Job Materials",
        "Rent": "Rent or Lease",
        "Advertising": "Advertising",
        "Travel": "Travel",
        "Professional Fees": "Legal & Professional Fees",
        "Subcontracts": "Subcontractors",
        "Cost of Goods": "Cost of Goods Sold",
        "Repairs & Maintenance": "Repairs & Maintenance",
        "Delivery & Shipping": "Shipping & Delivery Expense",
        "Government Remittances": "Government Remittances",
        "Owner Draw / Personal": "Owner\'s Draw",
        "Shareholder Loan (Debit)": "Shareholder Loans",
    }

    for t in transactions:
        date = t.get("date", "")
        desc = t.get("description", "")
        debit = t.get("debit", 0)
        credit = t.get("credit", 0)
        cat = t.get("category", "Uncategorized")
        notes = t.get("notes", "")

        # QBO: expenses are negative, credits/payments are positive
        if debit and debit > 0:
            amount = -round(debit, 2)
        elif credit and credit > 0:
            amount = round(credit, 2)
        else:
            continue  # skip zero rows

        account = ACCOUNT_MAP.get(cat, cat or "Uncategorized Expense")

        # Tax code: GST/HST if ITC was claimed
        itc = t.get("itc_amount", 0)
        tax_code = "HST/GST" if itc and itc > 0 else "Exempt"

        writer.writerow([date, desc, amount, account, tax_code, notes])

    return buf.getvalue().encode("utf-8-sig")  # BOM for Excel compatibility


def build_xero_csv(transactions, biz_name, period_str):
    """
    Export transactions in Xero bank statement import format.
    Xero columns: Date, Amount, Payee, Description, Reference, Check Number, Currency
    Xero convention: negative = money out (debit), positive = money in (credit).
    """
    import io, csv
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["Date", "Amount", "Payee", "Description", "Reference", "Check Number", "Currency"])

    for t in transactions:
        date = t.get("date", "")
        debit = t.get("debit", 0)
        credit = t.get("credit", 0)
        desc = t.get("description", "")
        cat = t.get("category", "")
        notes = t.get("notes", "")
        source = t.get("source", "")

        if debit and debit > 0:
            amount = -round(debit, 2)  # Xero: outflow is negative
        elif credit and credit > 0:
            amount = round(credit, 2)  # Xero: inflow is positive
        else:
            continue

        # Xero: Payee = merchant, Description = category + notes, Reference = source
        payee = desc
        description = f"{cat} — {notes}".strip(" —") if notes else cat
        reference = source

        writer.writerow([date, amount, payee, description, reference, "", "CAD"])

    return buf.getvalue().encode("utf-8-sig")
