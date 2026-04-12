"""
BookKeep AI Pro — Per-Client Data Persistence v1.0
===================================================
Stores processed statement results per named client in SQLite.

Every time a statement is processed and saved, it is linked to the
client by name. This enables:

  - Full statement history per client
  - Year-to-date rollup across saved statements
  - Month-over-month expense comparison
  - Category trends over time
  - One-click reload of any past statement
  - CRA audit package (all transactions for a tax year)

Schema
──────
statements        — one row per processed statement save
client_transactions — individual transaction rows, FK to statements
category_totals   — denormalised monthly category totals for fast YTD

Integration into app.py
────────────────────────
from client_data import (
    save_statement, load_client_statements, load_statement_transactions,
    delete_statement, get_ytd_summary, get_category_trends,
    export_client_package
)

# After download button — offer save:
if st.button("💾 Save to Client Record"):
    stmt_id = save_statement(business_name, period, txns, meta)
    st.success(f"Saved to {business_name} history (ID {stmt_id})")

# In client profile loader — show history:
history = load_client_statements(business_name)
"""

import json
import io
import logging
import re
from datetime import datetime
from db import get_conn, is_postgres, placeholder, placeholders, upsert_sql, now_sql, create_all_tables

logger = logging.getLogger("bookkeep_ai.client_data")


# ═══════════════════════════════════════════════════════════════════
# SCHEMA INITIALISATION
# ═══════════════════════════════════════════════════════════════════

def _get_db():
    """Return a database connection (Postgres or SQLite via db.py)."""
    return get_conn()


def _legacy_create():
    """Legacy: kept for backward compat — create_all_tables() in db.py handles this now."""
    try:
        create_all_tables()
    except Exception as e:
        logger.warning(f"_legacy_create: {e}")


# Keep old executescript block as dead code comment only — schema now in db.py


# ═══════════════════════════════════════════════════════════════════
# SAVE STATEMENT
# ═══════════════════════════════════════════════════════════════════

def save_statement(client_name, period, transactions, meta=None):
    """
    Persist a processed statement and all its transactions for a named client.

    Args:
        client_name:  string — must match the client profile name
        period:       string — e.g. "Jan 2025" or "Jan–Dec 2025"
        transactions: list of transaction dicts from app.py processing
        meta:         optional dict with keys:
                        file_names, bank, province, industry,
                        business_structure, extraction_confidence,
                        api_cost, statement_notes

    Returns:
        statement_id (int) on success, None on failure
    """
    if not client_name or not transactions:
        return None

    meta = meta or {}
    now = datetime.now().isoformat()

    # Compute summary metrics
    purchases = [t for t in transactions
                 if t.get("type") not in ("PAYMENT", "FEE_REBATE")]
    total_exp  = round(sum(t.get("debit", 0) for t in purchases if t.get("debit")), 2)
    total_inc  = round(sum(t.get("credit", 0) for t in transactions
                           if t.get("type") not in ("PAYMENT", "REFUND", "FEE_REBATE")
                           and t.get("credit")), 2)
    total_pay  = round(sum(t.get("credit", 0) for t in transactions
                           if t.get("type") == "PAYMENT" and t.get("credit")), 2)
    total_itc  = round(sum(t.get("itc_amount", 0) for t in transactions), 2)
    uncat      = sum(1 for t in transactions if "Uncategorized" in t.get("category", ""))
    cca        = sum(1 for t in transactions if "CCA_ASSET" in t.get("notes", ""))
    t5018      = sum(1 for t in transactions if "T5018" in t.get("notes", ""))

    try:
        conn = _get_db()
        p = placeholder()

        # Insert statement header
        cur = conn.cursor()
        cur.execute(f"""
            INSERT INTO statements
                (client_name, period, file_names, bank, province, industry,
                 business_structure, processed_date, total_transactions,
                 total_expenses, total_income, total_payments, total_itc,
                 uncategorized_count, cca_asset_count, t5018_count,
                 extraction_confidence, api_cost, statement_notes)
            VALUES ({placeholders(19)})
        """, (
            client_name,
            period or "Unknown period",
            json.dumps(meta.get("file_names", [])),
            meta.get("bank", ""),
            meta.get("province", "ON"),
            meta.get("industry", ""),
            meta.get("business_structure", ""),
            now,
            len(transactions),
            total_exp, total_inc, total_pay, total_itc,
            uncat, cca, t5018,
            meta.get("extraction_confidence", 0),
            meta.get("api_cost", 0.0),
            meta.get("statement_notes", ""),
        ))
        # Get last inserted ID — Postgres uses RETURNING, SQLite uses lastrowid
        if is_postgres():
            # Re-query for the last inserted id
            cur.execute("SELECT lastval()")
            stmt_id = cur.fetchone()[0]
        else:
            stmt_id = cur.lastrowid

        # Insert individual transactions
        txn_rows = [
            (
                stmt_id, client_name,
                t.get("date", ""), t.get("source", ""), t.get("description", ""),
                t.get("debit", 0) or 0, t.get("credit", 0) or 0,
                t.get("type", ""), t.get("category", ""), t.get("t2125", ""),
                t.get("itc_amount", 0) or 0, str(t.get("confidence", "")),
                t.get("notes", ""),
            )
            for t in transactions
        ]
        cur.executemany(f"""
            INSERT INTO client_transactions
                (statement_id, client_name, date, source, description,
                 debit, credit, txn_type, category, t2125,
                 itc_amount, confidence, notes)
            VALUES ({placeholders(13)})
        """, txn_rows)

        # Insert denormalised category totals for fast YTD queries
        cat_totals = {}
        for t in transactions:
            cat = t.get("category", "❓ Uncategorized")
            if not cat or t.get("type") in ("PAYMENT", "FEE_REBATE"):
                continue
            if cat not in cat_totals:
                cat_totals[cat] = {"amount": 0.0, "itc": 0.0, "count": 0}
            cat_totals[cat]["amount"] += t.get("debit", 0) or 0
            cat_totals[cat]["itc"]    += t.get("itc_amount", 0) or 0
            cat_totals[cat]["count"]  += 1

        cur.executemany(f"""
            INSERT INTO category_totals
                (statement_id, client_name, period, category,
                 total_amount, total_itc, txn_count)
            VALUES ({placeholders(7)})
        """, [
            (stmt_id, client_name, period or "", cat,
             round(v["amount"], 2), round(v["itc"], 2), v["count"])
            for cat, v in cat_totals.items()
        ])

        conn.commit()
        cur.close()
        conn.close()
        logger.info(f"Saved statement {stmt_id} for {client_name} ({len(transactions)} txns)")
        return stmt_id

    except Exception as e:
        logger.error(f"save_statement failed for {client_name}: {e}")
        return None


# ═══════════════════════════════════════════════════════════════════
# LOAD CLIENT HISTORY
# ═══════════════════════════════════════════════════════════════════

def load_client_statements(client_name):
    """
    Return all saved statement headers for a client, newest first.

    Returns list of dicts:
        id, period, bank, province, processed_date,
        total_transactions, total_expenses, total_itc,
        uncategorized_count, cca_asset_count, t5018_count,
        extraction_confidence, api_cost, file_names
    """
    try:
        conn = _get_db()
        p = placeholder()
        rows = conn.execute("""
            SELECT id, period, bank, province, industry, processed_date,
                   total_transactions, total_expenses, total_income,
                   total_payments, total_itc, uncategorized_count,
                   cca_asset_count, t5018_count, extraction_confidence,
                   api_cost, file_names, statement_notes
            FROM statements
            WHERE client_name = {p}
            ORDER BY processed_date DESC
        """, (client_name,)).fetchall()
        conn.close()

        return [{
            "id":                    r[0],
            "period":                r[1],
            "bank":                  r[2],
            "province":              r[3],
            "industry":              r[4],
            "processed_date":        r[5],
            "total_transactions":    r[6],
            "total_expenses":        r[7],
            "total_income":          r[8],
            "total_payments":        r[9],
            "total_itc":             r[10],
            "uncategorized_count":   r[11],
            "cca_asset_count":       r[12],
            "t5018_count":           r[13],
            "extraction_confidence": r[14],
            "api_cost":              r[15],
            "file_names":            json.loads(r[16] or "[]"),
            "statement_notes":       r[17],
        } for r in rows]

    except Exception as e:
        logger.error(f"load_client_statements failed for {client_name}: {e}")
        return []


def load_statement_transactions(statement_id):
    """
    Load all transactions for a specific saved statement.
    Returns list of transaction dicts compatible with app.py format.
    """
    try:
        conn = _get_db()
        p = placeholder()
        rows = conn.execute("""
            SELECT date, source, description, debit, credit,
                   txn_type, category, t2125, itc_amount, confidence, notes
            FROM client_transactions
            WHERE statement_id = {p}
            ORDER BY date, id
        """, (statement_id,)).fetchall()
        conn.close()

        return [{
            "date":        r[0],
            "source":      r[1],
            "description": r[2],
            "debit":       r[3] or 0,
            "credit":      r[4] or 0,
            "type":        r[5],
            "category":    r[6],
            "t2125":       r[7],
            "itc_amount":  r[8] or 0,
            "confidence":  r[9],
            "notes":       r[10],
            "balance":     0,
        } for r in rows]

    except Exception as e:
        logger.error(f"load_statement_transactions({statement_id}) failed: {e}")
        return []


def delete_statement(statement_id):
    """Delete a saved statement and all its transactions (CASCADE)."""
    try:
        conn = _get_db()
        p = placeholder()
        conn.execute("DELETE FROM statements WHERE id = {p}", (statement_id,))
        conn.commit()
        conn.close()
        return True
    except Exception as e:
        logger.error(f"delete_statement({statement_id}) failed: {e}")
        return False


# ═══════════════════════════════════════════════════════════════════
# YEAR-TO-DATE SUMMARY
# ═══════════════════════════════════════════════════════════════════

def get_ytd_summary(client_name, tax_year=None):
    """
    Aggregate all saved statements for a client into a YTD summary.

    Args:
        client_name: string
        tax_year:    int (e.g. 2025) — filter by year. None = all time.

    Returns dict:
        statements_count, total_transactions, total_expenses,
        total_income, total_itc, total_payments,
        uncategorized_count, cca_asset_count, t5018_count,
        by_category: {category: {amount, itc, count}},
        by_period:   [{period, total_expenses, total_itc}],
        periods_covered: [str]
    """
    try:
        conn = _get_db()
        p = placeholder()

        # Filter by year if specified
        year_filter = ""
        params_stmts = [client_name]
        params_cats  = [client_name]
        if tax_year:
            year_filter = "AND period LIKE {p}"
            params_stmts.append(f"%{tax_year}%")
            params_cats.append(f"%{tax_year}%")

        # Statement-level aggregation
        agg = conn.execute(f"""
            SELECT
                COUNT(*),
                SUM(total_transactions),
                SUM(total_expenses),
                SUM(total_income),
                SUM(total_payments),
                SUM(total_itc),
                SUM(uncategorized_count),
                SUM(cca_asset_count),
                SUM(t5018_count)
            FROM statements
            WHERE client_name = {p} {year_filter}
        """, params_stmts).fetchone()

        # Category-level rollup
        cat_rows = conn.execute(f"""
            SELECT category, SUM(total_amount), SUM(total_itc), SUM(txn_count)
            FROM category_totals
            WHERE client_name = {p} {year_filter}
            GROUP BY category
            ORDER BY SUM(total_amount) DESC
        """, params_cats).fetchall()

        # Per-period breakdown (for trend chart)
        period_rows = conn.execute(f"""
            SELECT period, total_expenses, total_itc, processed_date
            FROM statements
            WHERE client_name = {p} {year_filter}
            ORDER BY processed_date ASC
        """, params_stmts).fetchall()

        conn.close()

        by_category = {
            r[0]: {"amount": round(r[1] or 0, 2),
                   "itc":    round(r[2] or 0, 2),
                   "count":  r[3] or 0}
            for r in cat_rows if r[0]
        }

        by_period = [
            {"period": r[0], "total_expenses": r[1] or 0,
             "total_itc": r[2] or 0, "processed_date": r[3]}
            for r in period_rows
        ]

        return {
            "statements_count":    agg[0] or 0,
            "total_transactions":  agg[1] or 0,
            "total_expenses":      round(agg[2] or 0, 2),
            "total_income":        round(agg[3] or 0, 2),
            "total_payments":      round(agg[4] or 0, 2),
            "total_itc":           round(agg[5] or 0, 2),
            "uncategorized_count": agg[6] or 0,
            "cca_asset_count":     agg[7] or 0,
            "t5018_count":         agg[8] or 0,
            "by_category":         by_category,
            "by_period":           by_period,
            "periods_covered":     [r["period"] for r in by_period],
        }

    except Exception as e:
        logger.error(f"get_ytd_summary failed for {client_name}: {e}")
        return {}


# ═══════════════════════════════════════════════════════════════════
# CATEGORY TRENDS — month-over-month per category
# ═══════════════════════════════════════════════════════════════════

def get_category_trends(client_name, category, tax_year=None):
    """
    Return per-period amounts for a specific category.
    Used for the trend line in the client dashboard.

    Returns list of {period, amount, itc} dicts ordered by period.
    """
    try:
        conn = _get_db()
        p = placeholder()
        year_filter = "AND period LIKE {p}" if tax_year else ""
        params = [client_name, category]
        if tax_year:
            params.append(f"%{tax_year}%")

        rows = conn.execute(f"""
            SELECT ct.period, ct.total_amount, ct.total_itc
            FROM category_totals ct
            JOIN statements s ON s.id = ct.statement_id
            WHERE ct.client_name = {p} AND ct.category = {p} {year_filter}
            ORDER BY s.processed_date ASC
        """, params).fetchall()
        conn.close()

        return [{"period": r[0], "amount": round(r[1] or 0, 2),
                 "itc": round(r[2] or 0, 2)} for r in rows]

    except Exception as e:
        logger.error(f"get_category_trends failed: {e}")
        return []


# ═══════════════════════════════════════════════════════════════════
# ALL-CLIENT OVERVIEW — for accountants with many clients
# ═══════════════════════════════════════════════════════════════════

def get_all_clients_summary():
    """
    Return a high-level summary of all clients with saved data.
    Used to build the accountant overview dashboard.

    Returns list of {client_name, statements_count, latest_period,
                     total_expenses_ytd, total_itc_ytd, uncategorized} dicts.
    """
    try:
        conn = _get_db()
        rows = conn.execute("""
            SELECT
                client_name,
                COUNT(*) AS stmt_count,
                MAX(processed_date) AS latest,
                MAX(period) AS latest_period,
                SUM(total_expenses) AS ytd_exp,
                SUM(total_itc) AS ytd_itc,
                SUM(uncategorized_count) AS uncat_total
            FROM statements
            GROUP BY client_name
            ORDER BY MAX(processed_date) DESC
        """).fetchall()
        conn.close()

        return [{
            "client_name":       r[0],
            "statements_count":  r[1],
            "latest_processed":  r[2],
            "latest_period":     r[3],
            "total_expenses_ytd": round(r[4] or 0, 2),
            "total_itc_ytd":     round(r[5] or 0, 2),
            "uncategorized":     r[6] or 0,
        } for r in rows]

    except Exception as e:
        logger.error(f"get_all_clients_summary failed: {e}")
        return []


# ═══════════════════════════════════════════════════════════════════
# EXPORT — full client data package for CRA audit or year-end
# ═══════════════════════════════════════════════════════════════════

def export_client_package(client_name, tax_year=None):
    """
    Export all transactions for a client as a CSV bytes object.
    Suitable for CRA audit package or handoff to external accountant.

    Returns (csv_bytes, filename_string).
    """
    import csv as _csv

    try:
        conn = _get_db()
        p = placeholder()
        year_filter = ""
        params = [client_name]
        if tax_year:
            year_filter = "AND s.period LIKE ?"
            params.append(f"%{tax_year}%")

        rows = conn.execute(f"""
            SELECT
                ct.date, s.period, ct.source, ct.description,
                ct.debit, ct.credit, ct.txn_type, ct.category,
                ct.t2125, ct.itc_amount, ct.confidence, ct.notes,
                s.bank, s.province
            FROM client_transactions ct
            JOIN statements s ON s.id = ct.statement_id
            WHERE ct.client_name = {p} {year_filter}
            ORDER BY ct.date, ct.id
        """, params).fetchall()
        conn.close()

        buf = io.StringIO()
        writer = _csv.writer(buf)
        writer.writerow([
            "Date", "Period", "Bank/Source", "Description",
            "Debit", "Credit", "Type", "CRA Category",
            "T2125 Line", "ITC Amount", "Confidence", "Notes",
            "Bank", "Province"
        ])
        for r in rows:
            writer.writerow([
                r[0], r[1], r[2], r[3],
                f"{r[4]:.2f}" if r[4] else "",
                f"{r[5]:.2f}" if r[5] else "",
                r[6], r[7], r[8],
                f"{r[9]:.2f}" if r[9] else "",
                r[10], r[11], r[12], r[13]
            ])

        year_tag = f"_{tax_year}" if tax_year else ""
        safe_name = re.sub(r'[^\w\s-]', '', client_name).strip().replace(' ', '_')
        filename = f"{safe_name}_BookKeepAI{year_tag}_CRA_Package.csv"

        return buf.getvalue().encode("utf-8-sig"), filename

    except Exception as e:
        logger.error(f"export_client_package failed for {client_name}: {e}")
        return b"", f"{client_name}_export_failed.csv"


# ═══════════════════════════════════════════════════════════════════
# STREAMLIT UI COMPONENTS
# Called from app.py to render client history panels
# ═══════════════════════════════════════════════════════════════════

def render_client_history(client_name, st):
    """
    Render the client history panel inside Streamlit.
    Call this from app.py after the client profile selector.

    Shows:
      - YTD summary metrics
      - Statement history table with reload + delete
      - Category breakdown
      - CRA export button
    """
    import pandas as pd

    statements = load_client_statements(client_name)
    if not statements:
        st.caption(f"No saved statements for {client_name} yet. Process a statement and click 💾 Save to Client Record.")
        return

    # ── YTD Summary ──────────────────────────────────────────────────
    current_year = datetime.now().year
    ytd = get_ytd_summary(client_name, tax_year=current_year)

    if ytd.get("statements_count", 0) > 0:
        st.markdown(f"##### 📊 {current_year} Year-to-Date — {ytd['statements_count']} statement(s)")
        col1, col2, col3, col4, col5 = st.columns(5)
        col1.metric("Total Expenses",    f"${ytd['total_expenses']:,.2f}")
        col2.metric("HST ITC Claimable", f"${ytd['total_itc']:,.2f}")
        col3.metric("Transactions",       ytd['total_transactions'])
        col4.metric("Uncategorized",      ytd['uncategorized_count'])
        col5.metric("CCA Assets",         ytd['cca_asset_count'])

        # Category breakdown
        if ytd.get("by_category"):
            with st.expander("📂 Expenses by Category — YTD", expanded=False):
                cat_df = pd.DataFrame([
                    {"Category": cat, "Amount": f"${v['amount']:,.2f}",
                     "ITC": f"${v['itc']:,.2f}", "Transactions": v["count"]}
                    for cat, v in ytd["by_category"].items()
                    if "Personal" not in cat and "Uncategorized" not in cat
                ])
                if not cat_df.empty:
                    st.dataframe(cat_df, use_container_width=True, hide_index=True)

    # ── Statement History Table ───────────────────────────────────────
    st.markdown("##### 🗂 Statement History")
    for stmt in statements:
        proc_date = stmt["processed_date"][:10] if stmt["processed_date"] else "Unknown"
        uncat_flag = f" · ⚠️ {stmt['uncategorized_count']} uncategorized" if stmt["uncategorized_count"] else ""
        label = (
            f"📄 **{stmt['period'] or 'Unknown period'}** "
            f"| {stmt['total_transactions']} txns "
            f"| ${stmt['total_expenses']:,.2f} expenses "
            f"| ${stmt['total_itc']:,.2f} ITC"
            f"{uncat_flag} "
            f"| Saved {proc_date}"
        )
        with st.expander(label, expanded=False):
            sc1, sc2, sc3, sc4 = st.columns(4)
            sc1.metric("Transactions",  stmt["total_transactions"])
            sc2.metric("Expenses",      f"${stmt['total_expenses']:,.2f}")
            sc3.metric("ITC",           f"${stmt['total_itc']:,.2f}")
            sc4.metric("Confidence",    f"{stmt['extraction_confidence']}%")

            files = stmt.get("file_names", [])
            if files:
                st.caption(f"Files: {', '.join(files)}")

            col_reload, col_delete = st.columns([3, 1])
            with col_reload:
                if st.button(f"🔄 Reload transactions into session",
                              key=f"reload_{stmt['id']}"):
                    txns = load_statement_transactions(stmt["id"])
                    if txns:
                        st.session_state.transactions = txns
                        st.session_state.summary = {
                            "period": stmt["period"],
                            "transactions": str(len(txns))
                        }
                        st.success(f"✅ Loaded {len(txns)} transactions from {stmt['period']}")
                        st.rerun()
                    else:
                        st.error("Failed to load transactions.")
            with col_delete:
                if st.button("🗑️ Delete", key=f"del_{stmt['id']}",
                              help="Permanently delete this saved statement"):
                    if delete_statement(stmt["id"]):
                        st.success("Deleted.")
                        st.rerun()

    # ── CRA Export ───────────────────────────────────────────────────
    st.divider()
    col_exp1, col_exp2 = st.columns(2)
    with col_exp1:
        csv_bytes, csv_fname = export_client_package(client_name, tax_year=current_year)
        st.download_button(
            label=f"⬇️ Export {current_year} CRA Package (.csv)",
            data=csv_bytes,
            file_name=csv_fname,
            mime="text/csv",
            use_container_width=True,
            help=f"All {current_year} transactions for {client_name} — suitable for CRA audit or year-end handoff"
        )
    with col_exp2:
        csv_all, fname_all = export_client_package(client_name, tax_year=None)
        st.download_button(
            label="⬇️ Export All Years (.csv)",
            data=csv_all,
            file_name=fname_all,
            mime="text/csv",
            use_container_width=True,
            help="Complete transaction history for all saved periods"
        )


def render_save_button(client_name, transactions, period, meta, st):
    """
    Render the 💾 Save to Client Record button in the results section.
    Call this from app.py after the download button.

    Returns statement_id if saved, None otherwise.
    """
    if not client_name or not transactions:
        return None

    st.divider()
    col_save, col_info = st.columns([2, 3])
    with col_info:
        st.caption(
            f"💾 Save these {len(transactions)} transactions to **{client_name}'s** "
            f"permanent record. Enables YTD rollup, history reload, and CRA export."
        )
    with col_save:
        if st.button(
            f"💾 Save to {client_name}'s Record",
            type="secondary",
            use_container_width=True,
            key="save_to_client_record"
        ):
            stmt_id = save_statement(client_name, period, transactions, meta)
            if stmt_id:
                st.success(
                    f"✅ Saved to **{client_name}** — Statement #{stmt_id} | "
                    f"{len(transactions)} transactions | Period: {period or 'unspecified'}"
                )
                return stmt_id
            else:
                st.error("❌ Save failed — check logs.")
    return None


def render_all_clients_dashboard(st):
    """
    Render the accountant overview — all clients with saved data.
    Call from app.py sidebar or a separate tab.
    """
    import pandas as pd

    clients = get_all_clients_summary()
    if not clients:
        st.caption("No client data saved yet.")
        return

    st.markdown("#### 👥 All Clients — Overview")
    df = pd.DataFrame([{
        "Client":          c["client_name"],
        "Statements":      c["statements_count"],
        "Latest Period":   c["latest_period"] or "—",
        "YTD Expenses":    f"${c['total_expenses_ytd']:,.2f}",
        "YTD ITC":         f"${c['total_itc_ytd']:,.2f}",
        "Uncategorized":   c["uncategorized"] or 0,
        "Last Processed":  (c["latest_processed"] or "")[:10],
    } for c in clients])
    st.dataframe(df, use_container_width=True, hide_index=True)
