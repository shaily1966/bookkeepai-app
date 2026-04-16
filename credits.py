"""
BookKeep AI Pro — Desktop Credits System
=========================================
Manages the local credit balance for the desktop version.

Credits are stored in a local SQLite database on the bookkeeper's machine.
One credit = one statement processed.

Flow:
  1. Bookkeeper buys credits at bookkeepai.ca (Stripe/Gumroad)
  2. Receives a license key by email
  3. Enters key in the desktop app — credits added locally
  4. Each statement processed deducts one credit
  5. No internet required after activation (except Claude API)

Credit bundles (set on your payment page):
  10 credits  →  $50   ($5.00/statement)
  25 credits  →  $100  ($4.00/statement)
  50 credits  →  $175  ($3.50/statement)
  100 credits →  $300  ($3.00/statement)

License key format:
  BKAI-XXXX-XXXX-XXXX-XXXX
  Where XXXX are alphanumeric segments.
  Key encodes: credit amount + expiry + checksum.

Integration into app.py:
─────────────────────────
import os
from credits import (
    get_credit_balance, deduct_credit,
    activate_license_key, render_credits_sidebar
)

DESKTOP_MODE = os.environ.get("BOOKKEEPAI_DESKTOP", "0") == "1"

# In sidebar:
if DESKTOP_MODE:
    render_credits_sidebar(st)

# Before processing:
if DESKTOP_MODE:
    if get_credit_balance() <= 0:
        st.error("No credits remaining. Purchase more at bookkeepai.ca")
        st.stop()

# After successful processing:
if DESKTOP_MODE:
    deduct_credit()
"""

import sqlite3
import os
import hashlib
import re
from datetime import datetime, timedelta

# Credits DB lives next to the app on the user's machine
CREDITS_DB = os.environ.get("CREDITS_DB_PATH",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "bookkeepai_credits.db"))

# Your secret salt — change this before shipping.
# Keep it secret. It prevents people from forging license keys.
_SALT = "BKAI2025CANADA"


# ═══════════════════════════════════════════════════════════════════
# DB SETUP
# ═══════════════════════════════════════════════════════════════════

def _get_credits_db():
    conn = sqlite3.connect(CREDITS_DB, check_same_thread=False)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS credits (
            id          INTEGER PRIMARY KEY,
            balance     INTEGER DEFAULT 0,
            total_used  INTEGER DEFAULT 0,
            updated     TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS license_keys (
            key         TEXT PRIMARY KEY,
            credits     INTEGER,
            activated   TEXT,
            expires     TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS credit_history (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            action      TEXT,
            amount      INTEGER,
            balance     INTEGER,
            note        TEXT,
            timestamp   TEXT
        )
    """)
    # Ensure one credits row exists
    conn.execute("""
        INSERT OR IGNORE INTO credits (id, balance, total_used, updated)
        VALUES (1, 0, 0, ?)
    """, (datetime.now().isoformat(),))
    conn.commit()
    return conn


# ═══════════════════════════════════════════════════════════════════
# CREDIT BALANCE
# ═══════════════════════════════════════════════════════════════════

def get_credit_balance():
    """Return current credit balance."""
    try:
        conn = _get_credits_db()
        row = conn.execute("SELECT balance FROM credits WHERE id=1").fetchone()
        conn.close()
        return row[0] if row else 0
    except Exception:
        return 0


def get_total_used():
    """Return total statements processed lifetime."""
    try:
        conn = _get_credits_db()
        row = conn.execute("SELECT total_used FROM credits WHERE id=1").fetchone()
        conn.close()
        return row[0] if row else 0
    except Exception:
        return 0


def deduct_credit(note="Statement processed"):
    """
    Deduct one credit for a processed statement.
    Returns True if successful, False if insufficient balance.
    """
    try:
        conn = _get_credits_db()
        row = conn.execute("SELECT balance FROM credits WHERE id=1").fetchone()
        if not row or row[0] <= 0:
            conn.close()
            return False
        new_balance = row[0] - 1
        now = datetime.now().isoformat()
        conn.execute("""
            UPDATE credits SET balance=?, total_used=total_used+1, updated=?
            WHERE id=1
        """, (new_balance, now))
        conn.execute("""
            INSERT INTO credit_history (action, amount, balance, note, timestamp)
            VALUES ('DEDUCT', -1, ?, ?, ?)
        """, (new_balance, note, now))
        conn.commit()
        conn.close()
        return True
    except Exception:
        return False


def add_credits(amount, source="License key"):
    """Add credits to balance. Called after successful license activation."""
    try:
        conn = _get_credits_db()
        row = conn.execute("SELECT balance FROM credits WHERE id=1").fetchone()
        new_balance = (row[0] if row else 0) + amount
        now = datetime.now().isoformat()
        conn.execute("""
            UPDATE credits SET balance=?, updated=? WHERE id=1
        """, (new_balance, now))
        conn.execute("""
            INSERT INTO credit_history (action, amount, balance, note, timestamp)
            VALUES ('ADD', ?, ?, ?, ?)
        """, (amount, new_balance, source, now))
        conn.commit()
        conn.close()
        return new_balance
    except Exception:
        return 0


# ═══════════════════════════════════════════════════════════════════
# LICENSE KEY SYSTEM
# ═══════════════════════════════════════════════════════════════════

def _generate_checksum(credits, expiry_str):
    """Generate a 4-char checksum for a license key."""
    raw = f"{_SALT}{credits}{expiry_str}"
    return hashlib.sha256(raw.encode()).hexdigest()[:4].upper()


def generate_license_key(credits, valid_days=3650):
    """
    Generate a license key for a given credit amount.
    Call this from your payment fulfillment script (not in the app itself).

    Example:
        key = generate_license_key(25)  # 25-credit key
        # Email this key to the customer after Stripe payment

    Returns: BKAI-XXXX-XXXX-XXXX-XXXX format string
    """
    import random, string
    expiry = (datetime.now() + timedelta(days=valid_days)).strftime("%Y%m%d")
    checksum = _generate_checksum(f"{credits:04d}", expiry)

    # Encode credits and expiry into key segments
    seg1 = f"{credits:04d}"[:4]
    seg2 = expiry[:4]   # YYYY
    seg3 = expiry[4:]   # MMDD
    seg4 = checksum

    # Add some random chars for obfuscation
    rand = "".join(random.choices(string.ascii_uppercase + string.digits, k=4))

    return f"BKAI-{seg1}-{seg2}{seg3}-{rand}-{seg4}"


def validate_license_key(key):
    """
    Validate a license key and return (credits, expiry) or (0, None) if invalid.

    Key format: BKAI-CCCC-YYYYMMDD-RRRR-HHHH
    Where:
        CCCC     = credit amount (zero-padded)
        YYYYMMDD = expiry date
        RRRR     = random (ignored)
        HHHH     = checksum
    """
    key = key.strip().upper().replace(" ", "")

    # Format check
    pattern = r'^BKAI-(\d{4})-(\d{8})-([A-Z0-9]{4})-([A-Z0-9]{4})$'
    match = re.match(pattern, key)
    if not match:
        return 0, None

    credits_str, expiry_str, _, checksum = match.groups()

    # Checksum validation
    expected = _generate_checksum(credits_str, expiry_str)
    if checksum != expected:
        return 0, None

    # Expiry check
    try:
        expiry_date = datetime.strptime(expiry_str, "%Y%m%d")
        if expiry_date < datetime.now():
            return 0, None   # Expired
    except ValueError:
        return 0, None

    credits = int(credits_str)
    if credits <= 0:
        return 0, None

    return credits, expiry_date


def activate_license_key(key):
    """
    Activate a license key and add credits to balance.

    Returns:
        (True, credits_added, new_balance) on success
        (False, 0, message_string) on failure
    """
    credits, expiry = validate_license_key(key)

    if not credits:
        return False, 0, "Invalid or expired license key. Check the key and try again."

    # Check if already used
    try:
        conn = _get_credits_db()
        existing = conn.execute(
            "SELECT key FROM license_keys WHERE key=?", (key,)
        ).fetchone()

        if existing:
            conn.close()
            return False, 0, "This key has already been activated."

        # Record key as used
        conn.execute("""
            INSERT INTO license_keys (key, credits, activated, expires)
            VALUES (?, ?, ?, ?)
        """, (key, credits, datetime.now().isoformat(),
               expiry.isoformat() if expiry else ""))
        conn.commit()
        conn.close()

        # Add credits
        new_balance = add_credits(credits, source=f"License key: {key[:12]}...")
        return True, credits, new_balance

    except Exception as e:
        return False, 0, f"Activation failed: {str(e)}"


def get_credit_history(limit=20):
    """Return recent credit history for display."""
    try:
        conn = _get_credits_db()
        rows = conn.execute("""
            SELECT action, amount, balance, note, timestamp
            FROM credit_history
            ORDER BY id DESC LIMIT ?
        """, (limit,)).fetchall()
        conn.close()
        return [
            {"action": r[0], "amount": r[1], "balance": r[2],
             "note": r[3], "timestamp": r[4]}
            for r in rows
        ]
    except Exception:
        return []


# ═══════════════════════════════════════════════════════════════════
# STREAMLIT SIDEBAR RENDERER
# ═══════════════════════════════════════════════════════════════════

def render_credits_sidebar(st):
    """
    Render the credits panel in the Streamlit sidebar.
    Call this from app.py when DESKTOP_MODE is True.

    if DESKTOP_MODE:
        render_credits_sidebar(st)
    """
    st.sidebar.divider()
    st.sidebar.subheader("💳 Credits")

    balance = get_credit_balance()
    used    = get_total_used()

    # Balance display — colour by urgency
    if balance == 0:
        st.sidebar.error(f"**0 credits remaining**\nPurchase more to continue processing.")
    elif balance <= 3:
        st.sidebar.warning(f"**{balance} credit{'s' if balance != 1 else ''} remaining**\nRunning low — top up soon.")
    else:
        st.sidebar.success(f"**{balance} credit{'s' if balance != 1 else ''} remaining**")

    st.sidebar.caption(f"Total statements processed: {used}")

    # Buy credits button
    st.sidebar.link_button(
        "💳 Buy Credits — bookkeepai.ca",
        "https://bookkeepai.ca/credits",   # your Stripe/Gumroad page
        use_container_width=True,
    )

    # License key activation
    with st.sidebar.expander("🔑 Activate License Key", expanded=(balance == 0)):
        st.caption("Enter the key you received after purchase.")
        key_input = st.text_input(
            "License Key",
            placeholder="BKAI-XXXX-XXXX-XXXX-XXXX",
            key="license_key_input"
        )
        if st.button("Activate", key="activate_key_btn", use_container_width=True):
            if key_input.strip():
                success, credits_added, result = activate_license_key(key_input.strip())
                if success:
                    st.success(
                        f"✅ Activated! Added {credits_added} credits.\n"
                        f"New balance: {result} credits."
                    )
                    st.rerun()
                else:
                    st.error(f"❌ {result}")
            else:
                st.warning("Enter a license key first.")

    # Credit history
    with st.sidebar.expander("📋 Credit History", expanded=False):
        history = get_credit_history(10)
        if history:
            for h in history:
                icon = "➕" if h["action"] == "ADD" else "➖"
                ts   = h["timestamp"][:10] if h["timestamp"] else ""
                st.caption(
                    f"{icon} {h['amount']:+d} credits | {ts}\n"
                    f"Balance: {h['balance']} | {h['note'][:40]}"
                )
        else:
            st.caption("No history yet.")


# ═══════════════════════════════════════════════════════════════════
# CREDIT GATE — call before processing
# ═══════════════════════════════════════════════════════════════════

def check_credits_gate(st):
    """
    Check if credits are available. Show error and stop if not.
    Call this right before the Claude API processing begins.

    Usage in app.py:
        if DESKTOP_MODE:
            from credits import check_credits_gate
            check_credits_gate(st)
        # ... rest of processing
    """
    balance = get_credit_balance()
    if balance <= 0:
        st.error(
            "### No Credits Remaining\n\n"
            "You've used all your credits. Purchase more to continue processing statements.\n\n"
            "**[Buy Credits at bookkeepai.ca](https://bookkeepai.ca/credits)**"
        )
        st.info(
            "💡 **Credit bundles:**\n"
            "- 10 credits — $50 ($5.00/statement)\n"
            "- 25 credits — $100 ($4.00/statement)\n"
            "- 50 credits — $175 ($3.50/statement)\n"
            "- 100 credits — $300 ($3.00/statement)\n\n"
            "Credits never expire. Use them at your own pace."
        )
        st.stop()
    return balance
