"""
Canadian Bank Statement Schema System v3.3
==========================================
v3.3 Changes:
  - Added Tangerine, Simplii, National Bank, Desjardins, BMO Chequing, Scotiabank Chequing
  - Merchant normalization layer (strip store #s, region codes, bank prefixes)
  - Industry profiles: Construction, Retail, Professional, Restaurant, Rental
  - Province override from description suffix sensing (AB/BC/QC etc.)
  - Shareholder Loan detection for incorporated businesses
  - CCA Class assignment (Class 8, 10, 12, 50)
  - T5018 subcontractor aggregation
  - Post-processing validation engine (auto-fix common ledger errors)
  - Refund-match logic (prevents merchant refunds being misclassified as revenue)
  - Year-bound check for cross-year statements
  - Transaction coverage estimation
  - Expense anomaly detection
"""

import re
from datetime import datetime

# ═══════════════════════════════════════════════════════════════════
# BANK SCHEMAS
# ═══════════════════════════════════════════════════════════════════

BANK_SCHEMAS = {
    "td_chequing": {
        "bank": "TD Canada Trust",
        "type": "chequing",
        "fingerprints": ["EasyWeb", "TD Canada Trust", "tdcanadatrust.com", "TD Access Card"],
        "negative_fingerprints": ["Visa", "First Class Travel", "TD Cash Back"],
        "date_format": "MMM DD, YYYY",
        "date_regex": r'((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2},\s+\d{4})',
        "transaction_regex": r'^((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2},\s+\d{4})\s+(?:View (?:more|Cheque)\s+)?(.+?)\s+([\d,]+\.\d{2})\s+\$([\d,]+\.\d{2})',
        "columns": ["date", "description", "amount", "balance"],
        "amount_logic": "balance_change",
        "sort_order": "newest_first",
        "continuation_regex": r'^\d{5,}$|^SUPP$|^BONU$',
        "noise_patterns": ["View more", "View Cheque", "Opening Balance", "Closing Balance"],
        "section_start": None, "section_end": None, "tax_markers": {},
    },
    "td_visa": {
        "bank": "TD Visa",
        "type": "credit_card",
        "fingerprints": ["TD First Class Travel", "TD Cash Back", "TD Visa", "TD Aeroplan"],
        "negative_fingerprints": ["EasyWeb"],
        "date_format": "MMM DD",
        "date_regex": r'((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2})',
        "transaction_regex": r'^((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2})\s+((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2})\s+(.+?)\s+(-?[\d,]+\.\d{2})\s*$',
        "columns": ["trans_date", "post_date", "description", "amount"],
        "amount_logic": "signed", "sort_order": "chronological", "continuation_regex": None,
        "noise_patterns": ["Previous Balance", "Payment - Thank You", "NEW BALANCE", "MINIMUM PAYMENT"],
        "section_start": ["NEW CHARGES", "TRANSACTIONS", "Activity since"],
        "section_end": ["TOTAL NEW CHARGES", "Minimum Payment", "Interest Charges"],
        "tax_markers": {},
    },
    "cibc_visa": {
        "bank": "CIBC Visa",
        "type": "credit_card",
        "fingerprints": ["CIBC", "Aventura", "Dividend", "Aeroplan", "cibc.com"],
        "negative_fingerprints": ["chequing", "savings", "Business Operating", "SmartBusiness", "Business Plus", "Business Select"],
        "date_format": "MMM DD",
        "date_regex": r'((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2})',
        "transaction_regex": r'^((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2})\s+((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2})\s+(.+?)\s+(-?[\d,]+\.\d{2})\s*$',
        "columns": ["trans_date", "post_date", "description", "amount"],
        "amount_logic": "signed", "sort_order": "chronological", "continuation_regex": None,
        "noise_patterns": ["Previous Balance", "Payment", "NEW BALANCE", "Your new charges"],
        "section_start": ["Your new charges and credits", "TRANSACTIONS"],
        "section_end": ["Interest Charges", "TOTAL"],
        "tax_markers": {},
    },
    "triangle_mc": {
        "bank": "Triangle Mastercard",
        "type": "credit_card",
        "fingerprints": ["Triangle", "Canadian Tire Bank", "ctfs.com", "Triangle Mastercard", "Gas Bar", "Canadian Tire", "Sport Chek", "Mark's"],
        "negative_fingerprints": [],
        "date_format": "MMM DD",
        "date_regex": r'((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2})',
        "transaction_regex": r'^((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2})\s+((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2})\s+(.+?)\s+(-?[\d,]+\.\d{2})\s*(\*{0,2})\s*$',
        "columns": ["trans_date", "post_date", "description", "amount", "tax_flag"],
        "amount_logic": "signed", "sort_order": "chronological", "continuation_regex": None,
        "noise_patterns": [
            "Previous Balance", "PMT/PAIEMENT", "PAYMENT", "INTEREST",
            # Section headers that look like transactions but are totals — DO NOT extract:
            "Payments received",      # "Payments received Apr 17 to May 16" — summary line
            "Returns and other",       # "Returns and other credits" — section header total
            "Total payments",          # "Total payments received"
            "Total new charges",
            "Minimum payment",
            "New balance",
        ],
        "section_start": ["TRANSACTIONS", "Activity"],
        "section_end": ["TOTAL", "Interest", "Fees"],
        "tax_markers": {"*": "HST/GST included", "**": "PST included"},
        "double_entry_patterns": [r'(GAS BAR|ESSENCE)\s+.*?([\d,]+\.\d{2})\s+([\d,]+\.\d{2})'],
    },
    "rbc_visa": {
        "bank": "RBC Visa",
        "type": "credit_card",
        "fingerprints": ["RBC", "Royal Bank", "rbcroyalbank.com", "RBC Rewards"],
        "negative_fingerprints": ["chequing", "savings", "Day to Day"],
        "date_format": "MMM DD",
        "date_regex": r'((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2})',
        "transaction_regex": r'^((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[.\s]+\d{1,2}\s+)((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[.\s]+\d{1,2}\s+)(.+?)\s+(-?[\d,]+\.\d{2})(\s?CR)?\s*$',
        "columns": ["trans_date", "post_date", "description", "amount", "cr_flag"],
        "amount_logic": "cr_flag", "sort_order": "chronological", "continuation_regex": None,
        "noise_patterns": ["Previous Balance", "Payment - Thank You", "TOTAL"],
        "section_start": ["NEW ACTIVITY", "Transactions"],
        "section_end": ["Total", "Interest"],
        "tax_markers": {},
    },
    "rbc_chequing": {
        "bank": "RBC Chequing",
        "type": "chequing",
        "fingerprints": ["RBC", "Royal Bank", "Day to Day Banking", "Leo's Young Savers"],
        "negative_fingerprints": ["Visa", "Mastercard"],
        "date_format": "MMM DD",
        "date_regex": r'((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2})',
        "transaction_regex": r'^((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2})\s+(.+?)\s+([\d,]+\.\d{2})(\-?)\s+([\d,]+\.\d{2})\s*$',
        "columns": ["date", "description", "amount", "sign", "balance"],
        "amount_logic": "sign_suffix", "sort_order": "chronological", "continuation_regex": None,
        "noise_patterns": ["Opening Balance", "Closing Balance"],
        "section_start": None, "section_end": None, "tax_markers": {},
    },
    "bmo_mastercard": {
        "bank": "BMO Mastercard",
        "type": "credit_card",
        "fingerprints": ["BMO", "Bank of Montreal", "bmo.com", "BMO CashBack", "BMO AIR MILES"],
        "negative_fingerprints": ["chequing", "savings"],
        "date_format": "MMM. DD",
        "date_regex": r'((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\.?\s+\d{1,2})',
        "transaction_regex": r'^((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\.?\s+\d{1,2})\s+((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\.?\s+\d{1,2})\s+(.+?)\s+(-?[\d,]+\.\d{2})\s*$',
        "columns": ["trans_date", "post_date", "description", "amount"],
        "amount_logic": "signed", "sort_order": "chronological", "continuation_regex": None,
        "noise_patterns": ["Previous Statement Balance", "Payment", "TOTAL NEW ACTIVITY"],
        "section_start": ["NEW ACTIVITY"],
        "section_end": ["TOTAL NEW ACTIVITY", "Interest"],
        "tax_markers": {},
    },
    # NEW v3.3: BMO Chequing
    "bmo_chequing": {
        "bank": "BMO Chequing",
        "type": "chequing",
        "fingerprints": ["BMO", "Bank of Montreal", "BMO Bank", "BMO Debit Card", "BMO Chequing"],
        "negative_fingerprints": ["Mastercard", "Visa", "CashBack", "AIR MILES"],
        "date_format": "MM/DD/YYYY",
        "date_regex": r'(\d{2}/\d{2}/\d{4})',
        "transaction_regex": r'^(\d{2}/\d{2}/\d{4})\s+(.+?)\s+([\d,]+\.\d{2})?\s*\t?\s*([\d,]+\.\d{2})?\s+([\d,]+\.\d{2})\s*$',
        "columns": ["date", "description", "withdrawal", "deposit", "balance"],
        "amount_logic": "withdrawal_deposit", "sort_order": "chronological", "continuation_regex": None,
        "noise_patterns": ["Opening Balance", "Closing Balance", "Statement Period"],
        "section_start": None, "section_end": None, "tax_markers": {},
    },
    "scotia_visa": {
        "bank": "Scotiabank Visa",
        "type": "credit_card",
        "fingerprints": ["Scotiabank", "Scotia", "scotiabank.com", "SCENE", "TELESCOTIA"],
        "negative_fingerprints": ["chequing", "savings"],
        "date_format": "MMM DD",
        "date_regex": r'((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2})',
        "transaction_regex": r'^((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2})\s+((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2})\s+(.+?)\s+(-?[\d,]+\.\d{2})\s*$',
        "columns": ["trans_date", "post_date", "description", "amount"],
        "amount_logic": "signed", "sort_order": "chronological", "continuation_regex": None,
        "noise_patterns": ["Previous Balance", "Payment"],
        "section_start": ["TRANSACTIONS"],
        "section_end": ["TOTAL", "Interest"],
        "tax_markers": {},
    },
    # NEW v3.3: Scotiabank Chequing (merged date column — common source of regex failures)
    "scotia_chequing": {
        "bank": "Scotiabank Chequing",
        "type": "chequing",
        "fingerprints": ["Scotiabank", "Scotia", "ScotiaCard", "Scotia OnLine"],
        "negative_fingerprints": ["Visa", "Mastercard", "SCENE"],
        "date_format": "MMM DD, YYYY",
        "date_regex": r'((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2},\s+\d{4})',
        "transaction_regex": r'^((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2},\s+\d{4})\s+(.+?)\s+([\d,]+\.\d{2})\s*(DR|CR)?\s*([\d,]+\.\d{2})?\s*$',
        "columns": ["date", "description", "amount", "dr_cr_flag", "balance"],
        "amount_logic": "dr_cr_flag", "sort_order": "chronological", "continuation_regex": None,
        "noise_patterns": ["Opening Balance", "Closing Balance", "Previous Balance"],
        "section_start": None, "section_end": None, "tax_markers": {},
    },
    "amex": {
        "bank": "American Express",
        "type": "credit_card",
        "fingerprints": ["American Express", "AMEX", "Membership Rewards", "amex.ca"],
        "negative_fingerprints": [],
        "date_format": "DD MMM",
        "date_regex": r'(\d{1,2}\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec))',
        "transaction_regex": r'^(\d{1,2}\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec))\s+(.+?)\s+(-?\$?[\d,]+\.\d{2})\s*$',
        "columns": ["date", "description", "amount"],
        "amount_logic": "signed", "sort_order": "chronological", "continuation_regex": None,
        "noise_patterns": ["Previous Balance", "Payment Received", "TOTAL"],
        "section_start": ["New Charges"],
        "section_end": ["Total New Charges"],
        "tax_markers": {},
    },
    # NEW v3.3: Tangerine
    "tangerine": {
        "bank": "Tangerine",
        "type": "chequing",
        "fingerprints": ["Tangerine", "tangerine.ca", "ING Direct", "Tangerine Bank"],
        "negative_fingerprints": [],
        "date_format": "YYYY-MM-DD",
        "date_regex": r'(\d{4}-\d{2}-\d{2})',
        "transaction_regex": r'^(\d{4}-\d{2}-\d{2})\s+(.+?)\s+([\d,]+\.\d{2})?\s*\t?\s*([\d,]+\.\d{2})?\s*([\d,]+\.\d{2})?\s*$',
        "columns": ["date", "description", "debit_amt", "credit_amt", "balance"],
        "amount_logic": "debit_credit_cols", "sort_order": "chronological", "continuation_regex": None,
        "noise_patterns": ["Opening Balance", "Closing Balance"],
        "section_start": None, "section_end": None, "tax_markers": {},
    },
    # NEW v3.3: Simplii Financial
    "simplii": {
        "bank": "Simplii Financial",
        "type": "chequing",
        "fingerprints": ["Simplii", "simplii.com", "Simplii Financial", "PC Financial"],
        "negative_fingerprints": [],
        "date_format": "MMM DD, YYYY",
        "date_regex": r'((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2},\s+\d{4})',
        "transaction_regex": r'^((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2},\s+\d{4})\s+(.+?)\s+(-?[\d,]+\.\d{2})\s+([\d,]+\.\d{2})\s*$',
        "columns": ["date", "description", "amount", "balance"],
        "amount_logic": "signed", "sort_order": "chronological", "continuation_regex": None,
        "noise_patterns": ["Opening Balance", "Closing Balance"],
        "section_start": None, "section_end": None, "tax_markers": {},
    },
    # NEW v3.3: National Bank
    "national_bank": {
        "bank": "National Bank",
        "type": "credit_card",
        "fingerprints": ["National Bank", "BNC", "Banque Nationale", "bnc.ca", "NBC"],
        "negative_fingerprints": [],
        "date_format": "DD MMM YYYY",
        "date_regex": r'(\d{1,2}\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{4})',
        "transaction_regex": r'^(\d{1,2}\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{4})\s+(.+?)\s+(-?[\d,]+\.\d{2})\s*$',
        "columns": ["date", "description", "amount"],
        "amount_logic": "signed", "sort_order": "chronological", "continuation_regex": None,
        "noise_patterns": ["Solde precedent", "Previous Balance", "Payment", "Paiement"],
        "section_start": ["Transactions", "Achats"],
        "section_end": ["Total", "Interets"],
        "tax_markers": {},
    },
    # NEW v3.3: Desjardins
    "desjardins": {
        "bank": "Desjardins",
        "type": "credit_card",
        "fingerprints": ["Desjardins", "desjardins.com", "Caisse Desjardins", "Visa Desjardins", "Opus", "BONUSDOLLARS"],
        "negative_fingerprints": [],
        "date_format": "YYYY-MM-DD",
        "date_regex": r'(\d{4}-\d{2}-\d{2})',
        "transaction_regex": r'^(\d{4}-\d{2}-\d{2})\s+(\d{4}-\d{2}-\d{2})\s+(.+?)\s+(-?[\d,]+\.\d{2})\s*$',
        "columns": ["trans_date", "post_date", "description", "amount"],
        "amount_logic": "signed", "sort_order": "chronological", "continuation_regex": None,
        "noise_patterns": ["Solde precedent", "Paiement recu", "Nouveau solde"],
        "section_start": ["Transactions"],
        "section_end": ["Total"],
        "tax_markers": {},
    },
    # ── TD Mastercard (distinct from TD Visa) ──────────────────────────────
    "td_mastercard": {
        "bank": "TD Mastercard",
        "type": "credit_card",
        "fingerprints": ["TD Cash Back Mastercard", "TD Rewards Mastercard", "TD Mastercard", "TD Infinite Privilege Mastercard", "td.com/mastercard"],
        "negative_fingerprints": ["Visa", "EasyWeb", "Day to Day"],
        "date_format": "MMM DD",
        "date_regex": r'((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2})',
        "transaction_regex": r'^((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2})\s+((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2})\s+(.+?)\s+(-?[\d,]+\.\d{2})\s*$',
        "columns": ["trans_date", "post_date", "description", "amount"],
        "amount_logic": "signed", "sort_order": "chronological", "continuation_regex": None,
        "noise_patterns": ["Previous Balance", "Payment - Thank You", "NEW BALANCE", "MINIMUM PAYMENT", "INTEREST CHARGED"],
        "section_start": ["NEW CHARGES", "TRANSACTIONS", "Activity since"],
        "section_end": ["TOTAL NEW CHARGES", "Minimum Payment", "Interest Charges"],
        "tax_markers": {},
    },
    # ── Scotiabank Mastercard (Momentum, World Elite) ──────────────────────
    "scotia_mastercard": {
        "bank": "Scotiabank Mastercard",
        "type": "credit_card",
        "fingerprints": ["Scotia Momentum", "World Elite Mastercard", "Scotiabank Mastercard", "Scotia Visa Infinite Privilege"],
        "negative_fingerprints": ["chequing", "savings", "SCENE Visa"],
        "date_format": "MMM DD",
        "date_regex": r'((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2})',
        "transaction_regex": r'^((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2})\s+((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2})\s+(.+?)\s+(-?[\d,]+\.\d{2})\s*$',
        "columns": ["trans_date", "post_date", "description", "amount"],
        "amount_logic": "signed", "sort_order": "chronological", "continuation_regex": None,
        "noise_patterns": ["Previous Balance", "Payment", "New Balance", "Minimum Payment", "Interest"],
        "section_start": ["TRANSACTIONS", "New charges"],
        "section_end": ["TOTAL", "Interest"],
        "tax_markers": {},
    },
    # ── PC Financial Mastercard (President's Choice / Loblaws) ─────────────
    "pc_mastercard": {
        "bank": "PC Financial Mastercard",
        "type": "credit_card",
        "fingerprints": ["PC Financial", "PC Mastercard", "President's Choice Financial", "PCF", "pcfinancial.ca", "PC Optimum", "PC Points"],
        "negative_fingerprints": [],
        "date_format": "MMM DD",
        "date_regex": r'((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2})',
        "transaction_regex": r'^((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2})\s+((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2})\s+(.+?)\s+(-?[\d,]+\.\d{2})\s*$',
        "columns": ["trans_date", "post_date", "description", "amount"],
        "amount_logic": "signed", "sort_order": "chronological", "continuation_regex": None,
        "noise_patterns": ["Previous Balance", "Payment Received", "TOTAL", "Minimum Payment", "New Balance", "Available Credit"],
        "section_start": ["Transactions", "New Charges"],
        "section_end": ["Total", "Interest"],
        "tax_markers": {},
    },
    # ── MBNA Mastercard (TD-administered — True Line, Smart Cash, Rewards) ─
    "mbna_mastercard": {
        "bank": "MBNA Mastercard",
        "type": "credit_card",
        "fingerprints": ["MBNA", "mbna.ca", "True Line", "MBNA Rewards", "Smart Cash", "MBNA World Elite"],
        "negative_fingerprints": [],
        "date_format": "MMM DD",
        "date_regex": r'((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2})',
        "transaction_regex": r'^((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2})\s+((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2})\s+(.+?)\s+(-?[\d,]+\.\d{2})\s*$',
        "columns": ["trans_date", "post_date", "description", "amount"],
        "amount_logic": "signed", "sort_order": "chronological", "continuation_regex": None,
        "noise_patterns": ["Previous Balance", "PAYMENT - THANK YOU", "NEW BALANCE", "MINIMUM PAYMENT", "Interest Charges"],
        "section_start": ["NEW TRANSACTIONS", "Transactions"],
        "section_end": ["TOTAL NEW TRANSACTIONS", "Interest"],
        "tax_markers": {},
    },
    # ── Capital One Mastercard (incl. Costco Mastercard Canada) ───────────
    "capital_one_mc": {
        "bank": "Capital One Mastercard",
        "type": "credit_card",
        "fingerprints": ["Capital One", "capitalone.ca", "Costco Mastercard", "Costco Anywhere Visa", "Aspire Cash"],
        "negative_fingerprints": ["Triangle", "Canadian Tire Bank"],
        "date_format": "MMM DD",
        "date_regex": r'((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2})',
        "transaction_regex": r'^((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2})\s+((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2})\s+(.+?)\s+(-?[\d,]+\.\d{2})\s*$',
        "columns": ["trans_date", "post_date", "description", "amount"],
        "amount_logic": "signed", "sort_order": "chronological", "continuation_regex": None,
        "noise_patterns": ["Previous Balance", "Payment Received", "New Balance", "Minimum Payment Due", "Available Credit"],
        "section_start": ["Transactions"],
        "section_end": ["Total", "Interest"],
        "tax_markers": {},
    },
    # ── Rogers Mastercard / Rogers World Elite / Fido Mastercard ──────────
    "rogers_mastercard": {
        "bank": "Rogers Mastercard",
        "type": "credit_card",
        "fingerprints": ["Rogers Mastercard", "Rogers World Elite", "Rogers Bank", "rogersbank.com", "Fido Mastercard"],
        "negative_fingerprints": [],
        "date_format": "MMM DD",
        "date_regex": r'((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2})',
        "transaction_regex": r'^((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2})\s+((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2})\s+(.+?)\s+(-?[\d,]+\.\d{2})\s*$',
        "columns": ["trans_date", "post_date", "description", "amount"],
        "amount_logic": "signed", "sort_order": "chronological", "continuation_regex": None,
        "noise_patterns": ["Previous Balance", "Payment", "New Balance", "Minimum Payment"],
        "section_start": ["Transactions", "New Activity"],
        "section_end": ["Total", "Interest"],
        "tax_markers": {},
    },
    # ── HSBC Canada (Visa / Mastercard) ───────────────────────────────────
    "hsbc_canada": {
        "bank": "HSBC Canada",
        "type": "credit_card",
        "fingerprints": ["HSBC", "hsbc.ca", "HSBC Premier", "HSBC Advance", "HSBC World Elite", "HSBC +Rewards"],
        "negative_fingerprints": [],
        "date_format": "DD MMM",
        "date_regex": r'(\d{1,2}\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec))',
        "transaction_regex": r'^(\d{1,2}\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec))\s+(\d{1,2}\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec))\s+(.+?)\s+(-?[\d,]+\.\d{2})\s*$',
        "columns": ["trans_date", "post_date", "description", "amount"],
        "amount_logic": "signed", "sort_order": "chronological", "continuation_regex": None,
        "noise_patterns": ["Previous Balance", "PAYMENT", "New Balance", "Minimum Payment"],
        "section_start": ["Transactions"],
        "section_end": ["Total", "Interest"],
        "tax_markers": {},
    },
    # ── ATB Financial (Alberta Treasury Branches) ─────────────────────────
    "atb_financial": {
        "bank": "ATB Financial",
        "type": "chequing",
        "fingerprints": ["ATB Financial", "ATB Business", "atb.com", "ATB Mastercard", "Alberta Treasury Branches"],
        "negative_fingerprints": [],
        "date_format": "MMM DD, YYYY",
        "date_regex": r'((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2},\s+\d{4})',
        "transaction_regex": r'^((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2},\s+\d{4})\s+(.+?)\s+(-?[\d,]+\.\d{2})\s+([\d,]+\.\d{2})\s*$',
        "columns": ["date", "description", "amount", "balance"],
        "amount_logic": "signed", "sort_order": "chronological", "continuation_regex": None,
        "noise_patterns": ["Opening Balance", "Closing Balance"],
        "section_start": None, "section_end": None, "tax_markers": {},
    },
    # ── Meridian Credit Union (Ontario) ───────────────────────────────────
    "meridian_cu": {
        "bank": "Meridian Credit Union",
        "type": "chequing",
        "fingerprints": ["Meridian", "meridiancu.ca", "Meridian Credit Union", "Meridian Visa", "Meridian Business"],
        "negative_fingerprints": [],
        "date_format": "MMM DD, YYYY",
        "date_regex": r'((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2},\s+\d{4})',
        "transaction_regex": r'^((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2},\s+\d{4})\s+(.+?)\s+([\d,]+\.\d{2})?\s*(CR|DR)?\s+([\d,]+\.\d{2})\s*$',
        "columns": ["date", "description", "amount", "dr_cr_flag", "balance"],
        "amount_logic": "dr_cr_flag", "sort_order": "chronological", "continuation_regex": None,
        "noise_patterns": ["Opening Balance", "Closing Balance"],
        "section_start": None, "section_end": None, "tax_markers": {},
    },
    # ── Coast Capital Savings (BC credit union) ───────────────────────────
    "coast_capital": {
        "bank": "Coast Capital Savings",
        "type": "chequing",
        "fingerprints": ["Coast Capital", "coastcapitalsavings.com", "Coast Capital Savings", "Coast Capital Visa"],
        "negative_fingerprints": [],
        "date_format": "MMM DD, YYYY",
        "date_regex": r'((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2},\s+\d{4})',
        "transaction_regex": r'^((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2},\s+\d{4})\s+(.+?)\s+([\d,]+\.\d{2})?\s*(CR)?\s+([\d,]+\.\d{2})\s*$',
        "columns": ["date", "description", "amount", "cr_flag", "balance"],
        "amount_logic": "cr_flag", "sort_order": "chronological", "continuation_regex": None,
        "noise_patterns": ["Opening Balance", "Closing Balance", "Service Charge"],
        "section_start": None, "section_end": None, "tax_markers": {},
    },
    # ── Neo Financial / Walmart Rewards Mastercard ─────────────────────────
    "neo_financial": {
        "bank": "Neo Financial",
        "type": "credit_card",
        "fingerprints": ["Neo Financial", "neofinancial.com", "Neo Mastercard", "Walmart Rewards Mastercard", "Neo Money", "Neo Secured"],
        "negative_fingerprints": [],
        "date_format": "YYYY-MM-DD",
        "date_regex": r'(\d{4}-\d{2}-\d{2})',
        "transaction_regex": r'^(\d{4}-\d{2}-\d{2})\s+(\d{4}-\d{2}-\d{2})\s+(.+?)\s+(-?[\d,]+\.\d{2})\s*$',
        "columns": ["trans_date", "post_date", "description", "amount"],
        "amount_logic": "signed", "sort_order": "chronological", "continuation_regex": None,
        "noise_patterns": ["Previous Balance", "Payment", "New Balance", "Minimum Payment"],
        "section_start": ["Transactions"],
        "section_end": ["Total", "Interest"],
        "tax_markers": {},
    },
    # ── EQ Bank / Equitable Bank ──────────────────────────────────────────
    "eq_bank": {
        "bank": "EQ Bank",
        "type": "chequing",
        "fingerprints": ["EQ Bank", "eqbank.ca", "Equitable Bank", "EQ Savings Plus"],
        "negative_fingerprints": [],
        "date_format": "YYYY-MM-DD",
        "date_regex": r'(\d{4}-\d{2}-\d{2})',
        "transaction_regex": r'^(\d{4}-\d{2}-\d{2})\s+(.+?)\s+(-?[\d,]+\.\d{2})\s+([\d,]+\.\d{2})\s*$',
        "columns": ["date", "description", "amount", "balance"],
        "amount_logic": "signed", "sort_order": "chronological", "continuation_regex": None,
        "noise_patterns": ["Opening Balance", "Closing Balance"],
        "section_start": None, "section_end": None, "tax_markers": {},
    },
    # ── CIBC Business Banking ─────────────────────────────────────────────
    "cibc_business": {
        "bank": "CIBC Business Banking",
        "type": "chequing",
        "fingerprints": ["CIBC Business", "CIBC SmartBusiness", "CIBC Business Operating", "CIBC Business Plus"],
        "negative_fingerprints": ["Visa", "Aventura", "Dividend", "Aeroplan"],
        "date_format": "MMM DD, YYYY",
        "date_regex": r'((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2},\s+\d{4})',
        "transaction_regex": r'^((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2},\s+\d{4})\s+(.+?)\s+([\d,]+\.\d{2})?\s*(DR|CR)?\s*([\d,]+\.\d{2})?\s*$',
        "columns": ["date", "description", "amount", "dr_cr_flag", "balance"],
        "amount_logic": "dr_cr_flag", "sort_order": "chronological", "continuation_regex": None,
        "noise_patterns": ["Opening Balance", "Closing Balance", "Service Charges"],
        "section_start": None, "section_end": None, "tax_markers": {},
    },
    # ── RBC Business Operating Account ────────────────────────────────────
    "rbc_business": {
        "bank": "RBC Business Account",
        "type": "chequing",
        "fingerprints": ["RBC Business", "Royal Bank Business", "RBC Business Operating", "RBC Business Select"],
        "negative_fingerprints": ["Visa", "Mastercard", "RBC Rewards"],
        "date_format": "MMM DD",
        "date_regex": r'((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2})',
        "transaction_regex": r'^((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2})\s+(.+?)\s+([\d,]+\.\d{2})(\-?)\s+([\d,]+\.\d{2})\s*$',
        "columns": ["date", "description", "amount", "sign", "balance"],
        "amount_logic": "sign_suffix", "sort_order": "chronological", "continuation_regex": None,
        "noise_patterns": ["Opening Balance", "Closing Balance", "Service Charge"],
        "section_start": None, "section_end": None, "tax_markers": {},
    },
    # ── BMO Business Account ──────────────────────────────────────────────
    "bmo_business": {
        "bank": "BMO Business Account",
        "type": "chequing",
        "fingerprints": ["BMO Business", "BMO Business Select", "BMO Commercial", "BMO Bank of Montreal Business"],
        "negative_fingerprints": ["Mastercard", "CashBack", "AIR MILES"],
        "date_format": "MM/DD/YYYY",
        "date_regex": r'(\d{2}/\d{2}/\d{4})',
        "transaction_regex": r'^(\d{2}/\d{2}/\d{4})\s+(.+?)\s+([\d,]+\.\d{2})?\s*\t?\s*([\d,]+\.\d{2})?\s+([\d,]+\.\d{2})\s*$',
        "columns": ["date", "description", "withdrawal", "deposit", "balance"],
        "amount_logic": "withdrawal_deposit", "sort_order": "chronological", "continuation_regex": None,
        "noise_patterns": ["Opening Balance", "Closing Balance"],
        "section_start": None, "section_end": None, "tax_markers": {},
    },
    # ── Laurentian Bank / LBC Digital ─────────────────────────────────────
    "laurentian": {
        "bank": "Laurentian Bank",
        "type": "credit_card",
        "fingerprints": ["Laurentian", "Banque Laurentienne", "laurentianbank.ca", "lbc.ca", "LBC Visa", "LBC Digital"],
        "negative_fingerprints": [],
        "date_format": "DD/MM/YYYY",
        "date_regex": r'(\d{2}/\d{2}/\d{4})',
        "transaction_regex": r'^(\d{2}/\d{2}/\d{4})\s+(\d{2}/\d{2}/\d{4})\s+(.+?)\s+(-?[\d,]+\.\d{2})\s*$',
        "columns": ["trans_date", "post_date", "description", "amount"],
        "amount_logic": "signed", "sort_order": "chronological", "continuation_regex": None,
        "noise_patterns": ["Solde precedent", "Paiement", "Nouveau solde", "Previous Balance", "Payment"],
        "section_start": ["Transactions"],
        "section_end": ["Total", "Interet"],
        "tax_markers": {},
    },
}


# ═══════════════════════════════════════════════════════════════════
# INDUSTRY PROFILES (v3.3)
# ═══════════════════════════════════════════════════════════════════

INDUSTRY_PROFILES = {
    "Construction/Trades": {
        "subcontractor_threshold": 500,
        "t2125_remaps": {},
        "personal_to_shareholder_loan": True,
    },
    "Retail": {
        "subcontractor_threshold": 500,
        "t2125_remaps": {"Materials & Supplies": "Cost of Goods"},
        "personal_to_shareholder_loan": False,
        "revenue_keywords": [r'SQUARE|SHOPIFY|STRIPE.*PAYOUT|PAYPAL.*TRANSFER|ETSY.*DEPOSIT'],
    },
    "Restaurant/Food": {
        "subcontractor_threshold": 500,
        "t2125_remaps": {"Materials & Supplies": "Cost of Goods"},
        "cogs_merchants": [r'SYSCO|GORDON FOOD|GFS|US FOODS|FRESH DIRECT|RESTAURANT DEPOT|METRO WHOLESALE'],
    },
    "Professional Services": {
        "subcontractor_threshold": 500,
        "t2125_remaps": {},
        "meals_always_50": True,
    },
    "Rental Properties": {
        "subcontractor_threshold": 500,
        "t2125_remaps": {"Materials & Supplies": "Repairs & Maintenance"},
        "schedule_e": True,
    },
    "Other": {
        "subcontractor_threshold": 500,
        "t2125_remaps": {},
    },
}


# ═══════════════════════════════════════════════════════════════════
# CCA CLASS ASSIGNMENT (v3.3)
# ═══════════════════════════════════════════════════════════════════

def get_cca_class(description, amount):
    """
    v3.3: Determine correct CCA class.
    Class 12 = small tools < $500 (100% deduction)
    Class 50 = computers/software (55%)
    Class 10 = vehicles (30%)
    Class 8  = general equipment (20%)
    Returns (class_number, label_string).
    """
    desc = (description or "").upper()
    if re.search(r'COMPUTER|LAPTOP|TABLET|IPAD|MACBOOK|SERVER|SOFTWARE|SAAS|MICROSOFT|ADOBE', desc):
        return "50", "Class 50 (55%) — Computer Hardware/Software"
    if re.search(r'VEHICLE|AUTO|CAR|TRUCK|VAN|SUV|LEASE.*CAR', desc):
        return "10", "Class 10 (30%) — Automotive"
    if re.search(r'TOOL|DRILL|SAW|GRINDER|NAILER|COMPRESSOR|GENERATOR|HAMMER|WRENCH', desc):
        if amount < 500:
            return "12", "Class 12 (100%) — Small Tools (< $500)"
        return "8", "Class 8 (20%) — Power Tools/Equipment (> $500)"
    if re.search(r'FURNITURE|DESK|CHAIR|FILING|SHELF|SHELVING|IKEA', desc):
        return "8", "Class 8 (20%) — Office Furniture"
    if re.search(r'RENO|RENOVATION|FLOORING|DRYWALL|FRAMING|ROOFING|PLUMBING|ELECTRICAL', desc):
        return "8", "Class 8 (20%) — Leasehold Improvements"
    return "8", "Class 8 (20%) — Equipment (review with accountant)"


# ═══════════════════════════════════════════════════════════════════
# BANK DETECTION
# ═══════════════════════════════════════════════════════════════════

def detect_bank(raw_text, first_500_chars=None):
    if not first_500_chars:
        first_500_chars = raw_text[:2000].lower()
    else:
        first_500_chars = first_500_chars.lower()
    full_lower = raw_text.lower()
    best_match = None
    best_score = 0
    for key, schema in BANK_SCHEMAS.items():
        score = 0
        for fp in schema["fingerprints"]:
            if fp.lower() in first_500_chars:
                score += 10
            elif fp.lower() in full_lower:
                score += 3
        for nfp in schema.get("negative_fingerprints", []):
            if nfp.lower() in first_500_chars:
                score -= 15
        if score > best_score:
            best_score = score
            best_match = key
    if best_match and best_score >= 10:
        return best_match, BANK_SCHEMAS[best_match], min(100, best_score * 5)
    return None, None, 0


# ═══════════════════════════════════════════════════════════════════
# SCHEMA-BASED PARSER
# ═══════════════════════════════════════════════════════════════════

def parse_with_schema(raw_text, schema):
    lines = raw_text.split('\n')
    transactions = []
    in_section = schema.get("section_start") is None

    for line in lines:
        line = line.strip()
        if not line:
            continue
        if not in_section and schema.get("section_start"):
            for marker in schema["section_start"]:
                if marker.lower() in line.lower():
                    in_section = True
                    break
            if not in_section:
                continue
        if in_section and schema.get("section_end"):
            for marker in schema["section_end"]:
                if marker.lower() in line.lower():
                    in_section = False
                    break
        skip = any(n.lower() in line.lower() for n in schema.get("noise_patterns", []))
        if skip:
            continue

        m = re.match(schema["transaction_regex"], line)
        if not m:
            continue

        groups = m.groups()
        txn = {"raw_line": line}
        cols = schema["columns"]
        for idx, col in enumerate(cols):
            txn[col] = groups[idx].strip() if idx < len(groups) and groups[idx] else ""

        # Use POSTING DATE for cycle assignment — it is the authoritative date
        # that determines which statement cycle a transaction belongs to.
        # trans_date (transaction date) can fall 1-3 days before the cycle boundary
        # causing mis-assignment to the wrong monthly statement.
        # If schema has both trans_date and post_date, prefer post_date.
        if "post_date" in txn and txn.get("post_date", "").strip():
            txn["date"] = txn["post_date"]
            txn["trans_date_original"] = txn.get("trans_date", "")  # preserve for reference
        elif "trans_date" in txn:
            txn["date"] = txn["trans_date"]
        elif "date" not in txn:
            txn["date"] = ""

        logic = schema["amount_logic"]

        if logic == "debit_credit_cols":
            try:
                d = float(txn.get("debit_amt", "0").replace(",", "")) if txn.get("debit_amt") else 0
            except (ValueError, AttributeError):
                d = 0
            try:
                c = float(txn.get("credit_amt", "0").replace(",", "")) if txn.get("credit_amt") else 0
            except (ValueError, AttributeError):
                c = 0
            txn["debit"] = d; txn["credit"] = c; txn["_raw_amount"] = d or c

        elif logic == "withdrawal_deposit":
            try:
                w = float(txn.get("withdrawal", "0").replace(",", "")) if txn.get("withdrawal") else 0
            except (ValueError, AttributeError):
                w = 0
            try:
                dep = float(txn.get("deposit", "0").replace(",", "")) if txn.get("deposit") else 0
            except (ValueError, AttributeError):
                dep = 0
            txn["debit"] = w; txn["credit"] = dep; txn["_raw_amount"] = w or dep

        elif logic == "dr_cr_flag":
            amt_str = txn.get("amount", "0").replace(",", "").replace("$", "")
            try:
                amount = float(amt_str)
            except ValueError:
                amount = 0
            flag = txn.get("dr_cr_flag", "").upper()
            txn["debit"] = abs(amount) if flag != "CR" else 0
            txn["credit"] = abs(amount) if flag == "CR" else 0
            txn["_raw_amount"] = abs(amount)

        else:
            amt_str = txn.get("amount", "0").replace(",", "").replace("$", "")
            try:
                amount = float(amt_str)
            except ValueError:
                amount = 0

            if logic == "signed":
                txn["debit"] = abs(amount) if amount >= 0 else 0
                txn["credit"] = abs(amount) if amount < 0 else 0
            elif logic == "cr_flag":
                cr = txn.get("cr_flag", "")
                txn["debit"] = abs(amount) if "CR" not in (cr or "").upper() else 0
                txn["credit"] = abs(amount) if "CR" in (cr or "").upper() else 0
            elif logic == "sign_suffix":
                sign = txn.get("sign", "")
                txn["debit"] = abs(amount) if "-" in sign else 0
                txn["credit"] = abs(amount) if "-" not in sign else 0
            elif logic == "balance_change":
                txn["debit"] = abs(amount); txn["credit"] = 0
                txn["_needs_balance_resolution"] = True
            txn["_raw_amount"] = abs(amount)

        tax_flag = txn.get("tax_flag", "")
        if tax_flag and schema.get("tax_markers"):
            txn["tax_note"] = schema["tax_markers"].get(tax_flag, "")

        txn["description"] = re.sub(r'\s+', ' ', txn.get("description", "")).strip()

        bal_key = "balance"
        if bal_key in txn:
            try:
                txn["balance"] = float(str(txn[bal_key]).replace(",", ""))
            except (ValueError, AttributeError):
                txn["balance"] = 0
        else:
            txn["balance"] = 0

        transactions.append(txn)

    if schema["amount_logic"] == "balance_change" and len(transactions) > 1:
        _resolve_balance_changes(transactions, schema.get("sort_order", "chronological"))

    return transactions


def _resolve_balance_changes(transactions, sort_order):
    if sort_order == "newest_first":
        for idx in range(len(transactions)):
            t = transactions[idx]
            if not t.get("_needs_balance_resolution"):
                continue
            prev_bal = transactions[idx + 1]["balance"] if idx + 1 < len(transactions) else t["balance"]
            change = t["balance"] - prev_bal
            amt = t["_raw_amount"]
            if abs(change + amt) < 1.0:
                t["debit"] = amt; t["credit"] = 0
            elif abs(change - amt) < 1.0:
                t["debit"] = 0; t["credit"] = amt
            else:
                desc = t.get("description", "").upper()
                if any(kw in desc for kw in ["DEPOSIT", "PAYROLL", "CREDIT", "REFUND", "TRANSFER IN"]):
                    t["debit"] = 0; t["credit"] = amt
                else:
                    t["debit"] = amt; t["credit"] = 0
    else:
        for idx in range(len(transactions)):
            t = transactions[idx]
            if not t.get("_needs_balance_resolution"):
                continue
            prev_bal = transactions[idx - 1]["balance"] if idx > 0 else t["balance"]
            change = t["balance"] - prev_bal
            amt = t["_raw_amount"]
            if abs(change + amt) < 1.0:
                t["debit"] = amt; t["credit"] = 0
            elif abs(change - amt) < 1.0:
                t["debit"] = 0; t["credit"] = amt
            else:
                t["debit"] = amt; t["credit"] = 0


# ═══════════════════════════════════════════════════════════════════
# CANADIAN TAX HELPERS
# ═══════════════════════════════════════════════════════════════════

PROVINCE_TAX_RATES = {
    "ON": {"HST": 13, "GST": 5, "PST": 8},
    "BC": {"GST": 5, "PST": 7},
    "AB": {"GST": 5, "PST": 0},
    "SK": {"GST": 5, "PST": 6},
    "MB": {"GST": 5, "PST": 7},
    "QC": {"GST": 5, "QST": 9.975},
    "NS": {"HST": 15, "GST": 5, "PST": 10},
    "NB": {"HST": 15, "GST": 5, "PST": 10},
    "NL": {"HST": 15, "GST": 5, "PST": 10},
    "PE": {"HST": 15, "GST": 5, "PST": 10},
}


def get_itc_rate_fraction(province):
    """Return ITC recovery rate as fraction (e.g. ON = 13/113 = 0.11504)."""
    rates = PROVINCE_TAX_RATES.get(province, PROVINCE_TAX_RATES["ON"])
    if "HST" in rates:
        r = rates["HST"] / 100
        return r / (1 + r)
    return 0.05 / 1.05  # GST only


def detect_province_from_description(description):
    """
    v3.3: Override province from merchant description suffix.
    'STORE NAME AB' → 'AB'. Critical for correct ITC on inter-provincial purchases.
    """
    if not description:
        return None
    matches = re.findall(r'\b(ON|QC|BC|AB|SK|MB|NS|NB|NL|PE|NT|YT|NU)\b', description.upper())
    return matches[-1] if matches else None


def detect_province_from_text(text):
    header = text[:1000].upper()
    province_patterns = {
        "ON": [r'\bON\b\s+[A-Z]\d[A-Z]', r'ONTARIO', r'TORONTO', r'OTTAWA', r'MISSISSAUGA', r'BRAMPTON'],
        "BC": [r'\bBC\b\s+[A-Z]\d[A-Z]', r'BRITISH COLUMBIA', r'VANCOUVER', r'VICTORIA'],
        "AB": [r'\bAB\b\s+[A-Z]\d[A-Z]', r'ALBERTA', r'CALGARY', r'EDMONTON'],
        "QC": [r'\bQC\b\s+[A-Z]\d[A-Z]', r'QUEBEC', r'MONTREAL', r'LAVAL'],
        "SK": [r'\bSK\b\s+[A-Z]\d[A-Z]', r'SASKATCHEWAN', r'REGINA', r'SASKATOON'],
        "MB": [r'\bMB\b\s+[A-Z]\d[A-Z]', r'MANITOBA', r'WINNIPEG'],
        "NS": [r'\bNS\b\s+[A-Z]\d[A-Z]', r'NOVA SCOTIA', r'HALIFAX'],
        "NB": [r'\bNB\b\s+[A-Z]\d[A-Z]', r'NEW BRUNSWICK'],
        "NL": [r'\bNL\b\s+[A-Z]\d[A-Z]', r'NEWFOUNDLAND'],
        "PE": [r'\bPE\b\s+[A-Z]\d[A-Z]', r'PRINCE EDWARD'],
    }
    for prov, patterns in province_patterns.items():
        for pat in patterns:
            if re.search(pat, header):
                return prov
    return "ON"


# ═══════════════════════════════════════════════════════════════════
# MERCHANT NORMALIZATION (v3.3)
# ═══════════════════════════════════════════════════════════════════

MERCHANT_NORMALIZATIONS = [
    (r'AMZN[\s*]+\w*|AMAZON[\s.*]+\w*|AMZ\*', 'AMAZON'),
    (r'TIM HORTON[S]?|TIMS\s', 'TIM HORTONS'),
    (r'MCDONALD[S]?|MCDO\b|MCD\s', "MCDONALD'S"),
    (r'GOOGLE\s*\*[\w\s]+|GOOGLE ADS|GOOGLE CLOUD|GOOGLE WORKSPACE', 'GOOGLE'),
    (r'SHOPIFY[\s*]+\w*', 'SHOPIFY'),
    (r'PAYPAL[\s*]+\w*|PP\*', 'PAYPAL'),
    (r'FACEBOOK|FACEBK|META ADS|FB ADS|INSTAGRAM ADS', 'META / FACEBOOK ADS'),
    (r'STARBUCKS[\s*]+\w*|SBUX', 'STARBUCKS'),
    (r'HOME DEPOT[\s#]*\d*', 'HOME DEPOT'),
    (r'CANADIAN TIRE[\s#]*\d*|CT\s+#?\d+', 'CANADIAN TIRE'),
    (r'ROGERS[\s*]+\w*', 'ROGERS'),
    (r'BELL CANADA[\s*]*\w*|BELL\s+MOBILITY', 'BELL CANADA'),
    (r'TELUS[\s*]+\w*', 'TELUS'),
    (r'UBER\s*EATS[\s*]*\w*', 'UBER EATS'),
    (r'UBER[\s*]+(?!EATS)\w*', 'UBER'),
    (r'PETRO[\s-]?CAN[\w\s]*', 'PETRO-CANADA'),
    (r'ESSO[\s#]*\d*', 'ESSO'),
    (r'COSTCO\s+GAS[\s#]*\w*', 'COSTCO GAS'),
    (r'COSTCO\s+WHOLESALE[\s#]*\w*', 'COSTCO WHOLESALE'),
    (r'COSTCO[\s*]+\w*', 'COSTCO'),
    (r'WALMART[\s*]+\w*|WAL-MART[\s*]+\w*', 'WALMART'),
    (r'INTERAC\s+E-?TFR|E-TRANSFER\b|ETRANSFER\b', 'E-TRANSFER'),
]


def normalize_merchant(description):
    """
    v3.3 Merchant Normalization.
    Returns (normalized_name, was_changed).
    """
    if not description:
        return description, False
    original = description
    desc = description.upper().strip()

    bank_prefixes = [
        r'^POS\s+(?:PURCHASE|REFUND|RETURN)?\s*[-–]?\s*',
        r'^INTERAC\s+(?:PURCHASE|E-TRANSFER)?\s*[-–]?\s*',
        r'^CONTACTLESS\s+(?:PURCHASE|PMT)?\s*[-–]?\s*',
        r'^VISA\s+(?:PURCHASE|DEBIT)?\s*[-–]?\s*',
        r'^MC\s+(?:PURCHASE|DEBIT)?\s*[-–]?\s*',
        r'^PAD\s*[-–]?\s*',
        r'^PREAUTHORIZED\s+(?:DEBIT|PAYMENT|PMT)?\s*[-–]?\s*',
        r'^RECURRING\s+',
        r'^ONLINE\s+(?:PURCHASE|PMT)?\s*[-–]?\s*',
        r'^WWW\.\s*',
    ]
    for p in bank_prefixes:
        desc = re.sub(p, '', desc)

    desc = re.sub(r'\s+\d{8,15}\s*$', '', desc)
    desc = re.sub(r'\s+[A-Z\s]{2,15}\s+(?:ON|QC|BC|AB|SK|MB|NS|NB|PE|NL|NT|YT|NU)\s*(?:CA|CAN)?$', '', desc)
    desc = re.sub(r'\s+(?:ON|QC|BC|AB|SK|MB|NS|NB|PE|NL|NT|YT|NU)\s*(?:CA|CAN)?$', '', desc)
    desc = re.sub(r'\s+(?:CA|CAN|US|USA)\s*$', '', desc)
    desc = re.sub(r'\s*#\s*\d+', '', desc)
    desc = re.sub(r'\s+\d{3,6}\s*$', '', desc)
    desc = re.sub(r'\b[A-Z]{2,3}\d{6,}\b', '', desc)

    for pattern, canonical in MERCHANT_NORMALIZATIONS:
        if re.search(pattern, desc):
            desc = canonical
            break

    desc = desc.strip(' -–—*')
    was_changed = desc != original.upper().strip()
    return desc, was_changed


# ═══════════════════════════════════════════════════════════════════
# SHAREHOLDER LOAN DETECTION (v3.3)
# ═══════════════════════════════════════════════════════════════════

PERSONAL_KEYWORDS = re.compile(
    r'(?i)(pharmacy|rexall|shoppers drug|london drugs|jean coutu|'
    r'grocery|loblaws|no frills|metro\b|sobeys|freshco|food basics|'
    r'netflix|spotify|disney|apple music|crave|amazon prime|'
    r'cinema|cineplex|bowling|gym|goodlife|fitness|yoga|spa|'
    r'lcbo|beer store|wine rack|cannabis|ocs\b|'
    r'zara|h&m|gap\b|winners|marshalls|nordstrom|lululemon|'
    r'toy|baby|child|daycare|pet supply|veterinary|vet\b|'
    r'vacation|resort|waterpark|theme park)')


def detect_shareholder_loan(description, category, business_structure="Sole Proprietor"):
    """
    v3.3: For corporations, personal expenses become 'Shareholder Loan (Debit)'
    instead of 'Owner Draw / Personal'.
    """
    if business_structure not in ("Corporation", "Inc.", "Ltd."):
        return category
    if category == "Owner Draw / Personal" or PERSONAL_KEYWORDS.search(description or ""):
        return "Shareholder Loan (Debit)"
    return category


# ═══════════════════════════════════════════════════════════════════
# T5018 SUBCONTRACTOR DETECTION (v3.3)
# ═══════════════════════════════════════════════════════════════════

def detect_subcontractor_payment(description, amount, industry):
    """Flag potential T5018-reportable subcontractor e-transfers (Construction >= $500)."""
    if industry != "Construction/Trades":
        return False
    if amount < 500:
        return False
    desc_upper = (description or "").upper()
    return any(kw in desc_upper for kw in
               ["E-TRANSFER", "ETRANSFER", "INTERAC ETFR", "E-TFR", "SEND MONEY",
                "EMAIL TRANSFER", "INTERAC TRANSFER"])


def extract_payee_name(description):
    """Extract payee name from e-transfer description."""
    desc = re.sub(r'(?i)^(INTERAC\s+E-?TFR|E-?TRANSFER|ETRANSFER|SEND MONEY TO)\s*[-–]?\s*', '', (description or ""))
    desc = re.sub(r'\s+\d{8,}$', '', desc)
    return desc.strip().title() or "Unknown Payee"


def aggregate_t5018(transactions):
    """Aggregate subcontractor payments for T5018 reporting."""
    by_payee = {}
    for t in transactions:
        if "T5018" in t.get("notes", ""):
            payee = extract_payee_name(t.get("description", ""))
            if payee not in by_payee:
                by_payee[payee] = {"payee": payee, "total": 0, "count": 0, "transactions": []}
            by_payee[payee]["total"] += t.get("debit", 0)
            by_payee[payee]["count"] += 1
            by_payee[payee]["transactions"].append(t)
    result = sorted(by_payee.values(), key=lambda x: x["total"], reverse=True)
    for r in result:
        r["t5018_required"] = r["total"] >= 500
    return result


# ═══════════════════════════════════════════════════════════════════
# POST-PROCESSING VALIDATION ENGINE (v3.3)
# ═══════════════════════════════════════════════════════════════════

def run_validation(transactions):
    """
    Auto-fix common ledger errors. Returns (transactions, report).
    """
    report = []
    rules = [
        ("NEG_CREDIT", "error", "Credit amount cannot be negative",
         lambda t: t.get("credit", 0) < 0,
         lambda t: t.update({"credit": abs(t["credit"]), "notes": (t.get("notes","") + " FIX:NEG_CREDIT").strip()})),
        ("PAYMENT_WITH_CATEGORY", "warning", "PAYMENT should not have category",
         lambda t: t.get("type") == "PAYMENT" and t.get("category") not in ("", None),
         lambda t: t.update({"category": "", "notes": (t.get("notes","") + " FIX:PAYMENT_CAT_CLEARED").strip()})),
        ("INTEREST_CATEGORY", "warning", "INTEREST should be Bank Charges",
         lambda t: t.get("type") == "INTEREST" and t.get("category") not in ("Bank Charges", ""),
         lambda t: t.update({"category": "Bank Charges", "t2125": "8710", "notes": (t.get("notes","") + " FIX:INTEREST_CAT").strip()})),
        ("REFUND_AS_REVENUE", "error", "REFUND must not be revenue",
         lambda t: t.get("type") == "REFUND" and t.get("category","").lower() in ("revenue","income","sales"),
         lambda t: t.update({"category": "Expense Reduction", "notes": (t.get("notes","") + " FIX:REFUND_NOT_REVENUE").strip()})),
        ("MEALS_50_RULE", "info", "Meals & Entertainment must have 50% note",
         lambda t: t.get("category") == "Meals & Entertainment" and "MEALS_50_RULE" not in t.get("notes",""),
         lambda t: t.update({"notes": (t.get("notes","") + " MEALS_50_RULE").strip()})),
        ("INSURANCE_ITC", "warning", "Insurance ITC must be zero",
         lambda t: t.get("category") == "Insurance" and t.get("itc_amount", 0) > 0,
         lambda t: t.update({"itc_amount": 0, "notes": (t.get("notes","") + " FIX:INSURANCE_ITC_ZERO").strip()})),
        ("BANK_CHARGES_ITC", "warning", "Bank Charges ITC must be zero",
         lambda t: t.get("category") == "Bank Charges" and t.get("itc_amount", 0) > 0,
         lambda t: t.update({"itc_amount": 0, "notes": (t.get("notes","") + " FIX:BANK_ITC_ZERO").strip()})),
        ("FEE_REBATE_AS_INCOME", "error", "FEE_REBATE must not be income",
         lambda t: t.get("type") == "FEE_REBATE" and t.get("category","").lower() in ("revenue","income","sales"),
         lambda t: t.update({"category": "", "notes": (t.get("notes","") + " FIX:REBATE_NOT_INCOME").strip()})),
    ]
    for t in transactions:
        for rule_id, severity, desc, check_fn, fix_fn in rules:
            try:
                if check_fn(t):
                    fix_fn(t)
                    report.append({
                        "rule": rule_id, "severity": severity, "description": desc,
                        "date": t.get("date",""), "merchant": t.get("description",""),
                        "amount": t.get("debit",0) or t.get("credit",0),
                    })
            except Exception:
                pass
    return transactions, report


# ═══════════════════════════════════════════════════════════════════
# REFUND MATCH (v3.3)
# ═══════════════════════════════════════════════════════════════════

def match_refunds_to_purchases(transactions):
    """
    If a credit merchant matches a known debit merchant, classify as REFUND.
    Prevents merchant refunds being misclassified as income.
    """
    debit_merchants = set()
    for t in transactions:
        if t.get("debit", 0) > 0 and t.get("type") in ("PURCHASE", "FEE", ""):
            norm, _ = normalize_merchant(t.get("description", ""))
            debit_merchants.add(norm.upper())

    for t in transactions:
        if t.get("credit", 0) > 0 and t.get("type") not in ("PAYMENT", "FEE_REBATE"):
            norm, _ = normalize_merchant(t.get("description", ""))
            if norm.upper() in debit_merchants:
                if t.get("type") != "REFUND":
                    t["type"] = "REFUND"
                    t["notes"] = (t.get("notes","") + " REFUND_MATCH EXPENSE_REDUCTION").strip()
    return transactions


# ═══════════════════════════════════════════════════════════════════
# YEAR BOUND CHECK (v3.3)
# ═══════════════════════════════════════════════════════════════════

def apply_year_bound(transactions, statement_period):
    """
    Hard-assign years for cross-year statements (e.g. Dec 2024 - Jan 2025).
    Prevents Dec transactions defaulting to current year.
    """
    if not statement_period:
        return transactions
    years = re.findall(r'20\d{2}', statement_period)
    if len(years) < 2 or int(years[0]) == int(years[1]):
        return transactions
    year_start, year_end = int(years[0]), int(years[1])
    current_year = datetime.now().year

    for t in transactions:
        date_str = t.get("date", "")
        if re.match(r'^\d{4}-\d{2}-\d{2}$', date_str):
            year = int(date_str[:4])
            month = int(date_str[5:7])
            if year == current_year and year != year_start and year != year_end:
                if month >= 10:
                    t["date"] = f"{year_start}{date_str[4:]}"
                    t["notes"] = (t.get("notes","") + " YEAR_BOUND_FIX").strip()
                elif month <= 3:
                    t["date"] = f"{year_end}{date_str[4:]}"
                    t["notes"] = (t.get("notes","") + " YEAR_BOUND_FIX").strip()
    return transactions


# ═══════════════════════════════════════════════════════════════════
# COVERAGE CHECK (v3.3)
# ═══════════════════════════════════════════════════════════════════

def check_transaction_coverage(raw_text, parsed_count):
    """
    Estimate expected vs parsed transactions.
    Returns (estimated, coverage_pct, warning_message_or_None).
    """
    date_lines = re.findall(
        r'(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2}|'
        r'\d{4}-\d{2}-\d{2}|\d{2}/\d{2}/\d{4}', raw_text)
    estimated = max(1, len(date_lines) // 2)  # CC stmts have trans+post dates per row
    if parsed_count == 0:
        return estimated, 0, "🔴 No transactions parsed — check statement format"
    coverage = min(100, round(parsed_count / estimated * 100))
    if coverage < 70:
        return estimated, coverage, f"🔴 Low coverage ({coverage}%) — {parsed_count} parsed vs ~{estimated} expected. Verify completeness."
    elif coverage < 95:
        return estimated, coverage, f"⚠️ Partial coverage ({coverage}%) — {parsed_count} of ~{estimated} rows captured."
    return estimated, coverage, None


# ═══════════════════════════════════════════════════════════════════
# EXPENSE ANOMALY DETECTION (v3.3)
# ═══════════════════════════════════════════════════════════════════

def detect_expense_anomalies(transactions, baseline_multiplier=3.0):
    """Flag transactions that are unusually large vs the median for their category."""
    import statistics
    by_category = {}
    for t in transactions:
        if t.get("debit", 0) > 0 and t.get("category"):
            cat = t["category"]
            by_category.setdefault(cat, []).append(t["debit"])

    anomalies = []
    for cat, amounts in by_category.items():
        if len(amounts) < 2:
            continue
        median_amt = statistics.median(amounts)
        if median_amt <= 0:
            continue
        for t in transactions:
            if t.get("category") == cat and t.get("debit", 0) > (median_amt * baseline_multiplier):
                multiplier = round(t["debit"] / median_amt, 1)
                anomalies.append({
                    "date": t.get("date",""), "description": t.get("description",""),
                    "amount": t["debit"], "category": cat,
                    "median_for_category": round(median_amt, 2),
                    "multiplier": multiplier,
                    "flag": f"⚠️ {multiplier}x above category median (${median_amt:.2f})"
                })
                t["notes"] = (t.get("notes","") + f" ANOMALY:{multiplier}x").strip()
    return anomalies


# ═══════════════════════════════════════════════════════════════════
# INDUSTRY REMAPPING (v3.3)
# ═══════════════════════════════════════════════════════════════════

def apply_industry_remaps(transactions, industry):
    """Apply industry-specific T2125 category remaps."""
    profile = INDUSTRY_PROFILES.get(industry, INDUSTRY_PROFILES["Other"])
    remaps = profile.get("t2125_remaps", {})
    if not remaps:
        return transactions

    T2125_MAP = {
        "Motor Vehicle Expense": "9281", "Meals & Entertainment": "8523",
        "Office Supplies": "8810", "Utilities": "8220", "Bank Charges": "8710",
        "Insurance": "8690", "Materials & Supplies": "8811", "Rent": "8910",
        "Delivery & Shipping": "8730", "Advertising": "8520", "Travel": "9200",
        "Professional Fees": "8860", "Subcontracts": "8590",
        "Cost of Goods": "8320", "Repairs & Maintenance": "8960",
    }
    for t in transactions:
        old_cat = t.get("category", "")
        if old_cat in remaps:
            new_cat = remaps[old_cat]
            t["category"] = new_cat
            t["t2125"] = T2125_MAP.get(new_cat, "")
            t["notes"] = (t.get("notes","") + f" INDUSTRY_REMAP:{old_cat}>{new_cat}").strip()
    return transactions


# ═══════════════════════════════════════════════════════════════════
# MERCHANT PATTERNS
# ═══════════════════════════════════════════════════════════════════

CANADIAN_MERCHANT_PATTERNS = {
    r'(?:PETRO.?CAN|PETRO CANADA|ESSO|SHELL|PIONEER|ULTRAMAR|HUSKY|CO-OP GAS|GAS BAR|GASBAR|ESSENCE|FUEL|PARKLAND|MACS|CIRCLE K|COUCHE.?TARD|MOBIL|SUNOCO|CANADIAN TIRE GAS|CDN TIRE GAS|CT GAS|DOMO|FLYING J|PILOT|7.?ELEVEN.*GAS|SUNCOR|PETROLIA|SUPERSTORE GAS|COSTCO GAS|ONROUTE|MAWS FUEL|JIFFY LUBE|MR LUBE|OIL CHANGE)': {
        "category": "Motor Vehicle Expense", "t2125": "9281", "itc": "Full"},
    r'(?:TIM HORTON|STARBUCKS|MCDONALDS|MCDONALD\'?S|MC DONALD|SUBWAY|A\&W|WENDYS|WENDY|HARVEYS|SWISS CHALET|BOSTON PIZZA|MONTANA|PIZZA PIZZA|POPEYES|MARY BROWN|KFC|BURGER KING|TACO BELL|DAIRY QUEEN|DQ |DOMINO|PIZZA HUT|PANERA|CHIPOTLE|FIVE GUYS|FATBURGER|NANDOS|NANDO|OSMOW|SHAWARMA|PITA PIT|BOOSTER JUICE|SECOND CUP|BLENZ|COUNTRY STYLE|VISCONTI|PIZZA NOVA|241 PIZZA|PIZZA DELIGHT|TOPPER\'?S|SUNSET GRILL|GOLDEN GRIDDLE|MUCHO BURRITO|QUESADA|NEW YORK FRIES|HERO CERTIFIED|CAGE AUX SPORTS|ST-HUBERT|SCORES|GRECO|ALICE FAZOOLI|ESPLANADE BIER|SHOELESS JOE|BIER MARKT|COFFEE|CAFE|DINER|GRILL|\bREST(?:AURANT)?\b|STEAKHOUSE|SUSHI|THAI|CHINESE|INDIAN|MEXICAN|ITALIAN|GREEK|KOREAN|JAPANESE|VIETNAM|RAMEN|PHO|EARLS|MILESTONES|JACK ASTOR|KELSEY|EAST SIDE|CACTUS CLUB|MOXIE|ORIGINAL JOE|JOEYS|WHITE SPOT|DENNY|IHOP|CORA|SYMPOSIUM|WILD WING|ST.?LOUIS|LONE STAR|MANDARIN|PICKLE BARREL|SPRING ROLL|WINGS|PUB(?:\b|\s)|LOUNGE|BISTRO|BRASSERIE|KITCHEN|EATERY|TRATTORIA|OSTERIA|SMOKE.?S POUTINERIE|CASEY\'?S|THE HUB)': {
        "category": "Meals & Entertainment", "t2125": "8523", "itc": "50%"},
    # COSTCO WHOLESALE/WAREHOUSE on a business card = bulk business supplies.
    # Triangle MC format: 'COSTCO WHOLESALE W1261 WOODBRIDGE ON' or 'COSTCO GAS W1261 VAUGHAN ON'
    # Costco Gas already captured by Motor Vehicle Expense pattern above.
    r'(?:COSTCO WHOLESALE|COSTCO WAREHOUSE|COSTCO\s+W\d+)': {
        "category": "Materials & Supplies", "t2125": "8811", "itc": "Full"},
    r'(?:LOBLAWS|NO FRILLS|METRO(?:\s|$)|SOBEYS|FRESHCO|FOOD BASICS|FORTINOS|REAL CDN|REAL CANADIAN|WALMART(?!\s*TIRE)|SHOPPERS|LONGOS|FARM BOY|T\&T|NATIONS|OCEANS|SUPERSTORE|VALU.?MART|YOUR IND|INDEPENDENT|VOILA|INSTACART|SKIP THE DISHES|DOORDASH|UBER EATS|GROCERY|BULK BARN|DOLLARAMA|DOLLAR TREE)': {
        "category": "Owner Draw / Personal", "t2125": "", "itc": "No"},
    r'(?:HOME DEPOT|LOWES|LOWE\'?S|RONA|HOME HARDWARE|KENT|CASTLE|BMR|TIMBER MART|TOTEM|SLEGG|PATRICIAN|FASTENAL|GRAINGER|ACKLANDS|WURTH|HILTI|ULINE|CANAC|PRINCESS AUTO|TSC STORE|TRACTOR SUPPLY|RITCHIE BROS|HEAVY EQUIP)': {
        "category": "Materials & Supplies", "t2125": "8811", "itc": "Full"},
    r'(?:ROGERS|BELL\b|BELL CANADA|TELUS|FIDO|KOODO|VIRGIN|FREEDOM|SHAW|COGECO|VIDEOTRON|XPLORNET|TEKSAVVY|EXECULINK|START\.CA|PRIMUS|DISTRIBUTEL|CHATR|LUCKY MOBILE|PUBLIC MOBILE|FIZZ)': {
        "category": "Utilities", "t2125": "8220", "itc": "Full"},
    r'(?:HYDRO|ENBRIDGE|UNION GAS|TORONTO HYDRO|ALECTRA|FORTIS|HYDRO.?QU|BC HYDRO|EPCOR|ESSEX POWER|LONDON HYDRO|KITCHENER UTIL|WATERLOO NORTH|PEEL WATER|YORK REGION|DURHAM REGION|DIRECT ENERGY|JUST ENERGY)': {
        "category": "Utilities", "t2125": "8220", "itc": "Full"},
    r'(?:MANULIFE|SUN LIFE|GREAT.?WEST|INTACT|AVIVA|DESJARDINS INS|CO-OP.*INS|WAWANESA|ECONOMICAL|PEMBRIDGE|RSA |ZURICH|AIG|CHUBB|TRAVELERS|ALLSTATE|STATE FARM|BELAIR|INDUSTRIAL ALLIANCE|EQUITABLE|EMPIRE LIFE|INSURANCE|INSUR)': {
        "category": "Insurance", "t2125": "8690", "itc": "No"},
    r'(?:STAPLES|BUREAU EN GROS|BEST BUY|CANADA COMP|MICRO CENTER|MEMORY EXPRESS|NEWEGG|APPLE\.COM|APPLE STORE|DELL|LENOVO|MICROSOFT|ADOBE|GOOGLE.*CLOUD|AWS|DROPBOX|ZOOM|SLACK|NOTION|CANVA|INTUIT|QUICKBOOKS|XERO|FRESHBOOKS|WAVE|MAILCHIMP|HUBSPOT|SQUARESPACE|WIX|GODADDY|NAMECHEAP|OFFICE DEPOT|GRAND AND TOY)': {
        "category": "Office Supplies", "t2125": "8810", "itc": "Full"},
    r'(?:AMAZON|AMZN|AMZ\*)': {
        "category": "Office Supplies", "t2125": "8810", "itc": "Full", "low_confidence_industries": ["Construction/Trades"]},
    r'(?:PRESTO|TTC|METROLINX|GO TRANSIT|OC TRANSPO|UBER(?!\s*EATS)|LYFT|TAXI|CAB|PARKING|IMPARK|GREEN P|INDIGO PARK|DIAMOND PARK|407 ETR|HIGHWAY|TOLL|CAR WASH|AUTO SPA)': {
        "category": "Motor Vehicle Expense", "t2125": "9281", "itc": "Full"},
    r'(?:MONTHLY FEE|SERVICE CHARGE|NSF|OVERDRAFT|INTEREST CHARGE[S]?|CHQ ORDER|ANNUAL FEE|MEMBERSHIP FEE|CONVENIENCE FEE|WIRE FEE|TRANSFER FEE|FRAIS D\.INTER|BANK FEE)': {
        "category": "Bank Charges", "t2125": "8710", "itc": "No"},
    r'(?:CRA|CANADA REVENUE|GOV.*CANADA|SERVICE CANADA|SERVICE ONTARIO|ICBC|SAAQ|WSIB|EHT|PAYROLL REMIT|HST REMIT|GST REMIT|CPP |EI PREMIUM)': {
        "category": "Government Remittances", "t2125": "", "itc": "No"},
    r'(?:GOOGLE ADS|FACEBOOK|META ADS|INSTAGRAM|TIKTOK ADS|LINKEDIN|TWITTER|YELP|YELLOW PAGES|YP\.CA|KIJIJI|MARKETPLACE|GOOGLE.*ADWORDS|FACEBK|FB ADS|CANVA PRO|HOOTSUITE|CONSTANT CONTACT|VISTAPRINT|MOO\.COM|SIGN|BANNER|PRINT SHOP)': {
        "category": "Advertising", "t2125": "8520", "itc": "Full"},
    r'(?:CANADA POST|PUROLATOR|UPS|FEDEX|DHL|CANPAR|LOOMIS|DICOM|DAY \& ROSS|ESTES|XPO|SHIP)': {
        "category": "Delivery & Shipping", "t2125": "8730", "itc": "Full"},
    r'(?:AIR CANADA|WESTJET|PORTER|FLAIR|SWOOP|SUNWING|BOOKING\.COM|HOTELS\.COM|AIRBNB|VRBO|MARRIOTT|HILTON|HOLIDAY INN|BEST WESTERN|COMFORT INN|SUPER 8|DAYS INN|MOTEL|HOTEL|ENTERPRISE|HERTZ|AVIS|BUDGET|NATIONAL CAR|THRIFTY)': {
        "category": "Travel", "t2125": "9200", "itc": "Full"},
    r'(?:LAW\s|LEGAL|LAWYER|BARRIST|SOLICITOR|NOTARY|ACCOUNTING|ACCOUNTANT|CPA\b|BOOKKEEP|TAX PREP|H\&R BLOCK|TURBOTAX|WEALTHSIMPLE TAX|ARCHITECT|ENGINEER|CONSULT|SURVEYOR)': {
        "category": "Professional Fees", "t2125": "8860", "itc": "Full"},
    r'(?:NETFLIX|SPOTIFY|DISNEY\+|APPLE MUSIC|YOUTUBE|CRAVE|AMAZON PRIME|PARAMOUNT|NINTENDO|PLAYSTATION|XBOX|STEAM|CINEMA|CINEPLEX|LANDMARK|AMC|BOWLING|GOLF|GYM|FITNESS|GOODLIFE|FIT4LESS|PLANET FITNESS|YOGA|LCBO|BEER STORE|WINE RACK|CANNABIS|OCS\b|SQDC)': {
        "category": "Owner Draw / Personal", "t2125": "", "itc": "No"},
    r'(?:PHARMACY|PHARMA|REXALL|JEAN COUTU|LONDON DRUGS|MEDICAL|CLINIC|DENTAL|DENTIST|OPTOM|CHIRO|PHYSIO|MASSAGE|DR\.\s|DOCTOR|HOSPITAL|HEALTH|LIFELAB|DYNACARE)': {
        "category": "Owner Draw / Personal", "t2125": "", "itc": "No"},
    r'(?:OLD NAVY|GAP\b|H\&M|ZARA|WINNERS|MARSHALLS|HOMESENSE|HUDSON BAY|THE BAY|NORDSTROM|SIMONS|SPORT CHEK|ATMOSPHERE|MARKS\b|MARK\'S|NIKE|ADIDAS|FOOT LOCKER|ALDO|ROOTS|LULULEMON|ARITZIA)': {
        "category": "Owner Draw / Personal", "t2125": "", "itc": "No"},
    r'(?:CHATGPT|OPENAI|ANTHROPIC|GITHUB|HEROKU|DIGITAL OCEAN|LINODE|VULTR|CLOUDFLARE|SHOPIFY|STRIPE|SQUARE|PAYPAL FEE|CLOVER|LIGHTSPEED|TOAST)': {
        "category": "Office Supplies", "t2125": "8810", "itc": "Full"},
    r'(?:RENT\b|LEASE\b|LANDLORD|PROPERTY MGMT|MANAGEMENT.*PROPERTY|CONDO FEE|MAINTENANCE FEE)': {
        "category": "Rent", "t2125": "8910", "itc": "Full"},
    r'(?:CANADIAN TIRE(?!\s*GAS)|CDN TIRE STORE|CDN TIRE (?!GAS)|CT\s|PARTY CITY)': {
        "category": "Materials & Supplies", "t2125": "8811", "itc": "Full"},
    r'(?:NAPA|AUTOZONE|PARTSOURCE|LORDCO|MIDAS|MEINEKE|AAMCO|SPEEDY|ACTIVE GREEN|OK TIRE|KAL TIRE|FOUNTAIN TIRE|CANADIAN TIRE AUTOMOTIVE|CT AUTO|BRAKE|EXHAUST|TRANSMISS|MECHANIC|AUTO REPAIR|AUTO SERVICE)': {
        "category": "Motor Vehicle Expense", "t2125": "9281", "itc": "Full"},
    r'(?:MARKS WORK|WORK AUTHORITY|SAFETYZONE|SAFETY|WORKWEAR|BOOT|STEEL TOE|COVERALL|HI.?VIS)': {
        "category": "Materials & Supplies", "t2125": "8811", "itc": "Full"},
    # Finance charge credit / interest adjustment = Bank Charges credit (refund of interest)
    r'(?:FINANCE CHARGE CREDIT|INTEREST ADJUSTMENT|INTEREST CREDIT|FRAIS FINANC)': {
        "category": "Bank Charges", "t2125": "8710", "itc": "No"},
    # ScotiaBank / TELESCOTIA payment processor — payment, not a purchase
    r'(?:TELESCOTIA|SCOTIABANK PMT|CIBC BANK PMT|CIBC PMT|RBC PMT|TD PMT|BMO PMT|DESJARDINS PMT|NATIONAL BANK PMT|HSBC PMT|PAIEMENT BCIC|PAYMENT BCIC)': {
        "category": "Transfers (Ignore)", "t2125": "", "itc": "No"},
    # Google services — could be Workspace (Office Supplies) or personal (YouTube)
    r'(?:GOOGLE \*WORKSPACE|GOOGLE.*DRIVE|GOOGLE.*ADS|GOOGLE.*CLOUD)': {
        "category": "Office Supplies", "t2125": "8810", "itc": "Full"},
    r'(?:GOOGLE \*YOUTUBE|GOOGLE \*PLAY|GOOGLE \*STADIA|GOOGLE \*ONE STORAGE)': {
        "category": "Owner Draw / Personal", "t2125": "", "itc": "No"},
    # KDE (Canadian Tire in-store product detail line) → already in Purchases so skip
    # McDonalds variations
    r'(?:MCDONALDS|MCDONALD\s?\'?S|MC DONALD)': {
        "category": "Meals & Entertainment", "t2125": "8523", "itc": "50%"},
}


# ═══════════════════════════════════════════════════════════════════
# PROVINCE NAME → CODE MAP (v3.4)
# Single source of truth — eliminates the UI string vs code mismatch bug
# ═══════════════════════════════════════════════════════════════════

PROVINCE_NAME_TO_CODE = {
    "Ontario": "ON", "Quebec": "QC", "British Columbia": "BC", "BC": "BC",
    "Alberta": "AB", "Saskatchewan": "SK", "Manitoba": "MB",
    "Nova Scotia": "NS", "New Brunswick": "NB", "Newfoundland": "NL",
    "Prince Edward Island": "PE", "Northwest Territories": "NT",
    "Yukon": "YT", "Nunavut": "NU", "Other": "ON",
    # Codes pass through unchanged
    "ON": "ON", "QC": "QC", "AB": "AB", "SK": "SK", "MB": "MB",
    "NS": "NS", "NB": "NB", "NL": "NL", "PE": "PE", "NT": "NT",
    "YT": "YT", "NU": "NU",
}


def normalize_province(province_str):
    """
    Accepts either a full province name ('Ontario') or a code ('ON').
    Always returns a two-letter code. Defaults to 'ON' if unknown.
    This is the fix for the UI→tax-calc province mismatch bug.
    """
    if not province_str:
        return "ON"
    return PROVINCE_NAME_TO_CODE.get(province_str.strip(), "ON")


# ═══════════════════════════════════════════════════════════════════
# T2125 LINE MAP — module-level so it's importable everywhere
# ═══════════════════════════════════════════════════════════════════

T2125_LINE_MAP = {
    "Motor Vehicle Expense": "9281", "Meals & Entertainment": "8523",
    "Office Supplies": "8810", "Utilities": "8220", "Bank Charges": "8710",
    "Insurance": "8690", "Materials & Supplies": "8811", "Rent": "8910",
    "Delivery & Shipping": "8730", "Shipping": "8730", "Advertising": "8520",
    "Travel": "9200", "Professional Fees": "8860", "Subcontracts": "8590",
    "Cost of Goods": "8320", "Repairs & Maintenance": "8960",
    "Shareholder Loan (Debit)": "", "Owner Draw / Personal": "",
    "Government Remittances": "", "Bank Charges": "8710",
}


# ═══════════════════════════════════════════════════════════════════
# COMPILED MERCHANT PATTERNS (v3.4)
# Pre-compiled once at module load — ~50x faster for 500+ tx statements
# ═══════════════════════════════════════════════════════════════════

COMPILED_MERCHANT_PATTERNS = [
    (re.compile(pattern, re.IGNORECASE), info)
    for pattern, info in CANADIAN_MERCHANT_PATTERNS.items()
]


def clean_description(description):
    desc = description.upper().strip()
    for p in [r'^POS\s+(PURCHASE|REFUND|RETURN)\s*[-–—]?\s*',
              r'^INTERAC\s+(PURCHASE|E-TRANSFER)\s*[-–—]?\s*',
              r'^CONTACTLESS\s+(PURCHASE|PMT)\s*[-–—]?\s*',
              r'^VISA\s+(PURCHASE|DEBIT)\s*[-–—]?\s*',
              r'^MC\s+(PURCHASE|DEBIT)\s*[-–—]?\s*',
              r'^PREAUTHORIZED\s+(DEBIT|PAYMENT|PMT)\s*[-–—]?\s*',
              r'^PAD\s*[-–—]?\s*', r'^PAY\s*[-–—]?\s*', r'^WWW\.\s*',
              r'^RECURRING\s+', r'^ONLINE\s+(PURCHASE|PMT)\s*[-–—]?\s*']:
        desc = re.sub(p, '', desc)
    desc = re.sub(r'\s+\d{8,15}$', '', desc)
    desc = re.sub(r'\s+[A-Z]{2,}\s+(?:ON|QC|BC|AB|SK|MB|NS|NB|PE|NL|NT|YT|NU)\s*(?:CA|CAN)?$', '', desc)
    desc = re.sub(r'\s+(?:ON|QC|BC|AB|SK|MB|NS|NB|PE|NL|NT|YT|NU)\s*(?:CA|CAN)?$', '', desc)
    desc = re.sub(r'\s+(?:CA|CAN|US|USA)$', '', desc)
    desc = re.sub(r'\s*#\s*\d+', '', desc)
    return desc.strip(' -–—')


def pre_categorize_merchant(description):
    """Uses pre-compiled patterns for performance."""
    cleaned = clean_description(description)
    for text in [cleaned, description.upper()]:
        for compiled_pat, cat_info in COMPILED_MERCHANT_PATTERNS:
            if compiled_pat.search(text):
                return cat_info.copy()
    return None


def parse_date_fuzzy(date_str, period_hint=""):
    """
    Parse various date formats robustly.
    For year-less formats (e.g. 'Dec 23'), infers year from period_hint
    instead of defaulting to 1900 (the Python strptime default).
    This fixes the reconciliation day-diff bug (~45,000 days off).
    """
    if not date_str:
        return None
    s = date_str.strip()

    # Already has year — parse directly
    for fmt in ("%b %d, %Y", "%d-%b-%Y", "%Y-%m-%d", "%m/%d/%Y",
                "%d/%m/%Y", "%m/%d/%y", "%d %b %Y", "%b. %d, %Y"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue

    # Year-less formats — need to infer year from context
    dt_no_year = None
    for fmt in ("%b %d", "%b. %d", "%d %b", "%d-%b"):
        try:
            dt_no_year = datetime.strptime(s, fmt)
            break
        except ValueError:
            continue

    if dt_no_year is None:
        return None

    # Infer year from period_hint (e.g. "Jan 2024 - Feb 2024" or "Dec 2024 - Jan 2025")
    inferred_year = datetime.now().year
    if period_hint:
        years_in_hint = re.findall(r'20\d{2}', period_hint)
        if years_in_hint:
            if len(years_in_hint) == 1:
                inferred_year = int(years_in_hint[0])
            else:
                # Cross-year statement: Oct-Dec → first year, Jan-Mar → second year
                if dt_no_year.month >= 10:
                    inferred_year = int(years_in_hint[0])
                elif dt_no_year.month <= 3:
                    inferred_year = int(years_in_hint[-1])
                else:
                    inferred_year = int(years_in_hint[0])

    try:
        return dt_no_year.replace(year=inferred_year)
    except ValueError:
        return None


# ═══════════════════════════════════════════════════════════════════
# COORDINATE-BASED PARSING
# ═══════════════════════════════════════════════════════════════════

def extract_with_coordinates(pdf_page):
    words = pdf_page.extract_words(x_tolerance=3, y_tolerance=3)
    if not words:
        return []
    lines = {}
    for w in words:
        y_key = round(w["top"] / 3) * 3
        lines.setdefault(y_key, []).append(w)
    return [sorted(lines[y], key=lambda w: w["x0"]) for y in sorted(lines.keys())]


def detect_double_amounts(line_words, amount_x_threshold=400):
    amounts = []
    for w in line_words:
        text = w["text"].replace(",", "").replace("$", "")
        if re.match(r'^-?\d+\.\d{2}$', text) and w["x0"] > amount_x_threshold:
            amounts.append(float(text))
    return amounts


# ═══════════════════════════════════════════════════════════════════
# PUBLIC API
# ═══════════════════════════════════════════════════════════════════


# ═══════════════════════════════════════════════════════════════════
# MULTI-LINE TRANSACTION MERGING
# Some PDF extractors split a single transaction across multiple lines:
#   Jan 04          ← date-only line
#   AMAZON.CA       ← description-only line
#   45.20           ← amount-only line
# This preprocessor collapses them into canonical single-line format
# before the regex parser runs.
# ═══════════════════════════════════════════════════════════════════

# Patterns that indicate a line is a date-only opener
_DATE_ONLY_RE = re.compile(
    r'^((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[.\s]+\d{1,2}'
    r'(?:,?\s*\d{4})?|\d{2}[/-]\d{2}[/-]\d{2,4}|\d{4}-\d{2}-\d{2})\s*$',
    re.IGNORECASE
)
# A line that is ONLY an amount (with optional trailing balance)
_AMOUNT_ONLY_RE = re.compile(
    r'^\$?[\d,]+\.\d{2}(\s+\$?[\d,]+\.\d{2})?(\s*(CR|DR))?\s*$'
)
# A line that already looks like a complete transaction (date + description + amount)
_COMPLETE_LINE_RE = re.compile(
    r'^(?:(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[.\s]+\d{1,2}'
    r'|\d{2}[/-]\d{2}[/-]\d{2,4}|\d{4}-\d{2}-\d{2})'
    r'.+?\$?[\d,]+\.\d{2}',
    re.IGNORECASE
)


def merge_multiline_transactions(raw_text):
    """
    Pre-process raw PDF text to collapse split transactions into single lines.

    Handles three common fragmentation patterns:
      Pattern A — date / description / amount on separate lines
      Pattern B — date+description / amount on separate lines
      Pattern C — date on its own line, rest together (common in mobile PDFs)

    Lines that already look complete (date + content + amount) pass through unchanged.
    Returns the merged text, ready for transaction_regex matching.
    """
    lines = raw_text.split("\n")
    merged = []
    i = 0
    merges_done = 0

    while i < len(lines):
        line = lines[i].rstrip()
        stripped = line.strip()

        # Skip blank lines
        if not stripped:
            merged.append(line)
            i += 1
            continue

        # If already a complete transaction line → pass through
        if _COMPLETE_LINE_RE.match(stripped):
            merged.append(line)
            i += 1
            continue

        # Pattern A/C: line is a date-only opener
        if _DATE_ONLY_RE.match(stripped):
            combined = stripped
            j = i + 1
            # Absorb following non-blank lines until we hit an amount or another date
            while j < len(lines) and (j - i) < 5:
                nxt = lines[j].strip()
                if not nxt:
                    j += 1
                    break
                if _DATE_ONLY_RE.match(nxt):
                    break   # Next transaction starting — stop
                combined = combined + " " + nxt
                if _AMOUNT_ONLY_RE.match(nxt) or _COMPLETE_LINE_RE.match(combined):
                    j += 1
                    merges_done += 1
                    break
                j += 1
            merged.append(combined)
            i = j
            continue

        # Pattern B: description line followed immediately by amount-only line
        if i + 1 < len(lines):
            nxt = lines[i + 1].strip()
            if nxt and _AMOUNT_ONLY_RE.match(nxt):
                merged.append(stripped + " " + nxt)
                merges_done += 1
                i += 2
                continue

        merged.append(line)
        i += 1

    return "\n".join(merged)

def detect_and_parse(raw_text):
    # Pre-process: collapse multi-line transactions before regex matching
    raw_text = merge_multiline_transactions(raw_text)
    schema_key, schema, confidence = detect_bank(raw_text)
    if not schema:
        return None, [], None
    transactions = parse_with_schema(raw_text, schema)
    for t in transactions:
        cat = pre_categorize_merchant(t.get("description", ""))
        if cat:
            t["_suggested_category"] = cat
    return schema["bank"], transactions, schema_key


def get_supported_banks():
    return [s["bank"] for s in BANK_SCHEMAS.values()]
