#!/usr/bin/env python3
"""
Combined Flask app: Spending Insights + UPI Merchant Mapping Editor
"""
from flask import Flask, render_template, request, flash, jsonify
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv
from os import getenv
from datetime import datetime, timedelta
from math import ceil
import logging
import calendar
import json

load_dotenv()

app = Flask(__name__)
app.secret_key = getenv("FLASK_SECRET_KEY", "super-secret-please-change-this-in-production")

# ─── MySQL Config ──────────────────────────────────────────────────────────
MYSQL_CONFIG = {
    "host": getenv("MYSQL_HOST", "192.168.1.201"),
    "database": getenv("MYSQL_DB", "p_finance"),
    "user": getenv("MYSQL_USER", "root"),
    "password": getenv("MYSQL_PASSWORD"),
}

# Dropdown options
CATEGORIES = [
    "", "Snacks", "Groceries", "Food-Hotel", "Vehicle-Fuel-Maintanence", "Local-Shopping",
    "Online-Shopping", "Printout-Educational", "Entertainment", "Medical", "Travel",
    "Rentals", "Subscriptions", "Investment", "Insurance", "Bank Transfers",
    "Non-Veg", "Veg-Fruits", "Credit Card Payments", "Tax-Payments", "Renumeration"
]

FREQUENCIES = ["", "Daily", "Weekly", "Bi-Weekly", "Monthly", "Quarterly", "Yearly", "Random"]

def get_db_connection():
    try:
        conn = mysql.connector.connect(**MYSQL_CONFIG)
        if conn.is_connected():
            return conn
    except Error as e:
        logging.error(f"MySQL connection failed: {e}")
        return None

# ─── Helper Functions ──────────────────────────────────────────────────────
def get_budget_start_date(cursor):
    cursor.execute("SELECT setting_value FROM app_settings WHERE setting_key = 'budget_year_start'")
    row = cursor.fetchone()
    if row and row['setting_value']:
        try:
            return datetime.strptime(row['setting_value'], "%Y-%m-%d").date()
        except:
            pass
    return datetime(2026, 2, 23).date()  # default

def get_aggregates(spend_data):
    total = sum(row['total'] for row in spend_data)
    count = sum(row['count'] for row in spend_data)
    return total, count

def enhance_with_budgets(spend_data, budgets, budget_start, is_mtd=False, is_ytd=False):
    """
    Enhance spending data with budget & variance.
    For YTD: pro-rate using actual days elapsed since budget_start / 365 (simple & fair).
    """
    now = datetime.now()
    current_date = now.date()
    
    # Approximate days in a budget year (use 365.25 to handle leap years gently)
    DAYS_IN_BUDGET_YEAR = 365.25
    
    for row in spend_data:
        cat = row['category']
        
        if cat in budgets:
            monthly_budget = budgets[cat]
            annual_budget = monthly_budget * 12
            
            if is_ytd:
                if current_date < budget_start:
                    prorated_budget = 0.0
                else:
                    days_passed = (current_date - budget_start).days + 1  # inclusive
                    fraction_passed = days_passed / DAYS_IN_BUDGET_YEAR
                    prorated_budget = annual_budget * fraction_passed
                
                row['budget'] = round(prorated_budget, 2)
                row['variance'] = row['total'] - prorated_budget
            
            elif is_mtd:
                # Monthly: full budget (or pro-rate current month if desired)
                row['budget'] = round(monthly_budget, 2)
                row['variance'] = row['total'] - monthly_budget
            
            else:
                # Today / daily view
                daily_budget = monthly_budget / 30
                row['budget'] = round(daily_budget, 2)
                row['variance'] = row['total'] - daily_budget
        
        else:
            row['budget'] = 0.0
            row['variance'] = row['total']
    
    return sorted(spend_data, key=lambda x: x['total'], reverse=True)

def prepare_mtd_chart_data(mtd_data):
    """Prepare data specifically needed for MTD pie + bar charts"""
    # Pie: only categories with spend > 0
    pie_data = [row for row in mtd_data if row['total'] > 0]
    pie_data.sort(key=lambda x: x['total'], reverse=True)
    
    pie_labels = [row['category'] for row in pie_data]
    pie_values = [row['total'] for row in pie_data]

    # Bar: categories with spend > 0 OR budget > 0
    bar_data = []
    seen = set()
    for row in mtd_data:
        cat = row['category']
        if cat not in seen and (row['total'] > 0 or row['budget'] > 0):
            bar_data.append(row)
            seen.add(cat)
    
    bar_data.sort(key=lambda x: x['total'], reverse=True)
    
    bar_labels  = [row['category'] for row in bar_data]
    bar_spend   = [row['total'] for row in bar_data]
    bar_budget  = [row['budget'] for row in bar_data]

    return {
        'pie_labels': pie_labels,
        'pie_values': pie_values,
        'bar_labels': bar_labels,
        'bar_spend':  bar_spend,
        'bar_budget': bar_budget
    }

def get_annual_forecast(cursor, budgets, budget_start):
    """Annual budget forecasting based on current run rate (consistent with YTD logic)"""
    ytd_data = get_ytd_spend_by_category(cursor)
    ytd_data = enhance_with_budgets(ytd_data, budgets, budget_start, is_ytd=True)
    
    now = datetime.now().date()
    days_passed = max((now - budget_start).days + 1, 1)
    DAYS_IN_YEAR = 365.25
    fraction_passed = min(days_passed / DAYS_IN_YEAR, 1.0)
    
    forecast_rows = []
    total_ytd = 0.0
    total_projected = 0.0
    total_annual_budget = 0.0
    
    for row in ytd_data:
        ytd_spend = row['total']
        projected_annual = round(ytd_spend / fraction_passed, 2) if fraction_passed > 0 else ytd_spend
        
        annual_budget = budgets.get(row['category'], 0.0) * 12
        variance = round(projected_annual - annual_budget, 2)
        utilization = round((projected_annual / annual_budget * 100), 1) if annual_budget > 0 else 0
        
        forecast_rows.append({
            'category': row['category'],
            'ytd_spend': round(ytd_spend, 2),
            'projected_annual': projected_annual,
            'annual_budget': round(annual_budget, 2),
            'variance': variance,
            'utilization': utilization
        })
        
        total_ytd += ytd_spend
        total_projected += projected_annual
        total_annual_budget += annual_budget
    
    forecast_rows.sort(key=lambda x: x['projected_annual'], reverse=True)
    
    return {
        'rows': forecast_rows,
        'total_ytd': round(total_ytd, 2),
        'total_projected': round(total_projected, 2),
        'total_annual_budget': round(total_annual_budget, 2),
        'total_variance': round(total_projected - total_annual_budget, 2),
        'days_passed': days_passed,
        'fraction_passed_pct': round(fraction_passed * 100, 1)
    }

def get_merchant_spend_summary(cursor, limit=50):
    query = """
    SELECT 
        COALESCE(m.merchant_name, t.upi_id) AS merchant,
        COALESCE(m.category, 'Uncategorized') AS category,
        COUNT(*) AS transaction_count,
        COALESCE(SUM(t.amount), 0) AS total_spend,
        AVG(t.amount) AS avg_amount
    FROM upi_transactions t
    LEFT JOIN upi_id_mapping m ON t.upi_id = m.upi_id
    WHERE t.transaction_type LIKE '%_debit'
      AND COALESCE(m.category, '') != 'Bank Transfers'
    GROUP BY merchant, category
    HAVING total_spend > 0
    ORDER BY total_spend DESC
    LIMIT %s
    """
    cursor.execute(query, (limit,))
    rows = cursor.fetchall()
    
    for row in rows:
        row['total_spend'] = float(row['total_spend'])
        row['avg_amount']  = float(row['avg_amount']) if row['avg_amount'] is not None else 0.0
    
    return rows
# ─── Queries ───────────────────────────────────────────────────────────────
def get_today_spend_by_category(cursor):
    today = datetime.now().strftime("%Y-%m-%d")
    tomorrow = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")
    query = """
    SELECT COALESCE(m.category, 'Uncategorized') AS category,
           COALESCE(SUM(t.amount), 0) AS total,
           COUNT(*) AS count
    FROM upi_transactions t
    LEFT JOIN upi_id_mapping m ON t.upi_id = m.upi_id
    WHERE t.transaction_type LIKE '%_debit'
      AND COALESCE(m.category, '') != 'Bank Transfers'
      AND t.transaction_time >= %s AND t.transaction_time < %s
    GROUP BY m.category
    ORDER BY total DESC
    """
    cursor.execute(query, (today, tomorrow))
    rows = cursor.fetchall()
    
    # Convert Decimal → float here
    for row in rows:
        row['total'] = float(row['total'])
    
    return rows

def get_mtd_spend_by_category(cursor):
    today = datetime.now()
    first = today.replace(day=1).strftime("%Y-%m-%d")
    tomorrow = (today + timedelta(days=1)).strftime("%Y-%m-%d")
    query = """
    SELECT COALESCE(m.category, 'Uncategorized') AS category,
           COALESCE(SUM(t.amount), 0) AS total,
           COUNT(*) AS count
    FROM upi_transactions t
    LEFT JOIN upi_id_mapping m ON t.upi_id = m.upi_id
    WHERE t.transaction_type LIKE '%_debit'
      AND COALESCE(m.category, '') != 'Bank Transfers'
      AND t.transaction_time >= %s AND t.transaction_time < %s
    GROUP BY m.category
    ORDER BY total DESC
    """
    cursor.execute(query, (first, tomorrow))
    rows = cursor.fetchall()
    
    # Convert Decimal → float here
    for row in rows:
        row['total'] = float(row['total'])
    
    return rows

def get_ytd_spend_by_category(cursor):
    today = datetime.now()
    first = today.replace(month=1, day=1).strftime("%Y-%m-%d")
    tomorrow = (today + timedelta(days=1)).strftime("%Y-%m-%d")
    query = """
    SELECT COALESCE(m.category, 'Uncategorized') AS category,
           COALESCE(SUM(t.amount), 0) AS total,
           COUNT(*) AS count
    FROM upi_transactions t
    LEFT JOIN upi_id_mapping m ON t.upi_id = m.upi_id
    WHERE t.transaction_type LIKE '%_debit'
      AND COALESCE(m.category, '') != 'Bank Transfers'
      AND t.transaction_time >= %s AND t.transaction_time < %s
    GROUP BY m.category
    ORDER BY total DESC
    """
    cursor.execute(query, (first, tomorrow))
    rows = cursor.fetchall()
    
    # Convert Decimal → float here
    for row in rows:
        row['total'] = float(row['total'])
    
    return rows

def get_budgets(cursor):
    cursor.execute("SELECT category, monthly_budget FROM budgets")
    return {r['category']: float(r['monthly_budget']) for r in cursor.fetchall()}

def get_monthly_spend_trend(cursor, months_back=24):
    """
    Returns list of dicts: [{'month': '2025-02', 'total': 45231.50, 'count': 87}, ...]
    Ordered newest → oldest
    """
    query = """
    SELECT 
        DATE_FORMAT(t.transaction_time, '%Y-%m') AS month,
        COALESCE(SUM(t.amount), 0) AS total,
        COUNT(*) AS count
    FROM upi_transactions t
    LEFT JOIN upi_id_mapping m ON t.upi_id = m.upi_id
    WHERE t.transaction_type LIKE '%_debit'
      AND COALESCE(m.category, '') != 'Bank Transfers'
      AND t.transaction_time >= DATE_SUB(CURDATE(), INTERVAL %s MONTH)
    GROUP BY month
    ORDER BY month DESC
    LIMIT %s
    """
    cursor.execute(query, (months_back, months_back))
    rows = cursor.fetchall()
    for row in rows:
        row['total'] = float(row['total'])
    # Reverse so oldest → newest (better for line charts)
    return list(reversed(rows))

# ─── Routes ────────────────────────────────────────────────────────────────

@app.route('/health')
def health():
    return jsonify({"status": "healthy"}), 200

@app.route('/')
def insights():
    conn = get_db_connection()
    if not conn:
        return "Cannot connect to database", 500

    try:
        cursor = conn.cursor(dictionary=True)

        today_data = get_today_spend_by_category(cursor)
        mtd_data   = get_mtd_spend_by_category(cursor)
        ytd_data   = get_ytd_spend_by_category(cursor)
        budgets    = get_budgets(cursor)
        
        # ← Add these two lines
        budget_start = get_budget_start_date(cursor)
        budget_start_str = budget_start.strftime("%d %b %Y") if budget_start else "— not set —"

        today_total, today_count = get_aggregates(today_data)
        mtd_total,   mtd_count   = get_aggregates(mtd_data)
        ytd_total,   ytd_count   = get_aggregates(ytd_data)

        today_data = enhance_with_budgets(today_data, budgets, budget_start)
        mtd_data   = enhance_with_budgets(mtd_data,   budgets, budget_start, is_mtd=True)
        ytd_data   = enhance_with_budgets(ytd_data,   budgets, budget_start, is_ytd=True)

        now = datetime.now()

        return render_template(
            "insights.html",
            now=now,
            today_str=now.strftime("%d %b %Y"),
            month_str=now.strftime("%b %Y"),
            year_str=now.strftime("%Y"),
            today_total=today_total, today_count=today_count, today_data=today_data,
            mtd_total=mtd_total,     mtd_count=mtd_count,     mtd_data=mtd_data,
            ytd_total=ytd_total,     ytd_count=ytd_count,     ytd_data=ytd_data,
            budget_start=budget_start,
            budget_start_str=budget_start_str,
        )
    except Error as e:
        logging.error(f"Query error: {e}")
        return f"Database error: {e}", 500
    finally:
        if conn and conn.is_connected():
            conn.close()

@app.route('/charts')
def charts():
    conn = get_db_connection()
    if not conn:
        return "Cannot connect to database", 500

    try:
        cursor = conn.cursor(dictionary=True)

        budgets      = get_budgets(cursor)
        budget_start = get_budget_start_date(cursor)

        # MTD data for existing charts
        mtd_data = get_mtd_spend_by_category(cursor)
        mtd_data = enhance_with_budgets(mtd_data, budgets, budget_start, is_mtd=True)
        chart_data = prepare_mtd_chart_data(mtd_data)

        # NEW: Annual Forecast
        annual_forecast = get_annual_forecast(cursor, budgets, budget_start)

        # Monthly trend (from previous step)
        monthly_trend = get_monthly_spend_trend(cursor, months_back=24)

        now = datetime.now()
        month_str = now.strftime("%b %Y")

        forecast_end_str = "—"
        if budget_start:
            next_year = budget_start.replace(year=budget_start.year + 1)
            forecast_end_str = next_year.strftime("%d %b %Y")

        return render_template(
            "charts.html",
            month_str=month_str,
            **chart_data,
            monthly_trend=monthly_trend,
            months_back=24,
            annual_forecast=annual_forecast,
            budget_start_str=budget_start.strftime("%d %b %Y"),
            forecast_end_str=forecast_end_str,
        )

    except Error as e:
        logging.error(f"Charts error: {e}")
        return f"Database error: {e}", 500
    finally:
        if conn and conn.is_connected():
            conn.close()

@app.route("/mapping", methods=["GET", "POST"])
def mapping():
    conn = get_db_connection()
    if not conn:
        flash("Database connection failed", "danger")
        return render_template("mapping.html", rows=[])

    cursor = conn.cursor(dictionary=True)

    if request.method == "POST":
        upi_id    = request.form.get("upi_id")
        category  = request.form.get("category")
        frequency = request.form.get("frequency")

        if upi_id:
            try:
                cursor.execute("""
                    UPDATE upi_id_mapping
                       SET category = %s,
                           frequency = %s
                     WHERE upi_id = %s
                """, (category or None, frequency or None, upi_id))
                conn.commit()
                flash(f"Updated {upi_id} → {category or '—'} / {frequency or '—'}", "success")
            except Error as e:
                flash(f"Update failed: {e}", "danger")
        else:
            flash("No UPI ID received", "warning")

    cursor.execute("""
        SELECT id, upi_id, merchant_name, category, frequency
          FROM upi_id_mapping
      ORDER BY merchant_name
    """)
    rows = cursor.fetchall()

    cursor.close()
    conn.close()

    return render_template(
        "mapping.html",
        rows=rows,
        categories=CATEGORIES,
        frequencies=FREQUENCIES,
        db_host=MYSQL_CONFIG["host"],
        db_name=MYSQL_CONFIG["database"]
    )

@app.route('/merchants')
def merchants():
    conn = get_db_connection()
    if not conn:
        return "Cannot connect to database", 500

    try:
        cursor = conn.cursor(dictionary=True)

        # Get merchant summary (top 50 by default)
        merchant_data = get_merchant_spend_summary(cursor, limit=50)

        # Calculate grand total for percentage calculation
        grand_total = sum(row['total_spend'] for row in merchant_data)

        # Enrich with percentage
        for row in merchant_data:
            row['percentage'] = round((row['total_spend'] / grand_total * 100), 1) if grand_total > 0 else 0.0

        # Prepare chart data (top 12 for bar chart)
        chart_limit = 12
        chart_data = merchant_data[:chart_limit]
        
        chart_labels = [row['merchant'][:28] + "…" if len(row['merchant']) > 30 else row['merchant'] 
                        for row in chart_data]
        chart_values = [row['total_spend'] for row in chart_data]

        now = datetime.now()

        return render_template(
            "merchants.html",
            now=now,
            month_str=now.strftime("%b %Y"),
            merchants=merchant_data,
            grand_total=round(grand_total, 2),
            chart_labels=json.dumps(chart_labels),
            chart_values=json.dumps(chart_values),
            chart_limit=chart_limit,
            total_merchants_shown=len(merchant_data),
        )

    except Error as e:
        logging.error(f"Merchants query error: {e}")
        return f"Database error: {e}", 500
    finally:
        if conn and conn.is_connected():
            conn.close()

# ─── New Helper: Get all budgets as dict ───────────────────────────────────
def get_all_budgets(cursor):   # ← if you're using this one in /budgets route
    cursor.execute("SELECT category, monthly_budget FROM budgets ORDER BY category")
    return {row['category']: float(row['monthly_budget']) for row in cursor.fetchall()}

# ─── New Route: Budgets management ─────────────────────────────────────────
@app.route("/budgets", methods=["GET", "POST"])
def budgets():
    conn = get_db_connection()
    if not conn:
        flash("Database connection failed", "danger")
        return render_template("budgets.html", budgets_list=[])

    cursor = conn.cursor(dictionary=True)

    # ── POST handling remains the same ──
    if request.method == "POST":
        action = request.form.get("action")

        if action == "update":
            category = request.form.get("category")
            try:
                amount = float(request.form.get("monthly_budget", 0))
            except ValueError:
                amount = 0.0

            if category:
                try:
                    cursor.execute("""
                        INSERT INTO budgets (category, monthly_budget)
                        VALUES (%s, %s)
                        ON DUPLICATE KEY UPDATE
                            monthly_budget = %s,
                            updated_at = NOW()
                    """, (category, amount, amount))
                    conn.commit()
                    flash(f"Budget for '{category}' updated to ₹ {amount:,.2f}", "success")
                except Error as e:
                    flash(f"Error saving budget: {e}", "danger")

        elif action == "delete":
            category = request.form.get("category")
            if category:
                try:
                    cursor.execute("DELETE FROM budgets WHERE category = %s", (category,))
                    conn.commit()
                    flash(f"Budget for '{category}' removed", "info")
                except Error as e:
                    flash(f"Error deleting budget: {e}", "danger")

    # Load current budgets (same as before)
    cursor.execute("""
        SELECT category, monthly_budget, updated_at
          FROM budgets
      ORDER BY category
    """)
    budgets_list = cursor.fetchall()

    # ── CHANGED: only categories without budget yet ────────────────────────
    cursor.execute("""
        SELECT DISTINCT m.category
          FROM upi_id_mapping m
     LEFT JOIN budgets b ON m.category = b.category
         WHERE m.category IS NOT NULL 
           AND m.category != ''
           AND b.category IS NULL
      ORDER BY m.category
    """)
    known_categories = [r['category'] for r in cursor.fetchall()]

    cursor.close()
    conn.close()

    return render_template(
        "budgets.html",
        budgets_list=budgets_list,
        known_categories=known_categories
    )

@app.route('/transactions')
def transactions():
    conn = get_db_connection()
    if not conn:
        return "Cannot connect to database", 500
    
    try:
        cursor = conn.cursor(dictionary=True)
        
        # ─── Pagination ────────────────────────────────────────────────────────
        page = request.args.get('page', 1, type=int)
        per_page = 100
        offset = (page - 1) * per_page
        
        # Count total debit transactions
        cursor.execute("""
            SELECT COUNT(*) as total
            FROM upi_transactions t
            WHERE t.transaction_type LIKE '%_debit'
        """)
        total_transactions = cursor.fetchone()['total']
        total_pages = (total_transactions + per_page - 1) // per_page   # cleaner integer ceil
        
        # Fetch paginated transactions – keep merchant_name and upi_id separate
        query = """
            SELECT
                t.transaction_time,
                t.amount,
                t.transaction_type,
                t.upi_id,
                m.merchant_name,                  -- will be NULL if no match
                COALESCE(m.category, 'Uncategorized') AS category
            FROM upi_transactions t
            LEFT JOIN upi_id_mapping m ON t.upi_id = m.upi_id
            WHERE t.transaction_type LIKE '%_debit'
            ORDER BY t.transaction_time DESC
            LIMIT %s OFFSET %s
        """
        cursor.execute(query, (per_page, offset))
        txns = cursor.fetchall()
        
        # Convert Decimal → float (for Jinja / JavaScript safety)
        for txn in txns:
            txn['amount'] = float(txn['amount'])
            # Optional: provide fallback display value (but keep separate columns)
            txn['display_merchant'] = txn['merchant_name'] or txn['upi_id']
        
        # Optional: Calculate totals (excluding Bank Transfers)
        cursor.execute("""
            SELECT
                COALESCE(SUM(t.amount), 0) AS total_spend,
                COUNT(*) AS total_count
            FROM upi_transactions t
            LEFT JOIN upi_id_mapping m ON t.upi_id = m.upi_id
            WHERE t.transaction_type LIKE '%_debit'
              AND COALESCE(m.category, '') != 'Bank Transfers'
        """)
        summary = cursor.fetchone()
        total_spend_excl_transfers = float(summary['total_spend'])
        total_count_excl_transfers = summary['total_count']
        
        return render_template(
            "transactions.html",
            transactions=txns,
            page=page,
            total_pages=total_pages,
            per_page=per_page,
            total_transactions=total_transactions,
            total_spend_excl_transfers=total_spend_excl_transfers,
            total_count_excl_transfers=total_count_excl_transfers
        )
    
    except Error as e:
        logging.error(f"Transactions query error: {e}")
        return f"Database error: {e}", 500
    
    finally:
        if conn and conn.is_connected():
            conn.close()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    app.run(host="0.0.0.0", port=5000, debug=True)