import os, sqlite3, requests, yfinance as yf
import pandas as pd
import streamlit as st
from datetime import date

DB_PATH = "portfolio.db"
st.set_page_config(page_title="Portfolio Manager", layout="wide")

# ---------------- DB ----------------
def get_conn():
    c = sqlite3.connect(DB_PATH)
    c.execute("PRAGMA foreign_keys = ON;")
    return c

def init_db():
    conn = get_conn(); cur = conn.cursor()
    cur.execute("""CREATE TABLE IF NOT EXISTS positions(
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        ticker TEXT, type TEXT, platform TEXT,
        quantity REAL NOT NULL, avg_cost REAL NOT NULL,
        currency TEXT DEFAULT 'EUR', isin TEXT, ter REAL,
        purchase_date TEXT, price_source TEXT DEFAULT 'manual',
        price_symbol TEXT, notes TEXT
    );""")
    cur.execute("""CREATE TABLE IF NOT EXISTS prices(
        id INTEGER PRIMARY KEY,
        ticker TEXT NOT NULL, price REAL NOT NULL,
        currency TEXT DEFAULT 'EUR', asof TEXT NOT NULL, source TEXT
    );""")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_prices ON prices(ticker, asof);")
    conn.commit(); conn.close()

def upsert_position(row):
    conn = get_conn(); cur = conn.cursor()
    cur.execute("""INSERT INTO positions
        (name,ticker,type,platform,quantity,avg_cost,currency,isin,ter,purchase_date,price_source,price_symbol,notes)
        VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (row["name"],row["ticker"],row["type"],row["platform"],row["quantity"],row["avg_cost"],
         row.get("currency","EUR"),row.get("isin"),row.get("ter"),row.get("purchase_date"),
         row.get("price_source","manual"),row.get("price_symbol"),row.get("notes")))
    conn.commit(); conn.close()

def fetch_positions():
    conn = get_conn()
    df = pd.read_sql_query("SELECT * FROM positions ORDER BY name;", conn)
    conn.close()
    return df

def add_price_snapshot(ticker, price, currency="EUR", source="manual"):
    conn = get_conn(); cur = conn.cursor()
    cur.execute("INSERT INTO prices(ticker,price,currency,asof,source) VALUES(?,?,?,?,?)",
                (ticker, float(price), currency, date.today().isoformat(), source))
    conn.commit(); conn.close()

def latest_prices():
    conn = get_conn()
    df = pd.read_sql_query("""
      SELECT p1.ticker, p1.price, p1.currency, p1.asof
      FROM prices p1 JOIN (SELECT ticker, MAX(asof) a FROM prices GROUP BY ticker) p2
      ON p1.ticker=p2.ticker AND p1.asof=p2.a;
    """, conn)
    conn.close(); 
    return df

# ---------------- Price providers ----------------
def fetch_yahoo(symbol):
    try:
        t = yf.Ticker(symbol); info = t.fast_info
        return float(info.get("last_price")), info.get("currency","EUR"), "yahoo"
    except Exception:
        return None

def fetch_coingecko(coin_id, vs="eur"):
    try:
        r = requests.get(f"https://api.coingecko.com/api/v3/simple/price?ids={coin_id}&vs_currencies={vs}", timeout=15)
        r.raise_for_status()
        px = float(r.json()[coin_id][vs])
        return px, vs.upper(), "coingecko"
    except Exception:
        return None

def update_prices(selected_ids=None):
    df = fetch_positions()
    if selected_ids: df = df[df["id"].isin(selected_ids)]
    out = []
    for _, r in df.iterrows():
        src = (r.get("price_source") or "manual").lower()
        sym = r.get("price_symbol") or r.get("ticker") or r.get("name")
        res = fetch_yahoo(sym) if src=="yahoo" else fetch_coingecko(sym) if src=="coingecko" else None
        if res:
            px, cur, provider = res
            add_price_snapshot(r["ticker"] or r["name"], px, cur, provider)
            out.append((r["name"], px, provider))
        else:
            out.append((r["name"], None, "kein Feed"))
    return pd.DataFrame(out, columns=["Asset","Preis","Quelle/Status"])

# ---------------- UI ----------------
init_db()
st.title("Portfolio Manager")

tabs = st.tabs(["Übersicht (Overview)","Neue Position (New)","Preise (Prices)","Details (Details)"])

# --- Übersicht (robust, kein KeyError mehr) ---
with tabs[0]:
    st.subheader("Übersicht (Overview)")
    pos = fetch_positions()

    lp = latest_prices()
    if lp.empty:
        merged = pos.copy()
        for col in ["price", "currency", "asof"]:
            if col not in merged.columns:
                merged[col] = pd.NA
    else:
        lp = lp.rename(columns={"ticker": "_t"})
        join_key = pos["ticker"].where(pos["ticker"].notna(), pos["name"])
        merged = pos.merge(lp, left_on=join_key, right_on="_t", how="left")

    # numerisch casten
    for col in ["quantity", "avg_cost", "price"]:
        merged[col] = pd.to_numeric(merged.get(col), errors="coerce")

    # Kennzahlen
    merged["Marktwert €"] = (merged["quantity"].fillna(0) * merged["price"].fillna(0)).round(2)
    merged["Gewinn/Verlust €"] = (
        merged["Marktwert €"] - (merged["quantity"].fillna(0) * merged["avg_cost"].fillna(0))
    ).round(2)
    with pd.option_context("mode.use_inf_as_na", True):
        merged["Gewinn %"] = ((merged["price"] - merged["avg_cost"]) / merged["avg_cost"]).astype(float)
        merged["Gewinn %"] = merged["Gewinn %"].fillna(0).round(4)

    rename_map = {
        "name": "Asset/Name",
        "ticker": "Ticker/Symbol",
        "type": "Positionsart",
        "platform": "Plattform/Wallet/Broker",
        "quantity": "Stück",
        "avg_cost": "Ø-Kaufkurs €",
        "price": "Preis € (auto)",
        "Marktwert €": "Marktwert €",
        "Gewinn/Verlust €": "Gewinn/Verlust €",
        "Gewinn %": "Gewinn %",
        "purchase_date": "Kaufdatum",
        "ter": "TER % p.a.",
        "currency": "Währung",
        "isin": "ISIN/Contract",
        "notes": "Notizen",
    }
    cols = [c for c in rename_map.keys() if c in merged.columns]
    disp = merged[cols].rename(columns=rename_map)
    disp = disp.sort_values("Marktwert €", ascending=False, na_position="last")
    st.dataframe(disp, use_container_width=True)

# --- Neue Position ---
with tabs[1]:
    st.subheader("Neue Position erfassen (Add New Position)")
    col1,col2,col3 = st.columns(3)
    name = col1.text_input("Asset/Name (Asset/Name)")
    ticker = col2.text_input("Ticker/Symbol (Ticker/Symbol)")
    ptype = col3.selectbox("Positionsart (Type)", ["Aktie","ETF","Fonds","Krypto","Anleihe","Cash"])
    platform = col1.text_input("Plattform/Wallet/Broker (Platform/Wallet/Broker)")
    qty = col2.number_input("Stück (Quantity)", 0.0, step=0.001, format="%.3f")
    avg = col3.number_input("Ø-Kaufkurs € (Avg Cost €)", 0.0, step=0.01, format="%.2f")
    ter = col1.number_input("TER % p.a. (Expense Ratio)", 0.0, step=0.001, format="%.3f")
    pdate = col2.text_input("Kaufdatum YYYY-MM-DD (Purchase Date)")
    curr = col3.text_input("Währung (Currency)", "EUR")
    isin = col1.text_input("ISIN/Contract (ISIN/Contract)")
    notes = col2.text_input("Notizen (Notes)")
    src = col3.selectbox("Kursquelle (Price Source)", ["manual","yahoo","coingecko"])
    sym = col3.text_input("Symbol für Kursquelle (Symbol for Price Source)")
    if st.button("Hinzufügen (Add)"):
        upsert_position({
            "name":name,"ticker":ticker,"type":ptype,"platform":platform,
            "quantity":qty,"avg_cost":avg,"currency":curr,"isin":isin,"ter":ter,
            "purchase_date":(pdate or None),"price_source":src,"price_symbol":sym,"notes":notes
        })
        st.success("Gespeichert.")

# --- Preise ---
with tabs[2]:
    st.subheader("Kurse aktualisieren (Update Prices)")
    dfp = fetch_positions()
    ids = st.multiselect("Positionen (optional)", dfp["id"].tolist(),
                         format_func=lambda i: dfp[dfp["id"]==i]["name"].iloc[0] if not dfp.empty else i)
    if st.button("Preise abrufen (Fetch)"):
        res = update_prices(ids if ids else None)
        st.dataframe(res, use_container_width=True)

# --- Details ---
with tabs[3]:
    st.subheader("Details (Details)")
    pos = fetch_positions()
    if pos.empty:
        st.info("Keine Positionen vorhanden.")
    else:
        sel = st.selectbox("Asset wählen", pos["name"].tolist())
        row = pos[pos["name"]==sel].iloc[0]
        st.write(row.to_dict())
        conn = get_conn()
        ph = pd.read_sql_query("SELECT asof, price FROM prices WHERE ticker=? ORDER BY asof",
                               conn, params=((row['ticker'] or row['name']),))
        conn.close()
        if not ph.empty:
            ph["asof"] = pd.to_datetime(ph["asof"])
            st.line_chart(ph.set_index("asof")["price"])
        else:
            st.caption("Noch keine Preisdaten gespeichert.")
