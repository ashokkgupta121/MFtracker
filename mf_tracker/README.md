# 📈 Mutual Fund Portfolio Tracker

A desktop Windows application to track your mutual fund portfolio with real-time NAV data, P&L, and XIRR.

---

## 🚀 Setup & Installation

### Step 1 – Install Python
Download Python 3.9+ from https://www.python.org/downloads/

### Step 2 – Install dependencies
Open Command Prompt in this folder and run:
```
pip install -r requirements.txt
```

### Step 3 – Run the app
```
python main.py
```

---

## 📋 Features

| Feature | Description |
|---|---|
| **Add Fund** | Search by name or enter AMFI scheme code manually |
| **Import CSV** | Bulk import funds from a CSV file |
| **Refresh NAV** | Fetches live NAV history from MFAPI (free, no API key needed) |
| **Portfolio Table** | View units, invested amount, current value, P&L, and XIRR per fund |
| **Summary Cards** | Total invested, current value, overall P&L and XIRR at a glance |
| **NAV Chart** | Interactive line chart showing NAV history since purchase date |
| **Export CSV** | Save your portfolio to CSV for backup |

---

## 📂 CSV Import Format

Your CSV must have these columns:

```
scheme_code,name,units,purchase_nav,purchase_date
120503,SBI Bluechip Fund,100,45.23,2022-06-15
```

- **scheme_code** – AMFI code (find at https://www.mfapi.in)
- **name** – Display name
- **units** – Units purchased
- **purchase_nav** – NAV on purchase date
- **purchase_date** – Format: YYYY-MM-DD

A `sample_portfolio.csv` is included to test the import feature.

---

## 🔍 Finding Scheme Codes

Use the built-in search in the "Add Fund" dialog — it searches live from MFAPI.

Or visit: https://www.mfapi.in to search manually.

---

## 💾 Data Storage

Portfolio data is saved at:
```
C:\Users\<YourName>\.mf_tracker\portfolio.json
```

---

## 📦 Dependencies

- `PyQt5` – GUI framework
- `matplotlib` – NAV charts
- NAV data from **MFAPI** (https://api.mfapi.in) – free, no API key required

---

## 💡 Tips

- After importing CSV, always click **🔄 Refresh NAV** to load price history
- The yellow dashed line on the chart shows your purchase NAV
- Green = profit, Red = loss throughout the UI
- XIRR is annualized return accounting for time value of money
