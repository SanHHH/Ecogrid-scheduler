import requests
import pandas as pd
from datetime import datetime
import os

# 碳排放係數 (kgCO₂ per kWh → 換算成 per MW)
carbon_factors = {
    '燃煤(Coal)': 0.9,
    '燃氣(LNG)': 0.5,
    '燃油(Oil)': 0.8,
    '核能(Nuclear)': 0,
    '太陽能(Solar)': 0,
    '風力(Wind)': 0,
    '水力(Hydro)': 0,
}

def crawl_taipower():
    url = "https://www.taipower.com.tw/d006/loadGraph/loadGraph/data/genary.json"
    response = requests.get(url)
    response.raise_for_status()

    data = response.json().get("aaData", [])
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M")  # 用 UTC, GitHub Actions 比較準

    records, carbon_total = [], 0
    for row in data:
        if not row or "b>" not in row[0]:
            continue
        energy_type = row[0].split(">")[-2].replace("</b", "").strip()
        try:
            actual = float(row[4])  # 發電量 MW
        except:
            continue

        carbon = carbon_factors.get(energy_type, 0) * actual
        carbon_total += carbon

        records.append({"時間": now, "能源類型": energy_type, "發電量(MW)": actual})

    df = pd.DataFrame(records)
    if df.empty:
        print("⚠️ No valid data fetched.")
        return

    total_gen = df["發電量(MW)"].sum()
    emission_per_kwh = round(carbon_total / total_gen, 6) if total_gen > 0 else 0

    df_summary = pd.DataFrame([{
        "時間": now,
        "總發電量(MW)": round(total_gen, 2),
        "碳排放量(TCO₂)": round(carbon_total, 2),
        "每度電碳排(kgCO₂/kWh)": emission_per_kwh
    }])

    file_path = "taipower_emission_data.csv"  # ✅ 存在 repo 根目錄

    if os.path.exists(file_path):
        old_df = pd.read_csv(file_path)
        df_summary = pd.concat([old_df, df_summary]).drop_duplicates(subset=["時間"], keep="last")

    df_summary.to_csv(file_path, index=False, encoding="utf-8-sig")
    print(f"✅ 已更新: {file_path}")

if __name__ == "__main__":
    crawl_taipower()
