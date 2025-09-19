import requests
import pandas as pd
from datetime import datetime, timedelta, timezone
import os

# === 碳排放係數 (單位：tCO₂/MWh) ===
carbon_factors = {
    '儲能(Energy Storage System)': 0,
    '其它再生能源(Other Renewable Energy)': 0,
    '太陽能(Solar)': 0,
    '核能(Nuclear)': 0,
    '民營電廠-燃氣(IPP-LNG)': 0.5,
    '民營電廠-燃煤(IPP-Coal)': 0.9,
    '水力(Hydro)': 0,
    '汽電共生(Co-Gen)': 0.6,
    '燃氣(LNG)': 0.5,
    '燃油(Oil)': 0.8,
    '燃煤(Coal)': 0.9,
    '輕油(Diesel)': 0.85,
    '風力(Wind)': 0
}

# === CSV 檔案路徑 ===
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
file_path = os.path.join(BASE_DIR, "taipower_emission_data.csv")

def crawl_taipower():
    url = "https://www.taipower.com.tw/d006/loadGraph/loadGraph/data/genary.json"
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()
    aaData = data.get("aaData", [])

    # 取得台灣時間 (UTC+8)
    now = datetime.now(timezone.utc) + timedelta(hours=8)
    now_str = now.strftime("%Y-%m-%d %H:%M")

    records = []
    carbon_total = 0  # 總碳排放

    for row in aaData:
        if not row or "b>" not in row[0]:
            continue

        # 萃取能源類型
        energy_type = row[0].split(">")[-2].replace("</b", "").strip()

        try:
            capacity = float(row[3])  # 裝置容量
            actual = float(row[4])    # 實際發電
            percent = round((actual / capacity * 100) if capacity > 0 else 0, 2)
        except (ValueError, IndexError):
            continue

        # 計算碳排放
        carbon = carbon_factors.get(energy_type, 0) * actual
        carbon_total += carbon

        records.append({
            "時間": now_str,
            "能源類型": energy_type,
            "發電量(MW)": actual,
            "百分比(%)": percent
        })

    df = pd.DataFrame(records)

    # === Step 1: 聚合避免重複 ===
    grouped = df.groupby(["時間", "能源類型"]).agg({
        "發電量(MW)": "sum",
        "百分比(%)": "sum"
    }).reset_index()

    # === Step 2: Pivot 發電量 ===
    pivot_gen = grouped.pivot(index="時間", columns="能源類型", values="發電量(MW)").fillna(0)

    # === Step 3: Pivot 百分比，加上 (%) 後綴 ===
    pivot_percent = grouped.pivot(index="時間", columns="能源類型", values="百分比(%)").fillna(0)
    pivot_percent.columns = [f"{col}(%)" for col in pivot_percent.columns]

    # === Step 4: 合併兩張表 ===
    pivot_df = pivot_gen.join(pivot_percent)

    # === Step 5: 新增碳排與每度電碳排 ===
    total_gen_mw = grouped["發電量(MW)"].sum()
    total_gen_kwh = total_gen_mw * 1000  # MW → kWh
    emission_per_kwh = round((carbon_total * 1000 / total_gen_kwh), 6) if total_gen_kwh > 0 else 0

    pivot_df["碳排放量(TCO₂)"] = round(carbon_total, 2)
    pivot_df["每度電碳排(kgCO₂/kWh)"] = emission_per_kwh

    # === Step 6: 檔案初始化 / 合併舊資料 ===
    if os.path.exists(file_path):
        old_df = pd.read_csv(file_path, index_col=0)
        combined_df = pd.concat([old_df, pivot_df])
        combined_df = combined_df[~combined_df.index.duplicated(keep="last")]
    else:
        combined_df = pivot_df

    # === Step 7: 儲存 ===
    combined_df.to_csv(file_path, encoding="utf-8-sig")
    print(f"✅ 資料已更新並儲存到 {file_path}")

if __name__ == "__main__":
    crawl_taipower()
