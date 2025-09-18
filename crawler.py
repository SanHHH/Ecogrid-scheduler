import requests
import pandas as pd
from datetime import datetime
import os

# 設定專案目錄
BASE_DIR = os.path.abspath(os.path.dirname(__file__))
file_path = os.path.join(BASE_DIR, "taipower_emission_data.csv")

# 碳排係數
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

# 固定能源欄位順序
energy_columns = list(carbon_factors.keys())

def crawl_taipower():
    url = "https://www.taipower.com.tw/d006/loadGraph/loadGraph/data/genary.json"
    response = requests.get(url)
    response.raise_for_status()

    data = response.json()
    aaData = data.get("aaData", [])

    now = datetime.now().strftime("%Y-%m-%d %H:%M")

    records = []
    carbon_total = 0  # 當下時間碳排總和

    for row in aaData:
        if not row or "b>" not in row[0]:
            continue

        # 萃取能源類型
        energy_type = row[0].split(">")[-2].replace("</b", "").strip()
        try:
            capacity = float(row[3])
            actual = float(row[4])
        except (ValueError, IndexError):
            continue

        # 計算碳排放量
        carbon = carbon_factors.get(energy_type, 0) * actual
        carbon_total += carbon

        records.append({
            "時間": now,
            "能源類型": energy_type,
            "發電量": actual
        })

    df = pd.DataFrame(records)

    # 對同一時間、能源類型彙總
    grouped = df.groupby(["時間", "能源類型"]).agg({
        "發電量": "sum"
    }).reset_index()

    # Pivot：時間 x 能源類型
    pivot_df = grouped.pivot(index="時間", columns="能源類型", values="發電量")

    # 確保所有能源欄位都存在（缺少的填 0）
    pivot_df = pivot_df.reindex(columns=energy_columns, fill_value=0)

    # ➕ 新增統計資訊
    total_generation = pivot_df[energy_columns].sum(axis=1).iloc[0]
    renewable = pivot_df[["風力(Wind)", "太陽能(Solar)", "水力(Hydro)", "其它再生能源(Other Renewable Energy)"]].sum(axis=1).iloc[0]
    fossil = pivot_df[["燃煤(Coal)", "燃油(Oil)", "輕油(Diesel)", "燃氣(LNG)", "民營電廠-燃煤(IPP-Coal)", "民營電廠-燃氣(IPP-LNG)", "汽電共生(Co-Gen)"]].sum(axis=1).iloc[0]
    low_carbon = renewable + pivot_df[["核能(Nuclear)"]].sum(axis=1).iloc[0]

    # 計算每度電碳排
    total_generation_kwh = total_generation * 1000  # MW → kWh
    emission_per_kwh = round(carbon_total * 1000 / total_generation_kwh, 6) if total_generation > 0 else 0

    # 附加統計欄位
    pivot_df["總發電量(MW)"] = round(total_generation, 2)
    pivot_df["再生能源佔比(%)"] = round(renewable / total_generation * 100, 2) if total_generation > 0 else 0
    pivot_df["化石燃料佔比(%)"] = round(fossil / total_generation * 100, 2) if total_generation > 0 else 0
    pivot_df["低碳能源佔比(%)"] = round(low_carbon / total_generation * 100, 2) if total_generation > 0 else 0
    pivot_df["碳排放量(TCO2)"] = round(carbon_total, 2)
    pivot_df["每度電碳排(kgCO₂/kWh)"] = emission_per_kwh

    # 🔑 初始化檔案（第一次建立）
    if not os.path.exists(file_path):
        print("📂 檔案不存在，建立新檔案")
        pivot_df.to_csv(file_path, encoding="utf-8-sig")
        return

    # 合併歷史資料（保留唯一時間，欄位對齊）
    old_df = pd.read_csv(file_path, index_col=0)
    # 確保舊資料也有完整欄位
    for col in pivot_df.columns:
        if col not in old_df.columns:
            old_df[col] = 0
    for col in old_df.columns:
        if col not in pivot_df.columns:
            pivot_df[col] = 0

    combined_df = pd.concat([old_df, pivot_df])
    combined_df = combined_df[~combined_df.index.duplicated(keep='last')]

    combined_df.to_csv(file_path, encoding="utf-8-sig")
    print(f"✅ 資料已更新並儲存到 {file_path}")

if __name__ == "__main__":
    crawl_taipower()
