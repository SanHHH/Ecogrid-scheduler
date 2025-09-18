import requests
import pandas as pd
from datetime import datetime, timedelta, timezone
import os

# 定義台灣時區
tz = timezone(timedelta(hours=8))

# 碳排放係數 (kgCO₂/kWh, 這裡先用相對數值，實務可依官方資料更新)
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

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
file_path = os.path.join(BASE_DIR, "taipower_emission_data.csv")

def crawl_taipower():
    url = "https://www.taipower.com.tw/d006/loadGraph/loadGraph/data/genary.json"
    response = requests.get(url)
    response.raise_for_status()

    data = response.json().get("aaData", [])

    # 使用台灣時間
    now = datetime.now(tz).strftime("%Y-%m-%d %H:%M")

    records = []
    carbon_total = 0

    for row in data:
        if not row or "b>" not in row[0]:
            continue

        energy_type = row[0].split(">")[-2].replace("</b", "").strip()

        try:
            capacity = float(row[3])
            actual = float(row[4])
        except (ValueError, IndexError):
            continue

        percent = round((actual / capacity * 100), 2) if capacity > 0 else 0

        # 計算碳排
        carbon = carbon_factors.get(energy_type, 0) * actual
        carbon_total += carbon

        records.append({
            "時間": now,
            "能源類型": energy_type,
            "發電量(MW)": actual,
            "百分比(%)": percent
        })

    df = pd.DataFrame(records)

    # 確保所有能源種類都有欄位，沒有的補 0
    pivot_df = df.pivot(index="時間", columns="能源類型", values="發電量(MW)").fillna(0)

    # 同樣建立百分比欄位
    for energy in carbon_factors.keys():
        if energy not in pivot_df.columns:
            pivot_df[energy] = 0

    percent_df = df.pivot(index="時間", columns="能源類型", values="百分比(%)").fillna(0)
    for energy in carbon_factors.keys():
        colname = f"{energy}(%)"
        if energy in percent_df.columns:
            pivot_df[colname] = percent_df[energy]
        else:
            pivot_df[colname] = 0

    # 計算總發電量與碳排
    total_generation = df["發電量(MW)"].sum()
    pivot_df["總發電量(MW)"] = total_generation
    pivot_df["碳排放量(TCO₂)"] = round(carbon_total, 2)

    if total_generation > 0:
        emission_per_kwh = round(carbon_total / (total_generation * 1000), 6)
    else:
        emission_per_kwh = 0

    pivot_df["每度電碳排(kgCO₂/kWh)"] = emission_per_kwh

    # 初始化檔案（如果不存在）
    if os.path.exists(file_path):
        old_df = pd.read_csv(file_path, index_col=0)
        combined_df = pd.concat([old_df, pivot_df])
        combined_df = combined_df[~combined_df.index.duplicated(keep='last')]
    else:
        combined_df = pivot_df

    combined_df.to_csv(file_path, encoding="utf-8-sig")
    print(f"✅ 資料已更新並儲存到 {file_path}")

if __name__ == "__main__":
    crawl_taipower()
