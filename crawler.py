import requests
import pandas as pd
from datetime import datetime, timezone, timedelta
import os

# 碳排放係數
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

# 台電 JSON URL
url = "https://www.taipower.com.tw/d006/loadGraph/loadGraph/data/genary.json"

# 瀏覽器 headers
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/124.0.0.0 Safari/537.36",
    "Referer": "https://www.taipower.com.tw/"
}

# 檔案路徑
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
file_path = os.path.join(BASE_DIR, "taipower_emission_data.csv")

def crawl_taipower():
    # 台灣時間
    taiwan_tz = timezone(timedelta(hours=8))
    now = datetime.now(taiwan_tz).strftime("%Y-%m-%d %H:%M")

    # 發送請求
    response = requests.get(url, headers=headers, timeout=10)
    response.raise_for_status()
    data = response.json().get("aaData", [])

    records, carbon_total = [], 0
    for row in data:
        if not row or "b>" not in row[0]:
            continue

        energy_type = row[0].split(">")[-2].replace("</b", "").strip()
        try:
            capacity = float(row[3])
            actual = float(row[4])
            percent = round((actual / capacity * 100) if capacity > 0 else 0, 2)
        except (ValueError, IndexError):
            continue

        carbon = carbon_factors.get(energy_type, 0) * actual
        carbon_total += carbon

        records.append({
            "時間": now,
            "能源類型": energy_type,
            "發電量(MW)": actual,
            "百分比(%)": percent
        })

    df = pd.DataFrame(records)

    # 彙總
    grouped = df.groupby(["時間", "能源類型"]).agg({"發電量(MW)": "sum"}).reset_index()
    total_by_time = grouped.groupby("時間")["發電量(MW)"].transform("sum")
    grouped["百分比(%)"] = (grouped["發電量(MW)"] / total_by_time * 100).round(2)

    # Pivot → 每種能源獨立欄位
    pivot_df = grouped.pivot_table(index="時間", columns="能源類型", values="發電量(MW)", aggfunc="sum").fillna(0)
    pivot_df = pivot_df.astype(float)

    # 增加百分比欄位
    percent_df = grouped.pivot_table(index="時間", columns="能源類型", values="百分比(%)", aggfunc="sum").fillna(0)
    percent_df.columns = [f"{col}(%)" for col in percent_df.columns]

    # 合併發電量與百分比
    pivot_df = pivot_df.join(percent_df)

    # 加總碳排
    pivot_df["碳排放量(TCO2)"] = round(carbon_total, 2)

    # 每度電碳排
    total_generation_kwh = grouped["發電量(MW)"].sum() * 1000  # MW → kWh
    emission_per_kwh = round(carbon_total * 1000 / total_generation_kwh, 6) if total_generation_kwh > 0 else 0
    pivot_df["每度電碳排(kgCO₂/kWh)"] = emission_per_kwh

    # 如果舊檔存在，合併
    if os.path.exists(file_path):
        old_df = pd.read_csv(file_path, index_col=0)
        combined_df = pd.concat([old_df, pivot_df])
        combined_df = combined_df[~combined_df.index.duplicated(keep="last")]
    else:
        combined_df = pivot_df

    # 存檔
    combined_df.to_csv(file_path, encoding="utf-8-sig")
    print(f"✅ 已更新並儲存到 {file_path}")

if __name__ == "__main__":
    crawl_taipower()
