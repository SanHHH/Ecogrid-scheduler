import requests
import pandas as pd
from datetime import datetime
import os

# 基本路徑設定
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
file_path = os.path.join(BASE_DIR, "taipower_emission_data.csv")

# 所有可能能源類型
energy_types = [
    "燃煤(Coal)", "燃氣(LNG)", "燃油(Oil)", "核能(Nuclear)", "太陽能(Solar)",
    "風力(Wind)", "水力(Hydro)", "汽電共生(Co-Gen)", "民營電廠-燃氣(IPP-LNG)",
    "民營電廠-燃煤(IPP-Coal)", "輕油(Diesel)", "儲能(Energy Storage System)",
    "其它再生能源(Other Renewable Energy)"
]

carbon_factors = {
    '燃煤(Coal)': 0.9,
    '燃氣(LNG)': 0.5,
    '燃油(Oil)': 0.8,
    '核能(Nuclear)': 0,
    '太陽能(Solar)': 0,
    '風力(Wind)': 0,
    '水力(Hydro)': 0,
    '汽電共生(Co-Gen)': 0.6,
    '民營電廠-燃氣(IPP-LNG)': 0.5,
    '民營電廠-燃煤(IPP-Coal)': 0.9,
    '輕油(Diesel)': 0.8,
    '儲能(Energy Storage System)': 0,
    '其它再生能源(Other Renewable Energy)': 0,
}

def crawl_taipower():
    url = "https://www.taipower.com.tw/d006/loadGraph/loadGraph/data/genary.json"
    response = requests.get(url)
    response.raise_for_status()

    data = response.json().get("aaData", [])
    now = datetime.now().strftime("%Y-%m-%d %H:%M")

    records, carbon_total = [], 0
    for row in data:
        if not row or "b>" not in row[0]:
            continue
        energy_type = row[0].split(">")[-2].replace("</b", "").strip()
        try:
            actual = float(row[4])
        except:
            continue

        carbon = carbon_factors.get(energy_type, 0) * actual
        carbon_total += carbon
        records.append({"能源類型": energy_type, "發電量": actual})

    df = pd.DataFrame(records)
    total_gen = df["發電量"].sum() if not df.empty else 0

    # 建立固定結構的 summary
    summary = {
        "時間": now,
        "總發電量(MW)": total_gen,
        "碳排放量(TCO₂)": round(carbon_total, 2),
        "每度電碳排(kgCO₂/kWh)": round(carbon_total / total_gen, 6) if total_gen > 0 else 0,
    }

    # 為每一種能源建立 MW 和 %
    for et in energy_types:
        if not df.empty and et in df["能源類型"].values:
            val = df.loc[df["能源類型"] == et, "發電量"].values[0]
            pct = (val / total_gen * 100) if total_gen > 0 else 0
        else:
            val, pct = 0, 0
        summary[f"{et}(MW)"] = round(val, 2)
        summary[f"{et}(%)"] = f"{round(pct, 2)}%"  # 加上百分號

    df_summary = pd.DataFrame([summary])

    # 合併舊資料
    if os.path.exists(file_path):
        old_df = pd.read_csv(file_path)
        df_summary = pd.concat([old_df, df_summary]).drop_duplicates(subset=["時間"], keep="last")

    # 儲存
    df_summary.to_csv(file_path, index=False, encoding="utf-8-sig")
    print(f"✅ 已更新: {file_path}")

if __name__ == "__main__":
    crawl_taipower()
