import pandas as pd

df = pd.read_csv('sapmaintance.csv', encoding='ISO-8859-1', sep=";")

df.columns = df.columns.str.strip()

df.dropna(subset=["Opis", "Nr zlec.", "Oznaczenie"], inplace=True)

df["Priorytet"] = df["Priorytet"].fillna("Brak")

df["Data zawiadom."] = pd.to_datetime(df["Data zawiadom."], errors="coerce")
df["Nr zlec."] = df["Nr zlec."].astype("Int64")

df.drop_duplicates(inplace=True)

df.reset_index(drop=True, inplace=True)

df["Dzien_tygodnia"] = df["Data zawiadom."].dt.day_name()

df.sort_values(by=["Oznaczenie", "Data zawiadom."], inplace=True)
df["Dni_od_ostatniej_awarii"] = df.groupby("Oznaczenie")["Data zawiadom."].diff().dt.days
df_gaps = df[df["Dni_od_ostatniej_awarii"].notna()]
df = df.drop_duplicates(subset=["Oznaczenie", "Data zawiadom."], keep="first")
result = df_gaps[["Oznaczenie", "Data zawiadom.", "Dni_od_ostatniej_awarii"]]
result = result.sort_values(by=["Oznaczenie", "Data zawiadom."])

df_recent = df[df["Data zawiadom."] >= "2020-01-01"]
filtered = df_recent[["Oznaczenie", "Dni_od_ostatniej_awarii"]].dropna()
mean_gaps = (
    filtered.groupby("Oznaczenie")["Dni_od_ostatniej_awarii"]
    .mean()
    .sort_values(ascending=True)
)

print(mean_gaps[mean_gaps.index.str.contains("pakowaczka", case=False)])
print(result[result["Oznaczenie"].str.contains("pakowaczka", case=False, na=False)].sort_values(by="Data zawiadom.", ascending=False).head(20))

import matplotlib.pyplot as plt

top_machines = df["Oznaczenie"].value_counts().head(10)

top_machines.plot(kind="barh", title="Top 10 maszyn z największą liczbą awarii")
plt.xlabel("Liczba awarii")
plt.ylabel("Nazwa maszyny")
plt.gca().invert_yaxis()  
plt.tight_layout()
plt.show()

