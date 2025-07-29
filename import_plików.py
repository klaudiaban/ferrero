import pyodbc
import pandas as pd
from sqlalchemy import create_engine
import os

server = 'JASNACZERN'        
database = 'ferrero'
username = 'sa'
password = '1234'
driver = 'ODBC Driver 17 for SQL Server'  
conn_str = f"mssql+pyodbc://{username}:{password}@{server}/{database}?driver={driver.replace(' ', '+')}"
engine = create_engine(conn_str)

# =============== ZLECENIA ===============

csv_folder_zlecenia = 'zlecenia'
try:
    csv_file_zlecenia = next(f for f in os.listdir(csv_folder_zlecenia) if f.lower().endswith('.csv'))
    file_path_zlecenia = os.path.join(csv_folder_zlecenia, csv_file_zlecenia)

    zlecenia = pd.read_csv(file_path_zlecenia, encoding='utf-8-sig', sep=';')
    zlecenia = zlecenia[["Nr zlecenia", "Rodzaj zlecenia", "Data wprowadzenia", "Godzina utworzenia"]]
    zlecenia.columns = ["ZlecenieId", "ZlecenieRodzaj", "DataUtworzenia", "CzasUtworzenia"]

    zlecenia["ZlecenieId"] = pd.to_numeric(zlecenia["ZlecenieId"], errors="coerce")
    zlecenia["DataUtworzenia"] = pd.to_datetime(zlecenia["DataUtworzenia"], format="%d.%m.%Y", errors="coerce").dt.date
    zlecenia["CzasUtworzenia"] = pd.to_datetime(zlecenia["CzasUtworzenia"], format="%H:%M:%S", errors="coerce").dt.time

    with engine.connect() as conn:
        existing_ids = pd.read_sql("SELECT ZlecenieId FROM Zlecenia", conn)["ZlecenieId"].tolist()

    zlecenia = zlecenia[~zlecenia["ZlecenieId"].isin(existing_ids)]

    if not zlecenia.empty:
        zlecenia.to_sql('Zlecenia', con=engine, if_exists='append', index=False)
        print(f"Inserted {len(zlecenia)} new records from {csv_file_zlecenia} into the Zlecenia table.")
    else:
        print(f"No new records to insert from {csv_file_zlecenia} into the Zlecenia table.")

    os.remove(file_path_zlecenia)
except StopIteration:
    print("No .csv files found in the folder:", csv_folder_zlecenia)

# =============== ZAWIADOMIENIA =================

csv_folder_zawiadomienia = 'zawiadomienia'
try:
    csv_file_zawiadomienia = next(f for f in os.listdir(csv_folder_zawiadomienia) if f.endswith('.csv'))
    file_path_zawiadomienia = os.path.join(csv_folder_zawiadomienia, csv_file_zawiadomienia)

    zawiadomienia = pd.read_csv(file_path_zawiadomienia, encoding='utf-8-sig', sep=';')

    zawiadomienia = zawiadomienia[["Zawiadomienie", "Rodzaj zawiadomienia", "Nr zlecenia", "Lokalizacja", "Lokalizacja funkc.", "Urządzenie", "Utworzono dnia", "Kod uszkodzenia", "Kod przyczyny", "Początek zakłócenia", "Koniec zakłócenia", "Pocz. zakłóc. (godz.)", "Koniec zakłóc.(godz.)", "Przestój", "Czas przestoju", "Jedn. czasu przest."]]
    zawiadomienia.columns = ["ZawiadomienieId", "ZawiadomienieRodzaj", "ZlecenieId", "Lokalizacja", "LokalizacjaFunkcjonalnaId", "UrzadzenieId", "DataUtworzenia", "UszkodzenieId", "PrzyczynaId", "DataPoczatkuZaklocenia", "DataKoncaZaklocenia", "CzasPoczatkuZaklocenia", "CzasKoncaZaklocenia", "Przestoj", "CzasPrzestoju", "JednostkaCzasu"]

    zawiadomienia["ZawiadomienieId"] = pd.to_numeric(zawiadomienia["ZawiadomienieId"], errors="coerce", downcast="integer")
    zawiadomienia["ZlecenieId"] = pd.to_numeric(zawiadomienia["ZlecenieId"], errors="coerce")
    zawiadomienia["UrzadzenieId"] = pd.to_numeric(zawiadomienia["UrzadzenieId"], errors="coerce", downcast="integer")
    zawiadomienia["UszkodzenieId"] = pd.to_numeric(zawiadomienia["UszkodzenieId"], errors="coerce", downcast="integer")
    zawiadomienia["PrzyczynaId"] = pd.to_numeric(zawiadomienia["PrzyczynaId"], errors="coerce", downcast="integer")

    date_cols = ["DataUtworzenia", "DataPoczatkuZaklocenia", "DataKoncaZaklocenia"]
    for col in date_cols:
        zawiadomienia[col] = pd.to_datetime(zawiadomienia[col], format="%d.%m.%Y", errors="coerce").dt.date

    time_cols = ["CzasPoczatkuZaklocenia", "CzasKoncaZaklocenia"]
    for col in time_cols:
        zawiadomienia[col] = pd.to_datetime(zawiadomienia[col], format="%H:%M:%S", errors="coerce").dt.time

    zawiadomienia["CzasPrzestoju"] = zawiadomienia["CzasPrzestoju"].str.replace(",", ".", regex=False)
    zawiadomienia["CzasPrzestoju"] = pd.to_numeric(zawiadomienia["CzasPrzestoju"], errors="coerce")

    with engine.connect() as conn:
        existing_ids = pd.read_sql("SELECT ZawiadomienieId FROM Zawiadomienia", conn)["ZawiadomienieId"].tolist()

    zawiadomienia = zawiadomienia[~zawiadomienia["ZawiadomienieId"].isin(existing_ids)]

    if not zawiadomienia.empty:
        zawiadomienia.to_sql('Zawiadomienia', con=engine, if_exists='append', index=False)
        print(f"Inserted {len(zawiadomienia)} new records from {csv_file_zawiadomienia} into the Zawiadomienia table.")
    else:
        print(f"No new records to insert from {csv_file_zawiadomienia} into the Zawiadomienia table.")

    os.remove(file_path_zawiadomienia)
except StopIteration:
    print("No .csv files found in the folder:", csv_folder_zawiadomienia)

# =============== BILANS PRODUKCJI ===============

csv_folder_bilansprodukcji = 'bilansprodukcji'
try:
    csv_file_bilansprodukcji = next(f for f in os.listdir(csv_folder_bilansprodukcji) if f.endswith('.csv'))
    file_path_bilansprodukcji = os.path.join(csv_folder_bilansprodukcji, csv_file_bilansprodukcji)

    bilansprodukcji = pd.read_csv(file_path_bilansprodukcji, encoding='utf-8-sig', sep=';')

    with open(file_path_bilansprodukcji, encoding="utf-8") as f:
        first_line = f.readline()

    import re
    od_match = re.search(r"Od\s+(\d{2}\.\d{2}\.\d{4})", first_line)
    do_match = re.search(r"Do\s+(\d{2}\.\d{2}\.\d{4})", first_line)

    od_date = pd.to_datetime(od_match.group(1), dayfirst=True).date() if od_match else None
    do_date = pd.to_datetime(do_match.group(1), dayfirst=True).date() if do_match else None

    header_row_1 = bilansprodukcji.iloc[2].fillna("").astype(str).str.strip()
    header_row_2 = bilansprodukcji.iloc[3].fillna("").astype(str).str.strip()
    combined_headers = [
        (a + " " + b).strip().replace(".", "").replace("/", "").replace(" ", "")
        for a, b in zip(header_row_1, header_row_2)
    ]

    bilansprodukcji = bilansprodukcji.iloc[5:].copy()
    bilansprodukcji.columns = combined_headers
    bilansprodukcji.dropna(how="all", inplace=True)
    bilansprodukcji.reset_index(drop=True, inplace=True)

    bilansprodukcji["Od"] = od_date
    bilansprodukcji["Do"] = do_date
    bilansprodukcji.columns = [col.replace('%', 'Procent') for col in bilansprodukcji.columns]

    float_columns = [
        "QLTOTAkt", "QLTOTPln", "ProcentDvtProduk", "ZmianaCzysty", "ZmianaPrg", "ZmianaStd",
        "QZmAkt", "QZmDocel", "QZmStd", "QCPKAkt", "QCPKDocel", "QCPKStd",
        "OpeLNShAkt", "OpeLNShDocel", "OpeLNShStd", "OpeELShAkt", "OpeELShDocel", "OpeELShStd",
        "GQLAkt", "GQLDocel", "GQLStd", "ProcentSCEff", "ProcentSCStd", "ProcentSREff", "ProcentSRStd",
        "ProcentSFSPEff", "ProcentSFSPStd", "GodzPracAkt", "GodzPracDocel", "GodzPracStd",
        "ProcentELINIAEff", "ProcentELINIAObb", "ProcentELINIAStd",
        "ProcentEPracEff", "ProcentEPracObb", "ProcentEPracStd",
        "ProcentZyskuEff", "ProcentZyskuObb", "ProcentZyskuStd"
    ]

    bilansprodukcji = bilansprodukcji.loc[:, ~bilansprodukcji.columns.duplicated()]
    bilansprodukcji.drop(columns=["QZmStandard"], inplace=True)
    for col in float_columns:
        bilansprodukcji.loc[:, col] = bilansprodukcji[col].apply(lambda x: str(x).replace(' ', '').replace(',', '.'))
        bilansprodukcji.loc[:, col] = pd.to_numeric(bilansprodukcji[col], errors='coerce')

    bilansprodukcji = bilansprodukcji.astype({
        "Od": "datetime64[ns]",
        "Do": "datetime64[ns]",
        "Linia": "int",
        "Rodzina": "string",
        "QLTOTAkt": "float",
        "QLTOTPln": "float",
        "ProcentDvtProduk": "float",
        "ZmianaCzysty": "float",
        "ZmianaPrg": "float",
        "ZmianaStd": "float",
        "QZmAkt": "float",
        "QZmDocel": "float",
        "QZmStd": "float",
        "QCPKAkt": "float",
        "QCPKDocel": "float",
        "QCPKStd": "float",
        "OpeLNShAkt": "float",
        "OpeLNShDocel": "float",
        "OpeLNShStd": "float",
        "OpeELShAkt": "float",
        "OpeELShDocel": "float",
        "OpeELShStd": "float",
        "GQLAkt": "float",
        "GQLDocel": "float",
        "GQLStd": "float",
        "ProcentSCEff": "float",
        "ProcentSCStd": "float",
        "ProcentSREff": "float",
        "ProcentSRStd": "float",
        "ProcentSFSPEff": "float",
        "ProcentSFSPStd": "float",
        "GodzPracAkt": "float",
        "GodzPracDocel": "float",
        "GodzPracStd": "float",
        "ProcentELINIAEff": "float",
        "ProcentELINIAObb": "float",
        "ProcentELINIAStd": "float",
        "ProcentEPracEff": "float",
        "ProcentEPracObb": "float",
        "ProcentEPracStd": "float",
        "ProcentZyskuEff": "float",
        "ProcentZyskuObb": "float",
        "ProcentZyskuStd": "float"
    })

    bilansprodukcji.to_sql('BilansProdukcji', con=engine, if_exists='append', index=False)
    print(f"Inserted {len(bilansprodukcji)} new records from {csv_file_bilansprodukcji} into the BilansProdukcji table.")

    os.remove(file_path_bilansprodukcji)
except StopIteration:
    print("No .csv files found in the folder:", csv_folder_bilansprodukcji)