from pathlib import Path
import pandas as pd
import pyodbc
import os
from datetime import datetime

FILE_PATTERNS = {
    "zlecenia": {
        "folder": "zlecenia",
        "column_map": {
            "Nr zlecenia": "ZlecenieId",
            "Rodzaj": "ZlecenieRodzaj",
            "Data": "DataUtworzenia",
            "Godzina": "CzasUtworzenia"
        },
        "target_table": "Zlecenia",
        "columns": ["ZlecenieId", "ZlecenieRodzaj", "DataUtworzenia", "CzasUtworzenia"],
        "dtypes": {
            "ZlecenieId": "int",
            "ZlecenieRodzaj": "str",
            "DataUtworzenia": "date",
            "CzasUtworzenia": "time"
        },
        "primary_key": "ZlecenieId"
    },
    "zawiadomienia": {
        "folder": "zawiadomienia",
        "column_map": {
            "Zawiadomienie": "ZawiadomienieId",
            "Rodzaj": "ZawiadomienieRodzaj",
            "Nr zlecenia": "ZlecenieId",
            "Lokalizacja": "Lokalizacja",
            "Lokalizacja funkcjonalna": "LokalizacjaFunkcjonalnaId",
            "Urządzenie": "UrzadzenieId",
            "Utworzono dnia": "DataUtworzenia",
            "Kod uszkodzenia": "UszkodzenieId",
            "Kod przyczyny": "PrzyczynaId",
            "Początek zakłócenia": "DataPoczatkuZaklocenia",
            "Koniec zakłócenia": "DataKoncaZaklocenia",
            "Pocz. zakłóc. (godz.)": "CzasPoczatkuZaklocenia",
            "Koniec zakłóc.(godz.)": "CzasKoncaZaklocenia",
            "Przestój": "Przestoj",
            "Czas przestoju": "CzasPrzestoju",
            "Jedn. czasu przest.": "JednostkaCzasu"
        },
        "target_table": "Zawiadomienia",
        "columns": [
            "ZawiadomienieId", "ZawiadomienieRodzaj", "ZlecenieId", "Lokalizacja",
            "LokalizacjaFunkcjonalnaId", "UrzadzenieId", "DataUtworzenia", "UszkodzenieId",
            "PrzyczynaId", "DataPoczatkuZaklocenia", "DataKoncaZaklocenia",
            "CzasPoczatkuZaklocenia", "CzasKoncaZaklocenia", "Przestoj",
            "CzasPrzestoju", "JednostkaCzasu"
        ],
        "dtypes": {
            "ZawiadomienieId": "int",
            "ZawiadomienieRodzaj": "str",
            "ZlecenieId": "int",
            "Lokalizacja": "str",
            "LokalizacjaFunkcjonalnaId": "str",
            "UrzadzenieId": "int",
            "DataUtworzenia": "date",
            "UszkodzenieId": "int",
            "PrzyczynaId": "int",
            "DataPoczatkuZaklocenia": "date",
            "DataKoncaZaklocenia": "date",
            "CzasPoczatkuZaklocenia": "time",
            "CzasKoncaZaklocenia": "time",
            "Przestoj": "str",
            "CzasPrzestoju": "float",
            "JednostkaCzasu": "str"
        },
        "primary_key": "ZawiadomienieId"
    },
    "linie": {
        "folder": "lokalizacja_funkcjonalna",
        "column_map": {
            "Lokaliz. funkc.": "LiniaId",
            "Oznaczenie": "LiniaNazwa"
        },
        "target_table": "Linie",
        "columns": ["LiniaId", "LiniaNazwa"],
        "dtypes": {
            "LiniaId": "str",
            "LiniaNazwa": "str"
        },
        "primary_key": "LiniaId"
    },
    "lokalizacja_funkcjonalna": {
        "folder": "lokalizacja_funkcjonalna",
        "column_map": {
            "Lokaliz. funkc.": "LokalizacjaFunkcjonalnaId",
            "Oznaczenie": "LokalizacjaFunkcjonalnaNazwa",
            "LiniaId": "LiniaId"
        },
        "target_table": "LokalizacjaFunkcjonalna",
        "columns": ["LokalizacjaFunkcjonalnaId", "LokalizacjaFunkcjonalnaNazwa", "LiniaId"],
        "dtypes": {
            "LokalizacjaFunkcjonalnaId": "str",
            "LokalizacjaFunkcjonalnaNazwa": "str",
            "LiniaId": "str"
        },
        "primary_key": "LokalizacjaFunkcjonalnaId"
    },
    "przyczyny": {
        "folder": "zawiadomienia",
        "column_map": {
            "Kod przyczyny": "PrzyczynaId",
            "Tekst kodu przyczyny": "PrzyczynaNazwa"
        },
        "target_table": "Przyczyny",
        "columns": ["PrzyczynaId", "PrzyczynaNazwa"],
        "dtypes": {
            "PrzyczynaId": "int",
            "PrzyczynaNazwa": "str"
        },
        "primary_key": "PrzyczynaId"
    },
    "uszkodzenia": {
        "folder": "zawiadomienia",
        "column_map": {
            "Kod uszkodzenia": "UszkodzenieId",
            "Tekst dot. szkody": "UszkodzenieNazwa"
        },
        "target_table": "Uszkodzenia",
        "columns": ["UszkodzenieId", "UszkodzenieNazwa"],
        "dtypes": {
            "UszkodzenieId": "int",
            "UszkodzenieNazwa": "str"
        },
        "primary_key": "UszkodzenieId"
    },
    "urzadzenia": {
        "folder": "urzadzenia",
        "column_map": {
            "Urządzenie": "UrzadzenieId",
            "Oznaczenie obiektu technicznego": "UrzadzenieNazwa"
        },
        "target_table": "Urzadzenia",
        "columns": ["UrzadzenieId", "UrzadzenieNazwa"],
        "dtypes": {
            "UrzadzenieId": "int",
            "UrzadzenieNazwa": "str"
        },
        "primary_key": "UrzadzenieId"
    },
    "rodzaje_zawiadomienia": {
        "folder": "rodzaje_zawiadomienia",
        "column_map": {
            "Rdz.": "ZawiadomienieRodzaj",
            "Rodzaj zawiadomienia": "ZawiadomienieRodzajNazwa"
        },
        "target_table": "RodzajeZawiadomienia",
        "columns": ["ZawiadomienieRodzaj", "ZawiadomienieRodzajNazwa"],
        "dtypes": {
            "ZawiadomienieRodzaj": "str",
            "ZawiadomienieRodzajNazwa": "str"
        },
        "primary_key": "ZawiadomienieRodzaj"
    },
    "rodzaje_zlecenia": {
        "folder": "rodzaje_zlecenia",
        "column_map": {
            "Rdz.": "ZlecenieRodzaj",
            "Oznaczenie": "ZlecenieRodzajNazwa"
        },
        "target_table": "RodzajeZlecenia",
        "columns": ["ZlecenieRodzaj", "ZlecenieRodzajNazwa"],
        "dtypes": {
            "ZlecenieRodzaj": "str",
            "ZlecenieRodzajNazwa": "str"
        },
        "primary_key": "ZlecenieRodzaj"
    },
    "bilans_produkcji": {
        "folder": "bilans",
        "column_map": {
            "Od": "Od",
            "Do": "Do",
            "Linia": "LiniaId",
            "Rodzina": "Rodzina",
            "QLTotalAkt": "QLTotalAkt",
            "QLTotalPln": "QLTotalPln",
            "ProcentDvtProduk": "ProcentDvtProduk",
            "ZmianaCzysty": "ZmianaCzysty",
            "ZmianaPrg": "ZmianaPrg",
            "ZmianaStd": "ZmianaStd",
            "QZmianaAkt": "QZmianaAkt",
            "QZmianaDocel": "QZmianaDocel",
            "QZmianaStd": "QZmianaStd",
            "QCPKAkt": "QCPKAkt",
            "QCPKDocel": "QCPKDocel",
            "QCPKStd": "QCPKStd",
            "OpeLNShAkt": "OpeLNShAkt",
            "OpeLNShDocel": "OpeLNShDocel",
            "OpeLNShStd": "OpeLNShStd",
            "OpeELShAkt": "OpeELShAkt",
            "OpeELShDocel": "OpeELShDocel",
            "OpeELShStd": "OpeELShStd",
            "GQLAkt": "GQLAkt",
            "GQLDocel": "GQLDocel",
            "GQLStd": "GQLStd",
            "ProcentSCEff": "ProcentSCEff",
            "ProcentSCStd": "ProcentSCStd",
            "ProcentSREff": "ProcentSREff",
            "ProcentSRStd": "ProcentSRStd",
            "ProcentSFSPEff": "ProcentSFSPEff",
            "ProcentSFSPStd": "ProcentSFSPStd",
            "GodzPracAkt": "GodzPracAkt",
            "GodzPracDocel": "GodzPracDocel",
            "GodzPracStd": "GodzPracStd",
            "ProcentELiniaEff": "ProcentELiniaEff",
            "ProcentELiniaObb": "ProcentELiniaObb",
            "ProcentELiniaStd": "ProcentELiniaStd",
            "ProcentEPracEff": "ProcentEPracEff",
            "ProcentEPracObb": "ProcentEPracObb",
            "ProcentEPracStd": "ProcentEPracStd",
            "ProcentZyskuEff": "ProcentZyskuEff",
            "ProcentZyskuObb": "ProcentZyskuObb",
            "ProcentZyskuStd": "ProcentZyskuStd"
        },
        "target_table": "BilansProdukcji",
        "columns": [
            "Od", "Do", "LiniaId", "Rodzina", "QLTotalAkt", "QLTotalPln", "ProcentDvtProduk",
            "ZmianaCzysty", "ZmianaPrg", "ZmianaStd", "QZmianaAkt", "QZmianaDocel", "QZmianaStd",
            "QCPKAkt", "QCPKDocel", "QCPKStd", "OpeLNShAkt", "OpeLNShDocel", "OpeLNShStd",
            "OpeELShAkt", "OpeELShDocel", "OpeELShStd", "GQLAkt", "GQLDocel", "GQLStd",
            "ProcentSCEff", "ProcentSCStd", "ProcentSREff", "ProcentSRStd",
            "ProcentSFSPEff", "ProcentSFSPStd", "GodzPracAkt", "GodzPracDocel", "GodzPracStd",
            "ProcentELiniaEff", "ProcentELiniaObb", "ProcentELiniaStd",
            "ProcentEPracEff", "ProcentEPracObb", "ProcentEPracStd",
            "ProcentZyskuEff", "ProcentZyskuObb", "ProcentZyskuStd"
        ],
        "dtypes": {
            "Od": "date",
            "Do": "date",
            "LiniaId": "int",
            "Rodzina": "str",
            "QLTotalAkt": "float",
            "QLTotalPln": "float",
            "ProcentDvtProduk": "float",
            "ZmianaCzysty": "float",
            "ZmianaPrg": "float",
            "ZmianaStd": "float",
            "QZmianaAkt": "float",
            "QZmianaDocel": "float",
            "QZmianaStd": "float",
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
            "ProcentELiniaEff": "float",
            "ProcentELiniaObb": "float",
            "ProcentELiniaStd": "float",
            "ProcentEPracEff": "float",
            "ProcentEPracObb": "float",
            "ProcentEPracStd": "float",
            "ProcentZyskuEff": "float",
            "ProcentZyskuObb": "float",
            "ProcentZyskuStd": "float"
        }
    }
}

BASE_FOLDER = Path("G:/projekt")

# SQL SERVER
CONN_STR = (
    "DRIVER={};"
    "SERVER=;"
    "DATABASE=;"
    "UID=;"
    "PWD="
)

def extract_linia_from_lokalizacja(lok: str) -> str | None:
    parts = lok.split("-")
    if len(parts) == 5:
        return parts[-2]
    return None

def remove_duplicates_by_primary_key(df: pd.DataFrame, primary_key: str) -> pd.DataFrame:
    if primary_key in df.columns:
        return df.drop_duplicates(subset=primary_key)
    return df

def read_od_do(file_path: Path):
    with open(file_path, encoding="utf-8") as f:
        first_line = f.readline()

    import re
    od_match = re.search(r"Od\s+(\d{2}\.\d{2}\.\d{4})", first_line)
    do_match = re.search(r"Do\s+(\d{2}\.\d{2}\.\d{4})", first_line)

    od_date = pd.to_datetime(od_match.group(1), dayfirst=True).date() if od_match else None
    do_date = pd.to_datetime(do_match.group(1), dayfirst=True).date() if do_match else None

    return od_date, do_date

def process_csv(file_path: Path, settings: dict, conn):
    df = pd.read_csv(file_path, dtype=str)

    if settings["target_table"] == "LokalizacjaFunkcjonalna":
        df.rename(columns=settings["column_map"], inplace=True)
        df = df[df["LokalizacjaFunkcjonalnaId"].str.count("-") >= 4].copy()
        df["LiniaId"] = df["LokalizacjaFunkcjonalnaId"].apply(extract_linia_from_lokalizacja)

    elif settings["target_table"] == "Linie":
        df.rename(columns=settings["column_map"], inplace=True)
        df["LiniaId"] = df["LokalizacjaFunkcjonalnaId"].apply(extract_linia_from_lokalizacja)
        df = df[df["LiniaId"].notna()]
        
        df = df.groupby("LiniaId", as_index=False)["Nazwa"].first()
        df = df.rename(columns={"Nazwa": "LiniaNazwa"})

    elif settings["target_table"] == "BilansProdukcji":
        header_row_1 = df.iloc[2].fillna("").astype(str).str.strip()
        header_row_2 = df.iloc[3].fillna("").astype(str).str.strip()
        combined_headers = [
            (a + " " + b).strip().replace(".", "").replace("/", "").replace(" ", "")
            for a, b in zip(header_row_1, header_row_2)
        ]

        df = df.iloc[5:].copy()
        df.columns = combined_headers
        df.dropna(how="all", inplace=True)
        df.reset_index(drop=True, inplace=True)

        od_date, do_date = read_od_do(file_path)
        df["Od"] = od_date
        df["Do"] = do_date

    else:
        df.rename(columns=settings["column_map"], inplace=True)
        df = df[settings["columns"]]

    for col, dtype in settings["dtypes"].items():
        try:
            if dtype == "int":
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
            elif dtype == "float":
                df[col] = pd.to_numeric(df[col], errors="coerce")
            elif dtype == "date":
                df[col] = pd.to_datetime(df[col], errors="coerce").dt.date
            elif dtype == "time":
                df[col] = pd.to_datetime(df[col], errors="coerce").dt.time
            elif dtype == "str":
                df[col] = df[col].astype(str)
        except Exception as e:
            print(f"Failed to convert column {col} to {dtype}: {e}")

    primary_key = settings["columns"][0]
    df.dropna(subset=[primary_key], inplace=True)

    df = remove_duplicates_by_primary_key(df, primary_key)

    cursor = conn.cursor()
    placeholders = ", ".join(["?"] * len(settings["columns"]))
    col_names = ", ".join(settings["columns"])
    sql = f"INSERT INTO {settings['target_table']} ({col_names}) VALUES ({placeholders})"

    for _, row in df.iterrows():
        try:
            cursor.execute(sql, tuple(row[col] for col in settings["columns"]))
        except Exception as e:
            print(f"Row insert failed in {settings['target_table']}: {e}")

    conn.commit()

def main():
    conn = pyodbc.connect(CONN_STR)

    for key, settings in FILE_PATTERNS.items():
        folder = BASE_FOLDER / settings["folder"]
        if not folder.exists():
            continue

        for file_path in folder.glob("*.csv"):
            try:
                process_csv(file_path, settings, conn)
            except Exception as e:
                print(f"Error processing {file_path.name}: {e}")

    conn.close()

if __name__ == "__main__":
    main()

