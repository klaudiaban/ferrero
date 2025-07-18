from pathlib import Path
import pandas as pd
import pyodbc
import os
from datetime import datetime

FOLDER_PATH = Path("G:/projekt")

FILE_PATTERNS = {
    "zlecenia": {
        "filename_contains": "zlecenia",
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
        "filename_contains": "zawiadomienia",
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
        "filename_contains": "lokalizacja_funkcjonalna",
        "column_map": {
            "Lokaliz. funkc.": "Linia",
            "Oznaczenie": "LiniaNazwa"
        },
        "target_table": "Linie",
        "columns": ["Linia", "LiniaNazwa"],
        "dtypes": {
            "Linia": "str",
            "LiniaNazwa": "str"
        },
        "primary_key": "Linia"
    },
    "lokalizacja_funkcjonalna": {
        "filename_contains": "lokalizacja_funkcjonalna",
        "column_map": {
            "Lokaliz. funkc.": "LokalizacjaFunkcjonalnaId",
            "Oznaczenie": "LokalizacjaFunkcjonalnaNazwa",
            "Linia": "Linia"
        },
        "target_table": "LokalizacjaFunkcjonalna",
        "columns": ["LokalizacjaFunkcjonalnaId", "LokalizacjaFunkcjonalnaNazwa", "Linia"],
        "dtypes": {
            "LokalizacjaFunkcjonalnaId": "str",
            "LokalizacjaFunkcjonalnaNazwa": "str",
            "Linia": "str"
        },
        "primary_key": "LokalizacjaFunkcjonalnaId"
    },
    "przyczyny": {
        "filename_contains": "zawiadomienia",
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
        "filename_contains": "zawiadomienia",
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
        "filename_contains": "urzadzenia",
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
        "filename_contains": "rodzaje_zawiadomienia",
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
        "filename_contains": "rodzaje_zlecenia",
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
    }
}


# SQL SERVER
CONN_STR = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=your_server.database.windows.net;"
    "DATABASE=your_database;"
    "UID=your_username;"
    "PWD=your_password"
)

def remove_duplicates_by_primary_key(df: pd.DataFrame, primary_key: str) -> pd.DataFrame:
    if primary_key in df.columns:
        return df.drop_duplicates(subset=primary_key)
    else:
        return df

def process_csv(file_path: Path, settings: dict, conn):
    df = pd.read_csv(file_path)

    df.rename(columns=settings["column_map"], inplace=True)
    df = df[settings["columns"]]

    for col, dtype in settings.get("dtypes", {}).items():
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

    # Get primary key column name
    primary_key = settings.get("primary_key")
    if primary_key:
        df = remove_duplicates_by_primary_key(df, primary_key)

    cursor = conn.cursor()

    columns = settings["columns"]
    col_names = ", ".join(columns)
    placeholders = ", ".join(["?"] * len(columns))
    sql = f"INSERT INTO {settings['target_table']} ({col_names}) VALUES ({placeholders})"

    for _, row in df.iterrows():
        try:
            cursor.execute(sql, tuple(row[col] for col in columns))
        except Exception as e:
            print(f"Row insert failed: {e}")

    conn.commit()
    file_path.rename(file_path.with_suffix(".imported.csv"))


def main():
    conn = pyodbc.connect(CONN_STR)
    for file_path in FOLDER_PATH.glob("*.csv"):
        for key, settings in FILE_PATTERNS.items():
            if settings["filename_contains"].lower() in file_path.name.lower():
                try:
                    process_csv(file_path, settings, conn)
                except Exception as e:
                    print(f"Error processing {file_path.name}: {e}")
                break
    conn.close()

if __name__ == "__main__":
    main()
