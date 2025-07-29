import pyodbc
import pandas as pd
from sqlalchemy import create_engine
import os

csv_folder = 'zlecenia'
csv_file = next(f for f in os.listdir(csv_folder) if f.endswith('.csv'))
file_path = os.path.join(csv_folder, csv_file)

zlecenia = pd.read_csv(file_path, encoding='utf-8-sig', sep=';')
zlecenia = zlecenia[["Nr zlecenia", "Rodzaj zlecenia", "Data wprowadzenia", "Godzina utworzenia"]]
zlecenia.columns = ["ZlecenieId", "ZlecenieRodzaj", "DataUtworzenia", "CzasUtworzenia"]

zlecenia["ZlecenieId"] = pd.to_numeric(zlecenia["ZlecenieId"], errors="coerce")
zlecenia["DataUtworzenia"] = pd.to_datetime(zlecenia["DataUtworzenia"], format="%d.%m.%Y", errors="coerce").dt.date
zlecenia["CzasUtworzenia"] = pd.to_datetime(zlecenia["CzasUtworzenia"], format="%H:%M:%S", errors="coerce").dt.time

server = 'JASNACZERN'        
database = 'ferrero'
username = 'sa'
password = '1234'
driver = 'ODBC Driver 17 for SQL Server'  
conn_str = f"mssql+pyodbc://{username}:{password}@{server}/{database}?driver={driver.replace(' ', '+')}"
engine = create_engine(conn_str)

with engine.connect() as conn:
    existing_ids = pd.read_sql("SELECT ZlecenieId FROM Zlecenia", conn)["ZlecenieId"].tolist()

zlecenia = zlecenia[~zlecenia["ZlecenieId"].isin(existing_ids)]

if not zlecenia.empty:
    zlecenia.to_sql('Zlecenia', con=engine, if_exists='append', index=False)
    print(f"Inserted {len(zlecenia)} new records from {csv_file}")
else:
    print(f"No new records to insert from {csv_file}")

os.remove(file_path)
