import requests
import pandas as pd
from datetime import datetime
import sqlite3
import os

print(f"Coleta iniciada: {datetime.now()}")

os.makedirs("dados", exist_ok=True)

def coletar_contratos_am():
    url = "https://www.transparencia.am.gov.br/dados-abertos/contratos/csv"
    try:
        r = requests.get(url, timeout=60)
        with open("dados/contratos_am.csv", "wb") as f:
            f.write(r.content)
        df = pd.read_csv("dados/contratos_am.csv", encoding="latin1", sep=None, engine="python")
        print(f"Contratos AM: {len(df)} registros")
        return df
    except Exception as e:
        print(f"Erro: {e}")
        return pd.DataFrame()

def salvar_no_banco(df):
    if df.empty:
        return
    conn = sqlite3.connect("dados/anticorrupcao.db")
    df.to_sql("contratos", conn, if_exists="replace", index=False)
    conn.close()
    print("Banco SQLite atualizado")

def detectar_novidades(df_novo):
    if df_novo.empty:
        return
    arquivo_log = "dados/ultima_coleta.csv"
    if os.path.exists(arquivo_log):
        df_antigo = pd.read_csv(arquivo_log, encoding="latin1")
        col = df_novo.columns[2]
        novos = df_novo[~df_novo[col].isin(df_antigo[col])]
        print(f"{len(novos)} novos contratos detectados!")
        if len(novos) > 0:
            novos.to_csv("dados/novidades.csv", index=False)
    df_novo.to_csv(arquivo_log, index=False)

df = coletar_contratos_am()
salvar_no_banco(df)
detectar_novidades(df)
print(f"Coleta concluida: {datetime.now()}")
