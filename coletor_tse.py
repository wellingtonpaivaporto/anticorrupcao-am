import requests
import pandas as pd
import os
from datetime import datetime

print(f"🗳️ Coleta TSE iniciada: {datetime.now()}")

os.makedirs("dados", exist_ok=True)

# ================================================
# 1. CANDIDATOS DO AMAZONAS (Estado + Manaus 2026)
# ================================================
def coletar_candidatos(ano=2026, uf="AM"):
    url = f"https://dadosabertos.tse.jus.br/dataset/candidatos-{ano}/resource/consulta_cand_{ano}_{uf}.zip"
    try:
        print(f"  Baixando candidatos {uf} {ano}...")
        r = requests.get(url, timeout=60)
        with open(f"dados/candidatos_{uf}_{ano}.zip", "wb") as f:
            f.write(r.content)
        df = pd.read_csv(
            f"dados/candidatos_{uf}_{ano}.zip",
            encoding="latin1",
            sep=";",
            on_bad_lines="skip"
        )
        print(f"  ✅ {len(df)} candidatos encontrados!")
        df.to_csv(f"dados/candidatos_{uf}_{ano}.csv", index=False)
        return df
    except Exception as e:
        print(f"  ❌ Erro: {e}")
        return pd.DataFrame()

# ================================================
# 2. BENS DECLARADOS
# ================================================
def coletar_bens(ano=2026, uf="AM"):
    url = f"https://dadosabertos.tse.jus.br/dataset/candidatos-{ano}/resource/bem_candidato_{ano}_{uf}.zip"
    try:
        print(f"  Baixando bens declarados {uf} {ano}...")
        r = requests.get(url, timeout=60)
        with open(f"dados/bens_{uf}_{ano}.zip", "wb") as f:
            f.write(r.content)
        df = pd.read_csv(
            f"dados/bens_{uf}_{ano}.zip",
            encoding="latin1",
            sep=";",
            on_bad_lines="skip"
        )
        # Total de bens por candidato
        if not df.empty:
            col_cpf = [c for c in df.columns if "CPF" in c.upper()][0]
            col_val = [c for c in df.columns if "VALOR" in c.upper()][0]
            df[col_val] = pd.to_numeric(
                df[col_val].astype(str).str.replace(",", "."),
                errors="coerce"
            )
            total = df.groupby(col_cpf)[col_val].sum().reset_index()
            total.columns = ["CPF", "total_bens"]
            total.to_csv(f"dados/bens_total_{uf}_{ano}.csv", index=False)
            print(f"  ✅ Bens de {len(total)} candidatos calculados!")
            return total
    except Exception as e:
        print(f"  ❌ Erro: {e}")
        return pd.DataFrame()

# ================================================
# 3. DOAÇÕES ELEITORAIS
# ================================================
def coletar_doacoes(ano=2026, uf="AM"):
    url = f"https://dadosabertos.tse.jus.br/dataset/prestacao-de-contas-eleitorais-{ano}/resource/receitas_candidatos_{ano}_{uf}.zip"
    try:
        print(f"  Baixando doações {uf} {ano}...")
        r = requests.get(url, timeout=60)
        with open(f"dados/doacoes_{uf}_{ano}.zip", "wb") as f:
            f.write(r.content)
        df = pd.read_csv(
            f"dados/doacoes_{uf}_{ano}.zip",
            encoding="latin1",
            sep=";",
            on_bad_lines="skip"
        )
        print(f"  ✅ {len(df)} doações encontradas!")
        df.to_csv(f"dados/doacoes_{uf}_{ano}.csv", index=False)
        return df
    except Exception as e:
        print(f"  ❌ Erro: {e}")
        return pd.DataFrame()

# ================================================
# RODA TUDO
# ================================================
print("\n📋 COLETANDO DADOS DO TSE...\n")

df_cand = coletar_candidatos(2026, "AM")
df_bens = coletar_bens(2026, "AM")
df_doacoes = coletar_doacoes(2026, "AM")

print(f"\n✅ Coleta TSE concluída: {datetime.now()}")
print(f"   Candidatos: {len(df_cand)}")
print(f"   Bens: {len(df_bens)}")
print(f"   Doações: {len(df_doacoes)}")
