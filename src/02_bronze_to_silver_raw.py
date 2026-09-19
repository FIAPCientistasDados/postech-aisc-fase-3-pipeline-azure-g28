# ==========================================
# 01_bronze_to_silver_raw
# ==========================================

from pathlib import Path
import pandas as pd
import json
import zipfile
import re

# ------------------------------------------
# Diretórios
# ------------------------------------------

BASE_DIR = Path(
    "/Workspace/Users/alura.conta@gmail.com"
)

BRONZE = BASE_DIR / "bronze"
SILVER = BASE_DIR / "silver"

SILVER.mkdir(
    parents=True,
    exist_ok=True
)

# ------------------------------------------
# Funções auxiliares
# ------------------------------------------

def normalize_columns(df):

    new_cols = []

    for col in df.columns:

        col = str(col).strip().lower()

        col = (
            col.replace("ã", "a")
               .replace("á", "a")
               .replace("à", "a")
               .replace("â", "a")
               .replace("é", "e")
               .replace("ê", "e")
               .replace("í", "i")
               .replace("ó", "o")
               .replace("ô", "o")
               .replace("õ", "o")
               .replace("ú", "u")
               .replace("ç", "c")
        )

        col = re.sub(
            r"[^\w]+",
            "_",
            col
        )

        col = re.sub(
            r"_+",
            "_",
            col
        )

        col = col.strip("_")

        new_cols.append(col)

    df.columns = new_cols

    return df


def save_parquet(df, name):

    path = SILVER / name

    df.to_parquet(
        path,
        index=False
    )

    print(
        f"✅ {name} "
        f"{df.shape}"
    )


# ------------------------------------------
# Municípios
# ------------------------------------------

df = pd.read_csv(
    BRONZE / "ibge_municipios.json"
)

df = normalize_columns(df)

save_parquet(
    df,
    "municipios.parquet"
)

# ------------------------------------------
# Estados
# ------------------------------------------

df = pd.read_csv(
    BRONZE / "ibge_estados.json"
)

df = normalize_columns(df)

save_parquet(
    df,
    "estados.parquet"
)

# ------------------------------------------
# Resultados Municípios
# ------------------------------------------

municipios = []

for arquivo, ano in [

    (
        "resultados_municipios_2023.xlsx",
        2023
    ),

    (
        "resultados_municipios_2024.xlsx",
        2024
    ),

    (
        "resultados_municipios_2025.xlsx",
        2025
    ),
]:

    df = pd.read_excel(
        BRONZE / arquivo
    )

    df = normalize_columns(df)

    df["ano_referencia"] = ano

    municipios.append(df)

df = pd.concat(
    municipios,
    ignore_index=True
)

save_parquet(
    df,
    "resultados_municipios.parquet"
)

# ------------------------------------------
# Resultados UFs
# ------------------------------------------

ufs = []

for arquivo, ano in [

    (
        "resultados_ufs_2023.xlsx",
        2023
    ),

    (
        "resultados_ufs_2024.xlsx",
        2024
    ),

    (
        "resultados_ufs_2025.xlsx",
        2025
    ),
]:

    df = pd.read_excel(
        BRONZE / arquivo
    )

    df = normalize_columns(df)

    df["ano_referencia"] = ano

    ufs.append(df)

df = pd.concat(
    ufs,
    ignore_index=True
)

save_parquet(
    df,
    "resultados_ufs.parquet"
)

# ------------------------------------------
# INSE
# ------------------------------------------

df = pd.read_excel(
    BRONZE / "inse_2023_municipios.xlsx"
)

df = normalize_columns(df)

save_parquet(
    df,
    "inse.parquet"
)

# ------------------------------------------
# Censo 2022
# ------------------------------------------

with open(
    BRONZE / "censo_2022.json",
    "r",
    encoding="utf-8"
) as f:

    data = json.load(f)

df = pd.DataFrame(
    data
)

df = normalize_columns(df)

save_parquet(
    df,
    "censo_2022.parquet"
)

# ------------------------------------------
# PIB
# ------------------------------------------

with open(
    BRONZE / "pib_municipal.json",
    "r",
    encoding="utf-8"
) as f:

    data = json.load(f)

df = pd.DataFrame(
    data
)

df = normalize_columns(df)

save_parquet(
    df,
    "pib.parquet"
)

# ------------------------------------------
# IDEB
# ------------------------------------------

ideb_extract_dir = (
    BRONZE / "ideb_extraido"
)

if not ideb_extract_dir.exists():

    with zipfile.ZipFile(
        BRONZE /
        "ideb_anos_iniciais.zip"
    ) as z:

        z.extractall(
            ideb_extract_dir
        )

arquivos_excel = list(
    ideb_extract_dir.rglob("*.xls*")
)

if arquivos_excel:

    ideb = pd.read_excel(
        arquivos_excel[0]
    )

    ideb = normalize_columns(
        ideb
    )

    save_parquet(
        ideb,
        "ideb.parquet"
    )

else:

    print(
        "⚠️ IDEB sem planilha encontrada."
    )

# ------------------------------------------
# Microdados
# ------------------------------------------

print(
    "\n✅ Bronze → Silver concluído."
)

print(
    "\nPróxima etapa:"
)

print(
    "02_silver_quality"
)

print(
    "03_microdados_to_silver"
)
