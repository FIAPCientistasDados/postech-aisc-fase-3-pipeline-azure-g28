# ====================================================
# SILVER - INDICADOR MUNICÍPIO
# Compatível:
# - Databricks
# - VS Code
# ====================================================

import os
import sys
from pathlib import Path

import pandas as pd

# ====================================================
# IMPORTS DO PROJETO
# ====================================================

ROOT = Path(__file__).resolve().parent.parent

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.config.settings import (
    BRONZE_PATH,
    SILVER_PATH
)

# ====================================================
# DETECÇÃO DE AMBIENTE
# ====================================================

IS_DATABRICKS = (
    "DATABRICKS_RUNTIME_VERSION"
    in os.environ
)

if IS_DATABRICKS:
    from pyspark.sql import functions as F

print(
    f"Ambiente: {'Databricks' if IS_DATABRICKS else 'VS Code'}"
)

# ====================================================
# ARQUIVOS BRONZE
# ====================================================

arquivos = [
    BRONZE_PATH / "resultados_municipios_2023.xlsx",
    BRONZE_PATH / "resultados_municipios_2024.xlsx",
    BRONZE_PATH / "resultados_municipios_2025.xlsx"
]

# ====================================================
# LEITURA E CONSOLIDAÇÃO
# ====================================================

dados = []

for arquivo in arquivos:

    if not arquivo.exists():

        raise FileNotFoundError(
            f"Arquivo não encontrado: {arquivo}"
        )

    print(f"Lendo {arquivo.name}")

    df = pd.read_excel(
        arquivo,
        engine="openpyxl",
        header=1
    )

    df["_source_file"] = arquivo.name

    dados.append(df)

pdf = pd.concat(
    dados,
    ignore_index=True
)

print(
    f"Registros consolidados: {len(pdf):,}"
)

# ====================================================
# LIMPEZA DOS NOMES DAS COLUNAS
# ====================================================

pdf.columns = (
    pdf.columns
    .str.strip()
)

# ====================================================
# PADRONIZAÇÃO
# ====================================================

pdf["ANO"] = pd.to_numeric(
    pdf["ANO"],
    errors="coerce"
).astype("Int64")

pdf["CO_UF"] = pd.to_numeric(
    pdf["CO_UF"],
    errors="coerce"
).astype("Int64")

pdf["CO_MUNICIPIO"] = (
    pdf["CO_MUNICIPIO"]
    .astype(str)
    .str.strip()
)

pdf["NO_TP_REDE"] = (
    pdf["NO_TP_REDE"]
    .astype(str)
    .str.strip()
)

# ====================================================
# COLUNAS NUMÉRICAS
# ====================================================

colunas_numericas = [

    "PC_ALUNO_ALFABETIZADO",

    "META_FINAL_2024",
    "META_FINAL_2025",
    "META_FINAL_2026",
    "META_FINAL_2027",
    "META_FINAL_2028",
    "META_FINAL_2029",
    "META_FINAL_2030",

    "PC_AVALIADOS_LP"
]

for coluna in colunas_numericas:

    if coluna in pdf.columns:

        pdf[coluna] = pd.to_numeric(
            pdf[coluna],
            errors="coerce"
        )

# ====================================================
# CORREÇÃO DE TIPOS MISTOS
# ====================================================

for coluna in pdf.columns:

    if pdf[coluna].dtype == "object":

        tipos = (
            pdf[coluna]
            .dropna()
            .map(type)
            .nunique()
        )

        if tipos > 1:

            print(
                f"Padronizando coluna: {coluna}"
            )

            pdf[coluna] = (
                pdf[coluna]
                .astype(str)
            )

# ====================================================
# METADADOS
# ====================================================

pdf["ingested_at"] = pd.Timestamp.utcnow()

pdf["source"] = (
    "bronze/resultados_municipios"
)

pdf["version"] = "1.0"

# ====================================================
# DATA QUALITY
# ====================================================

chaves = [
    "ANO",
    "CO_MUNICIPIO",
    "NO_TP_REDE"
]

nulos_chave = (
    pdf[chaves]
    .isna()
    .sum()
    .sum()
)

duplicados = (
    pdf
    .duplicated(
        subset=chaves
    )
    .sum()
)

print("\n===== DATA QUALITY =====")

print(
    f"Nulos nas chaves: {nulos_chave}"
)

print(
    f"Duplicados: {duplicados}"
)

# ====================================================
# REMOVER DUPLICADOS
# ====================================================

pdf = pdf.drop_duplicates(
    subset=chaves
)

# ====================================================
# DATA QUALITY OUTPUT
# ====================================================

dq = pd.DataFrame([
    {
        "table_name":
            "silver.indicador_municipio",
        "rule":
            "completude_chaves",
        "status":
            "PASS"
            if nulos_chave == 0
            else "FAIL",
        "records_checked":
            len(pdf),
        "failures":
            int(nulos_chave)
    },

    {
        "table_name":
            "silver.indicador_municipio",
        "rule":
            "unicidade_chave",
        "status":
            "PASS"
            if duplicados == 0
            else "FAIL",
        "records_checked":
            len(pdf),
        "failures":
            int(duplicados)
    }
])

# ====================================================
# DATABRICKS
# ====================================================

if IS_DATABRICKS:

    spark.sql(
        "CREATE DATABASE IF NOT EXISTS monitoring"
    )

    df_dq = (
        spark.createDataFrame(dq)
        .withColumn(
            "run_at",
            F.current_timestamp()
        )
    )

    spark.sql("""
        CREATE TABLE IF NOT EXISTS
        monitoring.dq_results
        (
            table_name STRING,
            rule STRING,
            status STRING,
            records_checked BIGINT,
            failures BIGINT,
            run_at TIMESTAMP
        )
        USING DELTA
    """)

    df_dq.write \
        .mode("append") \
        .saveAsTable(
            "monitoring.dq_results"
        )

    spark.sql(
        "CREATE DATABASE IF NOT EXISTS silver"
    )

    df = spark.createDataFrame(pdf)

    df.write \
        .mode("overwrite") \
        .option(
            "overwriteSchema",
            "true"
        ) \
        .format("delta") \
        .partitionBy("ANO") \
        .saveAsTable(
            "silver.indicador_municipio"
        )

    print(
        "\n✅ Silver criada no Databricks"
    )

    print(
        f"Registros: {df.count():,}"
    )

# ====================================================
# VS CODE
# ====================================================

else:

    SILVER_PATH.mkdir(
        parents=True,
        exist_ok=True
    )

    destino_silver = (
        SILVER_PATH
        / "indicador_municipio.parquet"
    )

    destino_dq = (
        SILVER_PATH
        / "dq_results.parquet"
    )

    pdf.to_parquet(
        destino_silver,
        index=False
    )

    dq["run_at"] = pd.Timestamp.utcnow()

    dq.to_parquet(
        destino_dq,
        index=False
    )

    print(
        f"\n✅ Silver gravada:"
    )

    print(destino_silver)

    print(
        f"\n✅ DQ gravada:"
    )

    print(destino_dq)

# ====================================================
# RESUMO
# ====================================================

print("\n===== RESUMO =====")

print(
    f"Registros finais: {len(pdf):,}"
)

print(
    f"Anos carregados: "
    f"{pdf['ANO'].nunique()}"
)

print(
    pdf.groupby("ANO")
       .size()
       .reset_index(name="qtd")
)
