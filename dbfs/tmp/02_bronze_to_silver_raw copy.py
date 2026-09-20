# %python

import pandas as pd
from pathlib import Path
from pyspark.sql import functions as F

# ====================================================
# BRONZE
# ====================================================

BRONZE = Path(
    "/Workspace/Users/alura.conta@gmail.com/bronze"
)

arquivos = [
    BRONZE / "resultados_municipios_2023.xlsx",
    BRONZE / "resultados_municipios_2024.xlsx",
    BRONZE / "resultados_municipios_2025.xlsx"
]

# ====================================================
# LEITURA E CONSOLIDAÇÃO
# ====================================================

dados = []

for arquivo in arquivos:

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
# PADRONIZAÇÃO DOS TIPOS
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

for c in colunas_numericas:

    if c in pdf.columns:

        pdf[c] = pd.to_numeric(
            pdf[c],
            errors="coerce"
        )

# ====================================================
# EVITAR ERROS PYARROW
# ====================================================

for col in pdf.columns:

    if pdf[col].dtype == "object":

        tipos = (
            pdf[col]
                .dropna()
                .map(type)
                .nunique()
        )

        if tipos > 1:

            print(
                f"Padronizando coluna {col}"
            )

            pdf[col] = pdf[col].astype(str)

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
    f"Nulos na chave: {nulos_chave}"
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
# MONITORAMENTO
# ====================================================

spark.sql(
"""
CREATE DATABASE IF NOT EXISTS monitoring
"""
)

spark.sql("""
CREATE TABLE IF NOT EXISTS monitoring.dq_results
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

registros = [

    (
        "silver.indicador_municipio",
        "completude_chaves",
        "PASS" if nulos_chave == 0 else "FAIL",
        int(len(pdf)),
        int(nulos_chave)
    ),

    (
        "silver.indicador_municipio",
        "unicidade_chave",
        "PASS" if duplicados == 0 else "FAIL",
        int(len(pdf)),
        int(duplicados)
    )
]

df_dq = (
    spark.createDataFrame(
        registros,
        [
            "table_name",
            "rule",
            "status",
            "records_checked",
            "failures"
        ]
    )
    .withColumn(
        "run_at",
        F.current_timestamp()
    )
)

df_dq.write \
    .mode("append") \
    .saveAsTable(
        "monitoring.dq_results"
    )

# ====================================================
# PANDAS -> SPARK
# ====================================================

df = spark.createDataFrame(pdf)

# ====================================================
# SILVER
# ====================================================

spark.sql(
"""
CREATE DATABASE IF NOT EXISTS silver
"""
)

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

# ====================================================
# RESULTADOS
# ====================================================

print("\n✅ SILVER CRIADA COM SUCESSO")

print(
    f"Registros: {df.count():,}"
)

print(
    f"Anos carregados: "
    f"{df.select('ANO').distinct().count()}"
)

display(
    df.groupBy("ANO")
      .count()
      .orderBy("ANO")
)

display(
    df.limit(20)
)
