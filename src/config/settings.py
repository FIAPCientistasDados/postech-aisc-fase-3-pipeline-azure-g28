from pathlib import Path
import os

# Databricks
if "DATABRICKS_RUNTIME_VERSION" in os.environ:

    ROOT = Path("/Workspace/Users/alura.conta@gmail.com")

    BRONZE_PATH = ROOT / "bronze"
    SILVER_PATH = ROOT / "silver"
    GOLD_PATH = ROOT / "gold"

# VS Code / Linux / Windows
else:

    ROOT = Path(__file__).resolve().parents[2]

    DADOS_PATH = ROOT / "dados"

    BRONZE_PATH = DADOS_PATH / "bronze"
    SILVER_PATH = DADOS_PATH / "silver"
    GOLD_PATH = DADOS_PATH / "gold"