from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parent.parent

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import requests
import urllib3
import zipfile
from pathlib import Path
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from src.config.settings import BRONZE_PATH

urllib3.disable_warnings()

def extrair_zip(arquivo_zip: Path):

    pasta_destino = arquivo_zip.with_suffix("")

    if pasta_destino.exists():
        print(f"✅ ZIP já extraído: {pasta_destino.name}")
        return

    print(f"📦 Extraindo: {arquivo_zip.name}")

    with zipfile.ZipFile(arquivo_zip, "r") as zip_ref:
        zip_ref.extractall(pasta_destino)

    print(f"✅ Extraído para: {pasta_destino}")


def download(url: str, destino: str):

    destino = Path(destino)

    destino.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    session = requests.Session()

    retry = Retry(
        total=10,
        backoff_factor=5
    )

    session.mount(
        "https://",
        HTTPAdapter(max_retries=retry)
    )

    with session.get(
        url,
        stream=True,
        verify=False,
        timeout=(60, 3600)
    ) as r:

        r.raise_for_status()

        with open(destino, "wb") as f:

            for chunk in r.iter_content(
                chunk_size=1024 * 1024
            ):

                if chunk:
                    f.write(chunk)

    print(f"✅ Download concluído: {destino}")

ARQUIVOS = [
    # INEP
    {
        "nome": "resultados_ufs_2023.xlsx",
        "url": "https://download.inep.gov.br/avaliacao_da_alfabetizacao/resultados_e_metas_ufs.xlsx"
    },
    {
        "nome": "resultados_municipios_2023.xlsx",
        "url": "https://download.inep.gov.br/avaliacao_da_alfabetizacao/resultados_e_metas_municipios.xlsx"
    },
    {
        "nome": "microdados_2023.zip",
        "url": "https://download.inep.gov.br/dados_abertos/microdados_avaliacao_da_alfabetizacao_2023.zip"
    },

    {
        "nome": "resultados_ufs_2024.xlsx",
        "url": "https://download.inep.gov.br/alfabetiza_brasil/resultados_e_metas_ufs_2024_2.xlsx"
    },
    {
        "nome": "resultados_municipios_2024.xlsx",
        "url": "https://download.inep.gov.br/alfabetiza_brasil/resultados_e_metas_municipios_2024.xlsx"
    },
    {
        "nome": "microdados_2024.zip",
        "url": "https://download.inep.gov.br/dados_abertos/microdados_avaliacao_da_alfabetizacao_2024.zip"
    },

    {
        "nome": "resultados_ufs_2025.xlsx",
        "url": "https://download.inep.gov.br/avaliacao_da_alfabetizacao/resultados/resultados_e_metas_ufs_2025_v1.xlsx"
    },
    {
        "nome": "resultados_municipios_2025.xlsx",
        "url": "https://download.inep.gov.br/avaliacao_da_alfabetizacao/resultados/resultados_e_metas_municipios_2025_3.xlsx"
    },
    {
        "nome": "microdados_2025.zip",
        "url": "https://download.inep.gov.br/dados_abertos/microdados_AEEB_2025.zip"
    },
    # IBGE
    {
        "nome": "ibge_municipios.json",
        "url": "https://servicodados.ibge.gov.br/api/v1/localidades/municipios"
    },
    {
        "nome": "ibge_estados.json",
        "url": "https://servicodados.ibge.gov.br/api/v1/localidades/estados"
    },
    # IDEB
    {
        "nome": "ideb_anos_iniciais.zip",
        "url": "http://download.inep.gov.br/ideb/resultados/divulgacao_anos_iniciais_municipios_2025.zip"
    },
    # INSE 2023
    {
        "nome": "inse_2023_municipios.xlsx",
        "url": "http://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/2023/nivel_socioeconomico/INSE_2023_municipios.xlsx"
    },
    # PIB Municipal
    {
        "nome": "pib_municipal.json",
        "url": "https://apisidra.ibge.gov.br/values/t/5938/n6/all/v/37,516,520,528,6574/p/2021"
    },
    # Censo 2022
    {
        "nome": "censo_2022.json",
        "url": "https://apisidra.ibge.gov.br/values/t/4714/n6/all/v/93,6318,614/p/2022"
    }
]


from pathlib import Path

BASE_DIR = BRONZE_PATH

for arquivo in ARQUIVOS:

    destino = BASE_DIR / arquivo["nome"]

    if destino.exists():

        print(f"✅ Já existe: {arquivo['nome']}")

    if destino.suffix.lower() == ".zip":
        extrair_zip(destino)

    continue

    print(f"⬇️ Baixando {arquivo['nome']}")

    download(
        arquivo["url"],
        str(destino)
    )

    print(f"✅ Concluído: {arquivo['nome']}")

    if destino.suffix.lower() == ".zip":
        extrair_zip(destino)