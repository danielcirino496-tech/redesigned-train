"""
ingestion_pipeline.py
----------------------
Pipeline de Extração, Transformação e Carga (ETL) para unificação das vendas
de E-commerce (JSON) e Lojas Físicas (CSV) em um banco de dados SQLite local.

Autor: Analista / Engenheiro de Dados
Projeto: Data Ingestion & Profiling Engine (TP1)

Como executar:
    python src/ingestion_pipeline.py

Pré-requisitos:
    pip install pandas

O script assume que é executado a partir da raiz do repositório e que os
arquivos brutos estão em ./data/vendas_web.json e ./data/vendas_lojas.csv.
O banco varejo.db é (re)criado na raiz do repositório.
"""

import json
import os
import sqlite3
from datetime import datetime

import pandas as pd

# ---------------------------------------------------------------------------
# Configuração de caminhos (independente do diretório de onde o script é chamado)
# ---------------------------------------------------------------------------
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # raiz do repo
DATA_DIR = os.path.join(BASE_DIR, "data")

PATH_JSON = os.path.join(DATA_DIR, "vendas_web.json")
PATH_CSV = os.path.join(DATA_DIR, "vendas_lojas.csv")
PATH_DB = os.path.join(BASE_DIR, "varejo.db")

TABLE_NAME = "tb_vendas_consolidada"

# Formatos de data possíveis observados nas duas origens de dados
DATE_FORMATS = ("%Y-%m-%d", "%Y/%m/%d", "%d-%m-%Y", "%d/%m/%Y")


# ---------------------------------------------------------------------------
# FASE 1 — Preparação do banco de dados local (SQLite)
# ---------------------------------------------------------------------------
def preparar_banco(db_path: str) -> sqlite3.Connection:
    """Cria (ou recria) o banco SQLite e a tabela de destino via DDL."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute(f"DROP TABLE IF EXISTS {TABLE_NAME};")

    cursor.execute(
        f"""
        CREATE TABLE {TABLE_NAME} (
            id_transacao   TEXT PRIMARY KEY,
            data_venda     TEXT NOT NULL,
            id_produto     TEXT NOT NULL,
            quantidade     INTEGER NOT NULL,
            valor_unitario REAL NOT NULL,
            canal_venda    TEXT NOT NULL CHECK (canal_venda IN ('E-commerce', 'Loja Física'))
        );
        """
    )
    conn.commit()
    return conn


# ---------------------------------------------------------------------------
# FASE 2 — Extração
# ---------------------------------------------------------------------------
def extrair_ecommerce(path_json: str) -> pd.DataFrame:
    with open(path_json, "r", encoding="utf-8") as f:
        dados = json.load(f)
    return pd.DataFrame(dados)


def extrair_lojas(path_csv: str) -> pd.DataFrame:
    return pd.read_csv(path_csv)


# ---------------------------------------------------------------------------
# FASE 2 — Transformação
# ---------------------------------------------------------------------------
def normalizar_data(valor) -> str | None:
    """Converte datas em formatos mistos (YYYY-MM-DD, YYYY/MM/DD, DD-MM-YYYY,
    DD/MM/YYYY) para o padrão ISO YYYY-MM-DD. Retorna None se não for possível
    interpretar a data com segurança."""
    if pd.isna(valor):
        return None

    texto = str(valor).strip()
    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(texto, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None  # data em formato desconhecido -> será descartada na limpeza


def padronizar_schema_ecommerce(df: pd.DataFrame) -> pd.DataFrame:
    """Renomeia/mapeia as colunas do e-commerce para o schema unificado."""
    df = df.rename(
        columns={
            "cod_transacao": "id_transacao",
            "data_registro": "data_venda",
            "id_prod": "id_produto",
            "quant": "quantidade",
            "preco_unitario": "valor_unitario",
        }
    )
    df["canal_venda"] = "E-commerce"
    return df[["id_transacao", "data_venda", "id_produto", "quantidade", "valor_unitario", "canal_venda"]]


def padronizar_schema_lojas(df: pd.DataFrame) -> pd.DataFrame:
    """Renomeia/mapeia as colunas das lojas físicas para o schema unificado."""
    df = df.rename(
        columns={
            "ID_Venda": "id_transacao",
            "Data": "data_venda",
            "Produto_ID": "id_produto",
            "Qtd": "quantidade",
            "Valor_Unit": "valor_unitario",
        }
    )
    df["canal_venda"] = "Loja Física"
    return df[["id_transacao", "data_venda", "id_produto", "quantidade", "valor_unitario", "canal_venda"]]


def limpar_dados(df: pd.DataFrame) -> pd.DataFrame:
    """Aplica as regras de limpeza e normalização temporal descritas no enunciado:
    - remove linhas com quantidade <= 0
    - remove linhas com valor_unitario vazio/nulo
    - normaliza datas para YYYY-MM-DD (remove linhas com data não interpretável)
    """
    df = df.copy()

    # Tipagem numérica segura (valores inválidos viram NaN e são descartados)
    df["quantidade"] = pd.to_numeric(df["quantidade"], errors="coerce")
    df["valor_unitario"] = pd.to_numeric(df["valor_unitario"], errors="coerce")

    # Normalização temporal
    df["data_venda"] = df["data_venda"].apply(normalizar_data)

    # Regras de descarte
    df = df[df["quantidade"] > 0]
    df = df[df["valor_unitario"].notna()]
    df = df[df["data_venda"].notna()]

    # Tipos finais
    df["quantidade"] = df["quantidade"].astype(int)
    df["valor_unitario"] = df["valor_unitario"].astype(float)
    df["id_transacao"] = df["id_transacao"].astype(str)
    df["id_produto"] = df["id_produto"].astype(str)

    return df.reset_index(drop=True)


# ---------------------------------------------------------------------------
# FASE 2 — Carga (Load)
# ---------------------------------------------------------------------------
def carregar_no_banco(df: pd.DataFrame, conn: sqlite3.Connection) -> None:
    df.to_sql(TABLE_NAME, conn, if_exists="append", index=False)
    conn.commit()


# ---------------------------------------------------------------------------
# Orquestração do pipeline
# ---------------------------------------------------------------------------
def main():
    print("Iniciando pipeline de ingestão...")

    # Extração
    df_web_raw = extrair_ecommerce(PATH_JSON)
    df_lojas_raw = extrair_lojas(PATH_CSV)
    print(f"  Extraídos {len(df_web_raw)} registros brutos do E-commerce.")
    print(f"  Extraídos {len(df_lojas_raw)} registros brutos das Lojas Físicas.")

    # Transformação: padronização de schema
    df_web = padronizar_schema_ecommerce(df_web_raw)
    df_lojas = padronizar_schema_lojas(df_lojas_raw)

    # Integração
    df_unificado = pd.concat([df_web, df_lojas], ignore_index=True)

    # Limpeza + normalização temporal
    df_limpo = limpar_dados(df_unificado)
    descartados = len(df_unificado) - len(df_limpo)
    print(f"  Registros descartados na limpeza (qtd<=0, valor nulo/vazio ou data inválida): {descartados}")
    print(f"  Registros válidos após limpeza: {len(df_limpo)}")

    # Carga
    conn = preparar_banco(PATH_DB)
    carregar_no_banco(df_limpo, conn)
    conn.close()

    print(f"Pipeline concluído. Dados gravados em: {PATH_DB} (tabela '{TABLE_NAME}').")


if __name__ == "__main__":
    main()
