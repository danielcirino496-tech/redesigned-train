# Data Ingestion & Profiling Engine — TP1

Pipeline de **Extração, Transformação e Carga (ETL)** que unifica dados de vendas de dois
canais omnicanais de uma startup de varejo esportivo — **E-commerce** (JSON) e **Lojas
Físicas / PDV** (CSV) — em um banco de dados local **SQLite** (`varejo.db`), seguido de um
relatório analítico-visual exploratório de faturamento.

## Estrutura do repositório

```
├── data/
│   ├── vendas_web.json              # Entrada bruta (E-commerce)
│   ├── vendas_lojas.csv             # Entrada bruta (Lojas Físicas)
│   └── vendas_ecommerce_limpo.json  # Saída gerada (exportação de intercâmbio)
├── src/
│   └── ingestion_pipeline.py        # Script de ETL (SQLite + Pandas)
├── notebooks/
│   └── analise_vendas.ipynb         # Relatório exploratório e gráfico
├── varejo.db                        # Banco de dados SQLite (gerado pelo pipeline)
├── histograma_vendas.png            # Histograma gerado pelo notebook (Seaborn)
└── README.md
```

## Modelo de dados

Tabela `tb_vendas_consolidada` (SQLite):

| Coluna           | Tipo    | Descrição                                       |
|-------------------|---------|--------------------------------------------------|
| `id_transacao`    | TEXT PK | Identificador único da transação                 |
| `data_venda`      | TEXT    | Data padronizada no formato `YYYY-MM-DD`          |
| `id_produto`      | TEXT    | Identificador do produto                          |
| `quantidade`      | INTEGER | Quantidade vendida (sempre > 0)                   |
| `valor_unitario`  | REAL    | Preço unitário (nunca nulo/vazio)                 |
| `canal_venda`     | TEXT    | `"E-commerce"` ou `"Loja Física"`                 |

## Regras de limpeza aplicadas

- Linhas com `quantidade <= 0` são descartadas.
- Linhas com `valor_unitario` vazio/nulo são descartadas.
- Datas em formatos mistos (`YYYY-MM-DD`, `YYYY/MM/DD`, `DD-MM-YYYY`, `DD/MM/YYYY`) são
  normalizadas para `YYYY-MM-DD`; datas não interpretáveis são descartadas.
- As colunas de origem são renomeadas/mapeadas para o schema unificado antes da integração.

## Como executar

### 1. Pré-requisitos

```bash
python3 -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install pandas seaborn matplotlib jupyter
```

### 2. Rodar o pipeline de ingestão

Execute **a partir da raiz do repositório**:

```bash
python src/ingestion_pipeline.py
```

Isso irá:
1. Ler `data/vendas_web.json` e `data/vendas_lojas.csv`;
2. Padronizar os schemas, limpar e normalizar as datas;
3. (Re)criar `varejo.db` na raiz do repositório com a tabela `tb_vendas_consolidada` populada.

> O script resolve os caminhos dos arquivos automaticamente com base na sua própria
> localização (`src/`), portanto funciona independentemente do diretório de onde é chamado —
> não é necessário ajustar caminhos manualmente.

### 3. Rodar o notebook de análise

```bash
jupyter notebook notebooks/analise_vendas.ipynb
```

Execute todas as células (**Run All**) — o notebook deve ser executado a partir da pasta
`notebooks/` (comportamento padrão do Jupyter). Ele irá:
1. Exportar as vendas de E-commerce do banco para `data/vendas_ecommerce_limpo.json`;
2. Calcular e exibir o faturamento total, o preço unitário médio e o faturamento por canal;
3. Gerar e salvar o histograma `histograma_vendas.png` (distribuição de faturamento por
   transação, via Seaborn).
