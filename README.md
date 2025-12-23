# Desafio Bemol: Data Engineer II — Projeto Lakehouse Medallion (Databricks Community)

Este repositório foi montado para atender aos itens do desafio.  
Base pública escolhida: **Olist Customers** (arquivo `olist_customers_dataset.csv`).

## Como este projeto atende o desafio
- **Parte 1 (Ambiente + Cluster + Unity Catalog):** documentação em `docs/parte_1_ambiente_e_unity_catalog.md` fileciteturn3file0L17-L31
- **Parte 2 (Ingestão Bronze em Delta):** notebook `01_bronze/01_bronze_customers_ingest.py` fileciteturn3file0L35-L44
- **Parte 3 (Transformação Silver):** notebook `02_silver/01_silver_customers_clean.py` fileciteturn3file0L45-L52
- **Parte 4 (Classe template com read/transform/write + logging):** `utils/03_layer_controller.py` fileciteturn3file0L57-L65

> **Nota sobre “fonte externa” no Community:** o ambiente pode bloquear egress HTTP e DBFS público.
> Por isso, a fonte externa foi implementada como **upload via UI** para `landing.olist_customers_csv`,
> que é um input externo ao pipeline (fora do Spark job) e atende o objetivo do requisito.

## Estrutura
```
00_setup/
01_bronze/
02_silver/
03_gold/
utils/
docs/
```

## Execução no Databricks Community (passo a passo)
1) **Upload do CSV e criação da tabela landing**
- Databricks → **Create → Add or upload data**
- Upload do arquivo `olist_customers_dataset.csv`
- **Create table**
  - Database: `landing`
  - Table: `olist_customers_csv`

2) **Rodar pipeline**
1. `00_setup/00_setup_env.py` (Run all)
2. `01_bronze/01_bronze_customers_ingest.py` (Run all)
3. `01_bronze/02_bronze_data_quality.py` (Run all)
4. `02_silver/01_silver_customers_clean.py` (Run all)
5. `02_silver/02_silver_data_quality.py` (Run all)
6. `03_gold/01_dim_customer.sql`
7. `03_gold/02_marts_customers.sql`

## Validação rápida (SQL)
```sql
SHOW TABLES IN bronze;
SHOW TABLES IN silver;
SHOW TABLES IN gold;

SELECT * FROM gold.mart_customers_by_state ORDER BY total_customers DESC;
SELECT * FROM meta.run_log ORDER BY run_ts DESC;
```
