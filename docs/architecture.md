# Arquitetura (Mermaid)

```mermaid
flowchart LR
  A[landing.olist_customers_csv<br/>(upload)] --> B[bronze.customers<br/>Delta]
  B --> C[silver.dim_customer<br/>limpeza + dedup + padronização]
  C --> D[gold.dim_customer<br/>modelo consumível]
  C --> E[gold.mart_customers_by_state<br/>KPIs]
  C --> F[gold.mart_customers_by_city<br/>KPIs]
  B --> G[meta.run_log<br/>observabilidade]
  C --> G
  D --> G
```
