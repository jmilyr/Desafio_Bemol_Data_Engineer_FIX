# Parte 1 — Estruturação de Ambiente Azure Databricks (do zero)

Este documento descreve como eu configuraria um ambiente **Lakehouse** em Azure Databricks (produção),
incluindo **cluster**, **auto scale** e **Unity Catalog**. No Databricks Community/Free, algumas
capacidades (ex.: Unity Catalog) podem não estar habilitadas; por isso, o projeto simula a estrutura
com **databases**.

---

## 1.1 Criação do Workspace + Rede (visão de produção)
1. **Criar Azure Databricks Workspace** (Resource Group, Region).
2. (Recomendado) **VNet Injection** + Subnets (public/private) para isolar tráfego.
3. **Private Link** para Storage e Key Vault (se aplicável).
4. **Key Vault** para segredos (tokens, credenciais) e integração com Databricks secrets.

---

## 1.2 Storage Lake (ADLS Gen2)
- Criar Storage Account (ADLS Gen2) com containers por zona:
  - `landing/` (raw uploads)
  - `bronze/`
  - `silver/`
  - `gold/`
  - `checkpoints/` (streaming)
- Definir naming convention e lifecycle (tiering/retention).

---

## 1.3 Cluster: escolha de instância e auto scale (produção)
### Objetivo
Executar pipelines batch (bronze/silver/gold) com custo controlado.

### Configuração sugerida (exemplo)
- **Cluster mode:** Standard (single user ou shared conforme governança)
- **Runtime:** Databricks Runtime LTS (com suporte Delta)
- **Driver:** `Standard_D4as_v5` (4 vCPU, 16 GB) ou equivalente
- **Workers:** `Standard_D8as_v5` (8 vCPU, 32 GB) ou equivalente
- **Auto scale:** min 2 / max 8 workers
- **Auto-termination:** 30 min
- **Photon:** habilitar se disponível e adequado

### Política
- Criar **Cluster Policy** para padronizar runtimes, limites de autoscale, tags de custo e bibliotecas.
- Tags obrigatórias: `cost_center`, `project`, `env`.

> No Community/Free: usar **Serverless/Compute default**, sem escolha de instância, mas mantendo a lógica de camadas.

---

## 1.4 Unity Catalog (produção)
### Estrutura sugerida
- **Catalog:** `bemol_lakehouse`
- **Schemas:** `landing`, `bronze`, `silver`, `gold`, `meta`
- **External Locations:** apontando para containers ADLS (landing/bronze/silver/gold)

### Permissões (exemplo)
- Grupo `data_engineers`:
  - `USE CATALOG`, `USE SCHEMA`
  - `CREATE TABLE`, `MODIFY` em `landing/bronze/silver`
- Grupo `analytics_engineers`:
  - `USE SCHEMA` em `silver/gold`
  - `CREATE TABLE` em `gold`
- Grupo `bi_analysts`:
  - `SELECT` em `gold`
- Grupo `auditors`:
  - `SELECT` em `meta`

### Comandos de exemplo (UC)
```sql
CREATE CATALOG IF NOT EXISTS bemol_lakehouse;
CREATE SCHEMA IF NOT EXISTS bemol_lakehouse.bronze;

GRANT USE CATALOG ON CATALOG bemol_lakehouse TO `data_engineers`;
GRANT USE SCHEMA ON SCHEMA bemol_lakehouse.bronze TO `data_engineers`;
GRANT SELECT ON SCHEMA bemol_lakehouse.gold TO `bi_analysts`;
```

---

## 1.5 Observabilidade e Operação
- `meta.run_log` (já implementado no projeto) para registrar execução por camada.
- Alertas via Jobs (falha/sucesso) e integração com e-mail/Slack em produção.
- Métricas: duração, linhas processadas, taxas de erro, DQ failures.

