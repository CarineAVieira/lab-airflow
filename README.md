# Lab Airflow – ShopBrasil Pricing Pipeline

Pipeline desenvolvido para a disciplina de **Orquestração de Workflows**, utilizando **Apache Airflow** para automatizar a coleta, processamento e persistência de métricas de preços por categoria da FakeStore API.

## Objetivo

Construir um pipeline ETL resiliente capaz de:

* Coletar produtos da FakeStore API;
* Calcular métricas por categoria (quantidade, preço médio, mínimo e máximo);
* Persistir os resultados em PostgreSQL sem duplicidade de registros;
* Executar automaticamente todos os dias às **06:00 (America/Sao_Paulo)**.

## Funcionalidades

* TaskFlow API (`@dag` e `@task`)
* TaskGroups para organização do fluxo
* Dynamic Task Mapping (`.expand()`)
* Retry com exponential backoff
* Callbacks de sucesso, falha e retry
* Integração com FakeStore API
* Persistência em PostgreSQL utilizando `PostgresHook`
* UPSERT utilizando `ON CONFLICT`
* Pool (`ecommerce_pool`) para controle de concorrência
* Pipeline totalmente dockerizado

## Estrutura do projeto

```text
dags/
fake-api/
plugins/
scripts/
sql/
docker-compose.yml
```

## Tecnologias utilizadas

* Apache Airflow 2.9
* Python 3.11
* Docker Compose
* PostgreSQL
* FakeStore API

## Como executar

Subir os containers:

```bash
docker compose up -d
```

Acessar o Airflow:

```
http://localhost:8082
```

Executar a DAG:

```
shopbrasil_pricing_pipeline
```

## Banco de dados

Tabela gerada:

```
pricing_categoria_snapshot
```

A gravação é **idempotente**, utilizando `ON CONFLICT`, evitando registros duplicados durante reprocessamentos.

## Autor

Carine de Almeida Vieira
