import requests
import pendulum

from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup
from airflow.providers.postgres.hooks.postgres import PostgresHook


POSTGRES_CONN_ID = "postgres_lab"


def alerta_falha(context):
    task_id = context.get("task_instance").task_id
    print(f"[ALERTA] A task {task_id} falhou.")


def alerta_retry(context):
    task_id = context.get("task_instance").task_id
    print(f"[RETRY] A task {task_id} será reexecutada.")


def alerta_sucesso(context):
    task_id = context.get("task_instance").task_id
    print(f"[SUCESSO] A task {task_id} executou com sucesso.")


@dag(
    dag_id="shopbrasil_pricing_pipeline",
    description="Pipeline diário de métricas de preços por categoria da ShopBrasil",
    schedule="0 6 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="America/Sao_Paulo"),
    catchup=False,
    tags=["shopbrasil", "pricing", "fakestore"],
)
def shopbrasil_pricing_pipeline():

    with TaskGroup("ingestao"):

        @task(
            retries=3,
            retry_delay=pendulum.duration(minutes=2),
            retry_exponential_backoff=True,
            on_failure_callback=alerta_falha,
            on_retry_callback=alerta_retry,
            on_success_callback=alerta_sucesso,
        )
        def buscar_produtos():
            url = "https://fakestoreapi.com/products"

            try:
                response = requests.get(url, timeout=30)
                response.raise_for_status()
                produtos = response.json()

                if not produtos:
                    raise ValueError("A API retornou lista vazia de produtos.")

                return produtos

            except Exception as erro:
                print(f"Erro ao buscar produtos na FakeStore API: {erro}")
                raise

        @task
        def listar_categorias(produtos):
            categorias = sorted(list(set(produto["category"] for produto in produtos)))
            return categorias

        produtos = buscar_produtos()
        categorias = listar_categorias(produtos)

    with TaskGroup("analise"):

        @task(pool="ecommerce_pool")
        def calcular_metricas_categoria(categoria, produtos):
            produtos_categoria = [
                produto for produto in produtos
                if produto["category"] == categoria
            ]

            precos = [float(produto["price"]) for produto in produtos_categoria]

            return {
                "data_execucao": pendulum.now("America/Sao_Paulo").to_date_string(),
                "categoria": categoria,
                "quantidade_produtos": len(produtos_categoria),
                "preco_medio": round(sum(precos) / len(precos), 2),
                "preco_minimo": min(precos),
                "preco_maximo": max(precos),
            }

        metricas_por_categoria = calcular_metricas_categoria.expand(
            categoria=categorias,
            produtos=[produtos]
        )

    with TaskGroup("persistencia"):

        @task
        def criar_tabela():
            hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

            sql = """
            CREATE TABLE IF NOT EXISTS pricing_categoria_snapshot (
                data_execucao DATE NOT NULL,
                categoria TEXT NOT NULL,
                quantidade_produtos INTEGER NOT NULL,
                preco_medio NUMERIC(10,2) NOT NULL,
                preco_minimo NUMERIC(10,2) NOT NULL,
                preco_maximo NUMERIC(10,2) NOT NULL,
                atualizado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (data_execucao, categoria)
            );
            """

            hook.run(sql)

        @task
        def gravar_postgres(metricas):
            hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

            sql = """
            INSERT INTO pricing_categoria_snapshot (
                data_execucao,
                categoria,
                quantidade_produtos,
                preco_medio,
                preco_minimo,
                preco_maximo,
                atualizado_em
            )
            VALUES (
                %(data_execucao)s,
                %(categoria)s,
                %(quantidade_produtos)s,
                %(preco_medio)s,
                %(preco_minimo)s,
                %(preco_maximo)s,
                CURRENT_TIMESTAMP
            )
            ON CONFLICT (data_execucao, categoria)
            DO UPDATE SET
                quantidade_produtos = EXCLUDED.quantidade_produtos,
                preco_medio = EXCLUDED.preco_medio,
                preco_minimo = EXCLUDED.preco_minimo,
                preco_maximo = EXCLUDED.preco_maximo,
                atualizado_em = CURRENT_TIMESTAMP;
            """

            for linha in metricas:
                hook.run(sql, parameters=linha)

        tabela = criar_tabela()
        gravacao = gravar_postgres(metricas_por_categoria)

        tabela >> gravacao


shopbrasil_pricing_pipeline()