from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud import storage
from google.cloud.exceptions import Conflict
import polars as pl
import pandas as pd
import datetime
import os
import gc
import html # biblioteca para decodificar HTML
import json # biblioteca para manipulação de JSON
import re # biblioteca para expressões regulares
import glob # biblioteca para manipulação de arquivos globais
import io
import tarfile
import matplotlib.pyplot as plt
import seaborn as sns

# Caminho do projeto e credenciais
pasta_projeto = "D:\\__case_ifood"
credencial_gcp = os.path.join(pasta_projeto, "case-ifood-fsg-6f1d7cf34e08.json")
bucket_name = "case_ifood_fsg"

### FILTRO PARA ETL ###
semanas = [
    "2018-12-02", "2018-12-09", "2018-12-16", "2018-12-23",
    "2018-12-30", "2019-01-06", "2019-01-13", "2019-01-20", "2019-01-27"
]

### FILTRO PARA ANALYTICS ###
cities = ["Sao Paulo", "Rio De Janeiro", "Belo Horizonte", "Curitiba", 
           "Recife", "Salvador", "Brasilia", "Fortaleza","Porto Alegre"]


#################################### ANALYTICS ####################################
# Variável global para armazenar o dataframe
df_sales = None

# Função para autenticar e retornar o cliente BigQuery
def get_bq_client():
    credencial = service_account.Credentials.from_service_account_file(credencial_gcp)
    client = bigquery.Client(credentials=credencial, project="case-ifood-fsg")
    return client

# Função para rodar a consulta SQL e retornar o DataFrame
def get_sales_data():
    global df_sales  # Usando a variável global
    
    if df_sales is None:  # Carregar dados apenas se ainda não estiver carregado
        client = get_bq_client()

        # Consulta SQL para buscar os dados
        qry = """
            WITH sales_last_date AS (
             SELECT
                   MAX(insert_date) last_date
             FROM gold.sales
            )
            SELECT
                order_id,
                order_created_at,
                DATE(DATE_TRUNC(order_created_at, MONTH)) order_month,
                MAX(order_total_amount) amount,
                customer_id,
                customer_name,
                customer_created_at,
                customer_active,
                is_target,
                delivery_address_district,
                delivery_address_city,
                delivery_address_state,
                merchant_id,
                merchant_city,
                merchant_enabled,
                price_range,
                average_ticket,
                delivery_time,
                minimum_order_value,
                origin_platform
            FROM gold.sales s
            INNER JOIN sales_last_date sld
                ON s.insert_date = sld.last_date
            WHERE is_target IN ('target','control')
            GROUP BY ALL
        """
        
        # Executando a consulta
        result_sales = client.query(qry)
        arrow_table_sales = result_sales.to_arrow()

        # Convertendo para DataFrame do Polars
        df_sales = pl.from_arrow(arrow_table_sales)
        
        # Liberando memória
        del arrow_table_sales
        gc.collect()  # Coletar lixo para liberar memória

    return df_sales

def get_sales_data_some_columns():

# Função para retornar apenas algumas colunas do Banco
    client = get_bq_client()

    qry = """
        SELECT
                order_id,
                DATE(DATE_TRUNC(order_created_at, MONTH)) order_month,
                MAX(order_total_amount) amount,
                customer_id,
                customer_name,
                is_target,
                delivery_address_city,
                delivery_address_state,
                merchant_id,
                merchant_city,
                price_range,
                delivery_time,
                origin_platform
            FROM gold.sales
            WHERE is_target IN ('target','control')
            GROUP BY ALL
    """

    # Executando a consulta
    result_sales = client.query(qry)
    arrow_table_sales = result_sales.to_arrow()

    # Convertendo para DataFrame do Polars
    df_sales = pl.from_arrow(arrow_table_sales)
    
    # Liberando memória
    del arrow_table_sales
    gc.collect()  # Coletar lixo para liberar memória

    return df_sales

# Função para calcular métricas de avaliação do teste A/B
def calculate_evaluation_metrics(data_frame, columns_groupby, filter_cities):

    if filter_cities == None and columns_groupby == None:
        DataFrame = data_frame.group_by(["is_target"]).agg([
            pl.col("order_id").n_unique().alias("orders"),
            pl.col("customer_id").n_unique().alias("unique_customers"),
            pl.col("amount").sum().alias("revenue"),
            (pl.col("amount").sum() / pl.col("order_id").n_unique()).round(2).alias("TKM"),
            (pl.col("amount").sum() / pl.col("customer_id").n_unique()).round(2).alias("ARPU")
        ]).with_columns([
            pl.col("revenue").sum().alias("total_revenue"),
            pl.col("orders").sum().alias("total_orders"),
            pl.col("unique_customers").sum().alias("total_customers")
        ]).sort("is_target", descending=True)
    elif filter_cities == None:
        DataFrame = data_frame.group_by(["is_target"] + columns_groupby).agg([
        pl.col("order_id").n_unique().alias("orders"),
        pl.col("customer_id").n_unique().alias("unique_customers"),
        pl.col("amount").sum().alias("revenue"),
        (pl.col("amount").sum() / pl.col("order_id").n_unique()).round(2).alias("TKM"),
        (pl.col("amount").sum() / pl.col("customer_id").n_unique()).round(2).alias("ARPU")
    ]).with_columns([
        pl.col("revenue").sum().over(columns_groupby).alias("total_revenue"),
        pl.col("orders").sum().over(columns_groupby).alias("total_orders"),
        pl.col("unique_customers").sum().over(columns_groupby).alias("total_customers")
    ]).sort("is_target", descending=True)
    else:
        DataFrame = data_frame.filter(pl.col("merchant_city").is_in(filter_cities)).group_by(["is_target"] + columns_groupby).agg([
        pl.col("order_id").n_unique().alias("orders"),
        pl.col("customer_id").n_unique().alias("unique_customers"),
        pl.col("amount").sum().alias("revenue"),
        (pl.col("amount").sum() / pl.col("order_id").n_unique()).round(2).alias("TKM"),
        (pl.col("amount").sum() / pl.col("customer_id").n_unique()).round(2).alias("ARPU")
    ]).with_columns([
        pl.col("revenue").sum().over(columns_groupby).alias("total_revenue"),
        pl.col("orders").sum().over(columns_groupby).alias("total_orders"),
        pl.col("unique_customers").sum().over(columns_groupby).alias("total_customers")
    ]).sort("is_target", descending=True)
    
    return DataFrame

# Função que calcula as métricas por range de preço
def calculate_evaluation_metrics_price_range(data_frame, filter_cities):

    if filter_cities == None:
        DataFrame = data_frame.group_by(["is_target","price_range"]).agg([
        pl.col("order_id").n_unique().alias("orders"),
        pl.col("customer_id").n_unique().alias("unique_customers"),
        pl.col("amount").sum().alias("revenue"),
        (pl.col("amount").sum() / pl.col("order_id").n_unique()).round(2).alias("TKM"),
        (pl.col("amount").sum() / pl.col("customer_id").n_unique()).round(2).alias("ARPU")
    ]).with_columns([
        pl.col("revenue").sum().over("price_range").alias("total_revenue"),
        pl.col("orders").sum().over("price_range").alias("total_orders"),
        pl.col("unique_customers").sum().over("price_range").alias("total_customers")
    ]).sort("is_target", descending=True)
    else:
        DataFrame = data_frame.filter(pl.col("merchant_city").is_in(filter_cities)).group_by(["is_target","price_range"]).agg([
        pl.col("order_id").n_unique().alias("orders"),
        pl.col("customer_id").n_unique().alias("unique_customers"),
        pl.col("amount").sum().alias("revenue"),
        (pl.col("amount").sum() / pl.col("order_id").n_unique()).round(2).alias("TKM"),
        (pl.col("amount").sum() / pl.col("customer_id").n_unique()).round(2).alias("ARPU")
    ]).with_columns([
        pl.col("revenue").sum().over("price_range").alias("total_revenue"),
        pl.col("orders").sum().over("price_range").alias("total_orders"),
        pl.col("unique_customers").sum().over("price_range").alias("total_customers")
    ]).sort("is_target", descending=True)
    
    return DataFrame

# função que calcula as métricas por tempo de entrega
def calculate_evaluation_metrics_delivery_time(data_frame, filter_cities):

    if filter_cities == None:
        DataFrame = data_frame.with_columns([
            pl.when(pl.col("delivery_time") <= pl.col("delivery_time").quantile(0.25)).then(pl.lit("Q1"))
            .when(pl.col("delivery_time") <= pl.col("delivery_time").quantile(0.5)).then(pl.lit("Q2"))
            .when(pl.col("delivery_time") <= pl.col("delivery_time").quantile(0.75)).then(pl.lit("Q3"))
            .otherwise(pl.lit("Q4"))
            .alias("delivery_time_category")
        ]).group_by(["is_target","delivery_time_category"]).agg([
            pl.col("order_id").n_unique().alias("orders"),
            pl.col("customer_id").n_unique().alias("unique_customers"),
            pl.col("amount").sum().alias("revenue"),
            (pl.col("amount").sum() / pl.col("order_id").n_unique()).round(2).alias("TKM"),
            (pl.col("amount").sum() / pl.col("customer_id").n_unique()).round(2).alias("ARPU")
    ]).with_columns([
        pl.col("revenue").sum().over("delivery_time_category").alias("total_revenue"),
        pl.col("orders").sum().over("delivery_time_category").alias("total_orders"),
        pl.col("unique_customers").sum().over("delivery_time_category").alias("total_customers")
    ]).sort("is_target", descending=True)
    else:
        DataFrame = data_frame.filter(pl.col("merchant_city").is_in(filter_cities)).with_columns([
            pl.when(pl.col("delivery_time") <= pl.col("delivery_time").quantile(0.25)).then(pl.lit("Q1"))
            .when(pl.col("delivery_time") <= pl.col("delivery_time").quantile(0.5)).then(pl.lit("Q2"))
            .when(pl.col("delivery_time") <= pl.col("delivery_time").quantile(0.75)).then(pl.lit("Q3"))
            .otherwise(pl.lit("Q4"))
            .alias("delivery_time_category")
        ]).group_by(["is_target","delivery_time_category"]).agg([
        pl.col("order_id").n_unique().alias("orders"),
        pl.col("customer_id").n_unique().alias("unique_customers"),
        pl.col("amount").sum().alias("revenue"),
        (pl.col("amount").sum() / pl.col("order_id").n_unique()).round(2).alias("TKM"),
        (pl.col("amount").sum() / pl.col("customer_id").n_unique()).round(2).alias("ARPU")
    ]).with_columns([
        pl.col("revenue").sum().over("delivery_time_category").alias("total_revenue"),
        pl.col("orders").sum().over("delivery_time_category").alias("total_orders"),
        pl.col("unique_customers").sum().over("delivery_time_category").alias("total_customers")
    ]).sort("is_target", descending=True)
    
    return DataFrame

### FUNÇÕES QUE CALCULAM CLIENTES COM 2 PEDIDOS ###

# Função para calcular a qtde de clientes que fizeram 2 pedidos
def calculate_engagement_custormers_two_orders(data_frame, columns_groupby, filter_cities):
    if filter_cities is None and columns_groupby == None:
        customers_with_2_orders = data_frame.group_by(["is_target","customer_id"]).agg([
        pl.col("order_id").count().alias("orders")
    ]).filter(pl.col("orders") == 2).group_by(["is_target"]).agg([
        pl.col("customer_id").n_unique().alias("customers_with_2_orders") 
    ])
    elif filter_cities is None:
        customers_with_2_orders = data_frame.group_by(["is_target","customer_id"] + columns_groupby).agg([
        pl.col("order_id").count().alias("orders")
    ]).filter(pl.col("orders") == 2).group_by(["is_target"] + columns_groupby).agg([
        pl.col("customer_id").n_unique().alias("customers_with_2_orders") 
    ])
    else:
        customers_with_2_orders = data_frame.filter(pl.col("merchant_city").is_in(filter_cities)).group_by(["is_target","customer_id"] + columns_groupby).agg([
        pl.col("order_id").count().alias("orders")
    ]).filter(pl.col("orders") == 2).group_by(["is_target"] + columns_groupby).agg([
        pl.col("customer_id").n_unique().alias("customers_with_2_orders") 
    ])
    return customers_with_2_orders



# Função que retorna a qtde de clientes que fizeram 2 pedidos agregado por range de preço
def calculate_engagement_custormers_two_orders_price_range(data_frame, filter_cities):
    if filter_cities is None:
        customers_with_2_orders = data_frame.group_by(["is_target","customer_id","price_range"]).agg([
        pl.col("order_id").count().alias("orders")
    ]).filter(pl.col("orders") == 2).group_by(["is_target", "price_range"]).agg([
        pl.col("customer_id").n_unique().alias("customers_with_2_orders") 
    ])
    else:
        customers_with_2_orders = data_frame.filter(pl.col("merchant_city").is_in(filter_cities)).group_by(["is_target","customer_id","price_range"]).agg([
        pl.col("order_id").count().alias("orders")
    ]).filter(pl.col("orders") == 2).group_by(["is_target","price_range"]).agg([
        pl.col("customer_id").n_unique().alias("customers_with_2_orders") 
    ])
    return customers_with_2_orders

# Função que retorna a qtde de clientes que fizeram 2 pedidos agregado por tempo de entrega
def calculate_engagement_custormers_two_orders_delivery_time(data_frame, filter_cities):
    if filter_cities is None:
        customers_with_2_orders = data_frame.with_columns([
            pl.when(pl.col("delivery_time") <= pl.col("delivery_time").quantile(0.25)).then(pl.lit("Q1"))
            .when(pl.col("delivery_time") <= pl.col("delivery_time").quantile(0.5)).then(pl.lit("Q2"))
            .when(pl.col("delivery_time") <= pl.col("delivery_time").quantile(0.75)).then(pl.lit("Q3"))
            .otherwise(pl.lit("Q4"))
            .alias("delivery_time_category")
        ]).group_by(["is_target","customer_id","delivery_time_category"]).agg([
        pl.col("order_id").count().alias("orders")
    ]).filter(pl.col("orders") == 2).group_by(["is_target", "delivery_time_category"]).agg([
        pl.col("customer_id").n_unique().alias("customers_with_2_orders") 
    ])
    else:
        customers_with_2_orders = data_frame.filter(pl.col("merchant_city").is_in(filter_cities)).with_columns([
            pl.when(pl.col("delivery_time") <= pl.col("delivery_time").quantile(0.25)).then(pl.lit("Q1"))
            .when(pl.col("delivery_time") <= pl.col("delivery_time").quantile(0.5)).then(pl.lit("Q2"))
            .when(pl.col("delivery_time") <= pl.col("delivery_time").quantile(0.75)).then(pl.lit("Q3"))
            .otherwise(pl.lit("Q4"))
            .alias("delivery_time_category")
        ]).group_by(["is_target","customer_id","delivery_time_category"]).agg([
        pl.col("order_id").count().alias("orders")
    ]).filter(pl.col("orders") == 2).group_by(["is_target","delivery_time_category"]).agg([
        pl.col("customer_id").n_unique().alias("customers_with_2_orders") 
    ])
    return customers_with_2_orders

### FUNÇÕES QUE CALCULAM CLIENTES COM 3 PEDIDOS ###

# Função para calcular a qtde de clientes que fizeram 3 pedidos
def calculate_engagement_custormers_three_orders(data_frame, columns_groupby, filter_cities):
    if filter_cities is None and columns_groupby == None:
        customers_with_3_orders = data_frame.group_by(["is_target","customer_id"]).agg([
        pl.col("order_id").count().alias("orders")
    ]).filter(pl.col("orders") == 3).group_by(["is_target"]).agg([
        pl.col("customer_id").n_unique().alias("customers_with_3_orders")
    ])
    elif filter_cities == None:
        customers_with_3_orders = data_frame.group_by(["is_target","customer_id"] + columns_groupby).agg([
        pl.col("order_id").count().alias("orders")
    ]).filter(pl.col("orders") == 3).group_by(["is_target"]  + columns_groupby).agg([
        pl.col("customer_id").n_unique().alias("customers_with_3_orders")
    ])
    else:
        customers_with_3_orders = data_frame.filter(pl.col("merchant_city").is_in(filter_cities)).group_by(["is_target","customer_id"] + columns_groupby).agg([
        pl.col("order_id").count().alias("orders")
    ]).filter(pl.col("orders") == 3).group_by(["is_target"]  + columns_groupby).agg([
        pl.col("customer_id").n_unique().alias("customers_with_3_orders")
    ])
    return customers_with_3_orders


# Função que retorna a qtde de clientes que fizeram 3 pedidos agregado por range de preço
def calculate_engagement_custormers_three_orders_price_range(data_frame, filter_cities):
    if filter_cities is None:
        customers_with_3_orders = data_frame.group_by(["is_target","customer_id","price_range"]).agg([
        pl.col("order_id").count().alias("orders")
    ]).filter(pl.col("orders") == 2).group_by(["is_target", "price_range"]).agg([
        pl.col("customer_id").n_unique().alias("customers_with_3_orders") 
    ])
    else:
        customers_with_3_orders = data_frame.filter(pl.col("merchant_city").is_in(filter_cities)).group_by(["is_target","customer_id","price_range"]).agg([
        pl.col("order_id").count().alias("orders")
    ]).filter(pl.col("orders") == 2).group_by(["is_target","price_range"]).agg([
        pl.col("customer_id").n_unique().alias("customers_with_3_orders") 
    ])
    return customers_with_3_orders



# Função que retorna a qtde de clientes que fizeram 3 pedidos agregado por tempo de entrega
def calculate_engagement_custormers_three_orders_delivery_time(data_frame, filter_cities):
    if filter_cities is None:
        customers_with_3_orders = data_frame.with_columns([
            pl.when(pl.col("delivery_time") <= pl.col("delivery_time").quantile(0.25)).then(pl.lit("Q1"))
            .when(pl.col("delivery_time") <= pl.col("delivery_time").quantile(0.5)).then(pl.lit("Q2"))
            .when(pl.col("delivery_time") <= pl.col("delivery_time").quantile(0.75)).then(pl.lit("Q3"))
            .otherwise(pl.lit("Q4"))
            .alias("delivery_time_category")
        ]).group_by(["is_target","customer_id","delivery_time_category"]).agg([
        pl.col("order_id").count().alias("orders")
    ]).filter(pl.col("orders") == 3).group_by(["is_target", "delivery_time_category"]).agg([
        pl.col("customer_id").n_unique().alias("customers_with_3_orders") 
    ])
    else:
        customers_with_3_orders = data_frame.filter(pl.col("merchant_city").is_in(filter_cities)).with_columns([
            pl.when(pl.col("delivery_time") <= pl.col("delivery_time").quantile(0.25)).then(pl.lit("Q1"))
            .when(pl.col("delivery_time") <= pl.col("delivery_time").quantile(0.5)).then(pl.lit("Q2"))
            .when(pl.col("delivery_time") <= pl.col("delivery_time").quantile(0.75)).then(pl.lit("Q3"))
            .otherwise(pl.lit("Q4"))
            .alias("delivery_time_category")
        ]).group_by(["is_target","customer_id","delivery_time_category"]).agg([
        pl.col("order_id").count().alias("orders")
    ]).filter(pl.col("orders") == 3).group_by(["is_target","delivery_time_category"]).agg([
        pl.col("customer_id").n_unique().alias("customers_with_3_orders") 
    ])
    return customers_with_3_orders


### FUNÇÕES QUE CALCULAM CLIENTES COM MAIS DE 3 PEDIDOS ###

# Função para calcular a qtde de clientes que fizeram mais de 3 pedidos
def calculate_engagement_custormers_three_plus_orders(data_frame, columns_groupby, filter_cities):
    if filter_cities is None and columns_groupby == None:
        customers_with_3_plus_orders = data_frame.group_by(["is_target","customer_id"]).agg([
        pl.col("order_id").count().alias("orders")
    ]).filter(pl.col("orders") > 3).group_by(["is_target"]).agg([
        pl.col("customer_id").n_unique().alias("customers_with_3_plus_orders")
    ])
    elif filter_cities == None:
        customers_with_3_plus_orders = data_frame.group_by(["is_target","customer_id"] + columns_groupby).agg([
        pl.col("order_id").count().alias("orders")
    ]).filter(pl.col("orders") > 3).group_by(["is_target"] + columns_groupby).agg([
        pl.col("customer_id").n_unique().alias("customers_with_3_plus_orders")
    ])
    else:
        customers_with_3_plus_orders = data_frame.filter(pl.col("merchant_city").is_in(filter_cities)).group_by(["is_target","customer_id"] + columns_groupby).agg([
        pl.col("order_id").count().alias("orders")
    ]).filter(pl.col("orders") > 3).group_by(["is_target"] + columns_groupby).agg([
        pl.col("customer_id").n_unique().alias("customers_with_3_plus_orders")
    ])
    return customers_with_3_plus_orders


# Função para calcular a qtde de clientes que fizeram mais de 3 pedidos por Range de preço
def calculate_engagement_custormers_three_plus_orders_price_range(data_frame, filter_cities):
    if filter_cities is None:
        customers_with_3_plus_orders = data_frame.group_by(["is_target","customer_id","price_range"]).agg([
        pl.col("order_id").count().alias("orders")
    ]).filter(pl.col("orders") > 3).group_by(["is_target","price_range"]).agg([
        pl.col("customer_id").n_unique().alias("customers_with_3_plus_orders")
    ])
    else:
        customers_with_3_plus_orders = data_frame.filter(pl.col("merchant_city").is_in(filter_cities)).group_by(["is_target","customer_id","price_range"]).agg([
        pl.col("order_id").count().alias("orders")
    ]).filter(pl.col("orders") > 3).group_by(["is_target","price_range"]).agg([
        pl.col("customer_id").n_unique().alias("customers_with_3_plus_orders")
    ])
    return customers_with_3_plus_orders


# Função para calcular a qtde de clientes que fizeram mais de 3 pedidos por Tempo de entrega
def calculate_engagement_custormers_three_plus_orders_delivery_time(data_frame, filter_cities):
    if filter_cities is None:
        customers_with_3_plus_orders = data_frame.with_columns([
            pl.when(pl.col("delivery_time") <= pl.col("delivery_time").quantile(0.25)).then(pl.lit("Q1"))
            .when(pl.col("delivery_time") <= pl.col("delivery_time").quantile(0.5)).then(pl.lit("Q2"))
            .when(pl.col("delivery_time") <= pl.col("delivery_time").quantile(0.75)).then(pl.lit("Q3"))
            .otherwise(pl.lit("Q4"))
            .alias("delivery_time_category")
        ]).group_by(["is_target","customer_id","delivery_time_category"]).agg([
        pl.col("order_id").count().alias("orders")
    ]).filter(pl.col("orders") > 3).group_by(["is_target","delivery_time_category"]).agg([
        pl.col("customer_id").n_unique().alias("customers_with_3_plus_orders")
    ])
    else:
        customers_with_3_plus_orders = data_frame.filter(pl.col("merchant_city").is_in(filter_cities)).with_columns([
            pl.when(pl.col("delivery_time") <= pl.col("delivery_time").quantile(0.25)).then(pl.lit("Q1"))
            .when(pl.col("delivery_time") <= pl.col("delivery_time").quantile(0.5)).then(pl.lit("Q2"))
            .when(pl.col("delivery_time") <= pl.col("delivery_time").quantile(0.75)).then(pl.lit("Q3"))
            .otherwise(pl.lit("Q4"))
            .alias("delivery_time_category")
        ]).group_by(["is_target","customer_id","delivery_time_category"]).agg([
        pl.col("order_id").count().alias("orders")
    ]).filter(pl.col("orders") > 3).group_by(["is_target","delivery_time_category"]).agg([
        pl.col("customer_id").n_unique().alias("customers_with_3_plus_orders")
    ])
    return customers_with_3_plus_orders
#################################### ANALYTICS ####################################

#################################### ETL ##########################################

def split_dataframe(df, chunk_size):
    """Divide um DataFrame Pandas em pedaços menores."""
    return [df[i:i+chunk_size] for i in range(0, len(df), chunk_size)]

def create_dataset_and_table(df, dataset_nome, tabela_nome, client, location="southamerica-east1", use_chunk=False, chunk_size=1_000_000):
    dataset_id = f"{"case-ifood-fsg"}.{dataset_nome}"
    table_id = f"{dataset_id}.{tabela_nome}"

    """
    Cria um dataset (se necessário) e insere os dados em uma tabela BigQuery usando chunks, se use_chunk=True
    Suporta DataFrames do Pandas ou Polars.
    """
    # 1. Criar dataset se não existir
    try:
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = location
        client.create_dataset(dataset, exists_ok=True)
        print(f"Dataset '{dataset_id}' pronto.")
    except Exception as e:
        print(f"Erro ao criar dataset: {e}")
        return
    # 2. Converter de Polars para Pandas, se necessário
    if isinstance(df, pl.DataFrame):
        df = df.to_pandas()
        print("Convertido de Polars para Pandas.")

    if not isinstance(df, pd.DataFrame):
        print("O objeto fornecido não é um DataFrame válido.")
        return

    if use_chunk == False:

        try:
            write_disposition = bigquery.WriteDisposition.WRITE_APPEND
            job_config = bigquery.LoadJobConfig(write_disposition=write_disposition)
            job = client.load_table_from_dataframe(df, table_id,job_config=job_config)
            job.result()
            print(f"Tabela '{table_id}' criada com {job.output_rows} linhas.")
        except Exception as e:
            print(f"Erro ao criar tabela: {e}")
    else:      
        # 3. Inserir em chunks
        try:
            chunks = split_dataframe(df, chunk_size)
            total_rows = 0

            for i, chunk in enumerate(chunks):
                print(f"Enviando chunk {i+1}/{len(chunks)} com {len(chunk)} linhas...")
                write_disposition = bigquery.WriteDisposition.WRITE_APPEND
                job_config = bigquery.LoadJobConfig(write_disposition=write_disposition)
                job = client.load_table_from_dataframe(chunk, table_id, job_config=job_config)
                job.result()
                total_rows += len(chunk)

            print(f"Tabela '{table_id}' carregada com {total_rows} linhas.")
        except Exception as e:
            print(f"Erro ao carregar tabela por chunks: {e}")

# Função criada para limpar e tratar colunas no formato json
def safe_json_parse(text):
    try:

         # Garante que o texto é string
        if not isinstance(text, str):
            return None
        
        # Etapa 1: limpeza básica
        text = text.strip()

        # Remove aspas externas: "...." → ....
        if text.startswith('"') and text.endswith('"'):
            text = text[1:-1]

        # Etapa 2: transforma \\\" → " (escape duplo)
        text = text.replace('\\\"', '"')

        # Etapa 3: transforma \" → " (escape simples)
        text = text.replace('\"', '"')

        # Etapa 4: substitui aspas duplas duplicadas no início e fim: ""abc"" → "abc"
        text = re.sub(r'""([^"]*?)""', r'"\1"', text)

        # Etapa 5: reduz excesso de aspas seguidas internas: """ → "
        text = re.sub(r'"+', r'"', text)

        # Etapa 6: remove barras soltas antes de aspas
        text = re.sub(r'\\"', r'"', text)

        # Etapa 7: parse final
        return json.loads(text)

    except Exception:
        return None

def send_parquets_to_bigquery(pasta_projeto, dataset_nome, tabela_nome, client, var_insert_date, location="southamerica-east1"):
    """
    Percorre os arquivos 'sales_*.parquet' na pasta_projeto e faz append no BigQuery.
    """
    arquivos = sorted([arq for arq in os.listdir(pasta_projeto) if arq.startswith("sales_") and arq.endswith(".parquet")])

    if not arquivos:
        print("Nenhum arquivo 'sales_*.parquet' encontrado na pasta.")
        return

    for arquivo in arquivos:
        caminho_arquivo = os.path.join(pasta_projeto, arquivo)
        print(f"Lendo arquivo: {arquivo}")

        try:
            df = pl.read_parquet(caminho_arquivo)
            df = df.with_columns([
                    pl.lit(var_insert_date).alias("insert_date")
                ])
        except Exception as e:
            print(f"Erro ao ler {arquivo}: {e}")
            continue

        print(f"Enviando para BigQuery: {arquivo}")
        create_dataset_and_table(
            df=df,
            dataset_nome=dataset_nome,
            tabela_nome=tabela_nome,
            client=client,
            use_chunk=True,
            location=location
        )