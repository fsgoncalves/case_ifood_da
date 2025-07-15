from google.cloud import bigquery
from google.oauth2 import service_account
import polars as pl
import pandas as pd
import os
import gc
import matplotlib.pyplot as plt
import seaborn as sns

# Caminho do projeto e credenciais
pasta_projeto = "D:\\__case_ifood"
credencial_gcp = os.path.join(pasta_projeto, "case-ifood-fsg-6f1d7cf34e08.json")

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
def calculate_evaluation_metrics(data_frame, columns_groupby):
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
    
    return DataFrame

# Função para calcular a qtde de clientes que fizeram 2 pedidos
def calculate_engagement_custormers_two_orders(data_frame, columns_groupby, val = 2):
    customers_with_2_orders = data_frame.group_by(["is_target","customer_id"] + columns_groupby).agg([
    pl.col("order_id").count().alias("orders")
]).filter(pl.col("orders") == val).group_by(["is_target"] + columns_groupby).agg([
    pl.col("customer_id").n_unique().alias("customers_with_2_orders") 
])
    return customers_with_2_orders

# Função para calcular a qtde de clientes que fizeram 3 pedidos
def calculate_engagement_custormers_three_orders(data_frame, columns_groupby, val = 3):

    customers_with_3_orders = data_frame.group_by(["is_target","customer_id"] + columns_groupby).agg([
    pl.col("order_id").count().alias("orders")
]).filter(pl.col("orders") == val).group_by(["is_target"]  + columns_groupby).agg([
    pl.col("customer_id").n_unique().alias("customers_with_3_orders")
])
    return customers_with_3_orders

# Função para calcular a qtde de clientes que fizeram mais de 3 pedidos
def calculate_engagement_custormers_three_plus_orders(data_frame, columns_groupby, val = 3):

    customers_with_3_plus_orders = data_frame.group_by(["is_target","customer_id"] + columns_groupby).agg([
    pl.col("order_id").count().alias("orders")
]).filter(pl.col("orders") > val).group_by(["is_target"] + columns_groupby).agg([
    pl.col("customer_id").n_unique().alias("customers_with_3_plus_orders")
])
    return customers_with_3_plus_orders