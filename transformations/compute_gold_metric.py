import os

import pandas as pd

print("Iniciando cálculo da métrica para os preços dos combustíveis...")


SILVER_PATH = "../data_lake/silver/silver_C_2025_01.csv"
GOLD_PATH = "../data_lake/gold/"

def load_silver():
    df = pd.read_csv(SILVER_PATH, parse_dates=['data_da_coleta'])
    return df

def save_gold(df, file_name):
    os.makedirs(GOLD_PATH, exist_ok=True)
    gold_file_path = os.path.join(GOLD_PATH, file_name )
    df.to_csv(gold_file_path, index=False)
    print(f"salva em: {gold_file_path}")


def compute_metric():
    df = load_silver()

    # Filtra apenas os registros com valor de venda não nulo
    df = df[df["valor_de_venda"].notna()]


    # Calcula o preço médio de venda por produto
    price_mean_by_product = (
        df.groupby("produto")["valor_de_venda"]
        .mean()
        .reset_index()
        .rename(columns={"valor_de_venda": "preco_medio_venda"})    
    )
    save_gold(price_mean_by_product, "gold_metric_preco_medio_venda_produto.csv")

    # Calcula o preço médio de venda por produto e estado
    price_mean_by_product = (
        df.groupby(["estado","produto"])["valor_de_venda"]
        .mean()
        .reset_index()
        .rename(columns={"valor_de_venda": "preco_medio_venda"})    
    )
    save_gold(price_mean_by_product, "gold_metric_preco_medio_venda_estado.csv")

    #ranking dos estados por preço médio de venda de produto = 'Gasolina'

    gasolina_df = df[df["produto"] == "GASOLINA"]
    ranking_gasolina = (
        gasolina_df.groupby("estado")["valor_de_venda"]
        .mean()
        .reset_index()
        .rename(columns={"valor_de_venda": "preco_medio_gasolina"})
        .sort_values(by="preco_medio_gasolina", ascending=False)
        .reset_index(drop=True)
    )
    save_gold(ranking_gasolina, "gold_metric_ranking_estados_gasolina.csv") 


if __name__ == "__main__":
    compute_metric()
    print("Cálculo da métrica concluído.")  