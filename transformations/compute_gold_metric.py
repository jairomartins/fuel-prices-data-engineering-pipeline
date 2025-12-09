import os

import pandas as pd

print("Iniciando cálculo da métrica para os preços dos combustíveis...")


SILVER_PATH = "../data_lake/silver/silver_C_2025_01.csv"
GOLD_PATH = "../data_lake/gold/"

def load_silver():
    df = pd.read_csv(SILVER_PATH, parse_dates=['data_da_coleta'])
    df['valor_de_venda'] = pd.to_numeric(df['valor_de_venda'], errors='coerce')

    return df

def save_gold(df, file_name):
    os.makedirs(GOLD_PATH, exist_ok=True)
    gold_file_path = os.path.join(GOLD_PATH, file_name )
    df.to_csv(gold_file_path, index=False)
    print(f"salva em: {gold_file_path}")


def compute_metric():
    df = load_silver()

    # Exemplo de cálculo de métrica: Preço médio por tipo de combustível
    gold_df = df.groupby('produto', as_index=False)['valor_de_venda'].mean()
    gold_df.rename(columns={'valor_de_venda': 'preco_medio'}, inplace=True)

    save_gold(gold_df, "gold_metric_precos_combustivel.csv")

if __name__ == "__main__":
    compute_metric()
    print("Cálculo da métrica concluído.")  