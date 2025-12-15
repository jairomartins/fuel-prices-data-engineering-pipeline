import os

import pandas as pd

print("Iniciando cálculo da métrica para os preços dos combustíveis...")


SILVER_PATH = "../data_lake/silver/"
GOLD_PATH = "../data_lake/gold/"



def load_silver():
    dfs = []
    for file in os.listdir(SILVER_PATH):
        if file.endswith(".csv"):
            file_path = os.path.join(SILVER_PATH, file)
            print(f"📥 Lendo silver: {file_path}")
            
            df = pd.read_csv(
                file_path,
                parse_dates=["data_da_coleta"]
            )
            print(df.head(2))
            dfs.append(df)
            cont += 1

    if not dfs:
        raise ValueError("Nenhum arquivo CSV encontrado no SILVER_PATH")

    silver_df = pd.concat(dfs, ignore_index=True)
    return silver_df

def save_gold(df, file_name):
    os.makedirs(GOLD_PATH, exist_ok=True)
    gold_file_path = os.path.join(GOLD_PATH, file_name)
    
    df.to_csv(gold_file_path, index=False)
    print(f"💾 Gold salvo em: {gold_file_path}")



def compute_gold_preco_medio_mensal():
    df = load_silver()

    df["ano_mes"] = df["data_da_coleta"].dt.to_period("M")

    gold_df = (
        df.groupby(["ano_mes", "produto"], as_index=False)
          .agg(valor_medio=("valor_de_venda", "mean"))
    )

    save_gold(gold_df, "preco_medio_mensal_produto.csv")


if __name__ == "__main__":
    compute_gold_preco_medio_mensal()
    print("Cálculo da métrica concluído.")  