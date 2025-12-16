import os
import pandas as pd

BRONZE_PATH = "../data_lake/bronze/"
SILVER_PATH = "../data_lake/silver/"

# Função para limpar e padronizar os preços dos combustíveis
def clear_and_standardize_prices():
    os.makedirs(SILVER_PATH, exist_ok=True)

    for file in os.listdir(BRONZE_PATH):
        if not file.endswith(".csv"):
            continue

        file_path = os.path.join(BRONZE_PATH, file)
        print(f"[R] Processando arquivo: {file_path}")

        df = pd.read_csv(file_path, sep=";", encoding="utf-8-sig", low_memory=False) # Ler o arquivo CSV encoding utf-8-sig = BOM para lidar com caracteres especiais

        # Padronizar colunas
        df.columns = (
            df.columns
            .str.strip() # Remover espaços em branco
            .str.lower() # Converter para minúsculas
            .str.replace(r"[^\w]", "_", regex=True) # Substituir caracteres especiais por _
            .str.replace(r"_+", "_", regex=True) # Substituir múltiplos _ por um único _
            .str.strip("_") # Remover _ do início e fim
        )

        # Converter preço em float
        df["valor_de_venda"] = (
            df["valor_de_venda"]
            .str.replace(",", ".", regex=False)
            .astype(float)
        )

        # Datas
        if "data_da_coleta" in df.columns:
            df["data_da_coleta"] = pd.to_datetime(
                df["data_da_coleta"], errors="coerce"
            )

        # Renomear coluna
        df.rename(columns={"estado_-_sigla": "estado"}, inplace=True)

        # Remover linhas totalmente vazias
        df.dropna(how="all", inplace=True)

        silver_file_path = os.path.join(SILVER_PATH, f"silver_{file}")
        df.to_csv(silver_file_path, index=False)

        print(f"[S] Arquivo salvo em silver: {silver_file_path}")

if __name__ == "__main__":
    clear_and_standardize_prices()
    print("✔ Silver layer concluída com sucesso.")
