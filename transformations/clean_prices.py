import os
import pandas as pd

BRONZE_PATH = "../data_lake/bronze/"
SILVER_PATH = "../data_lake/silver/"

# Função para limpar e padronizar os preços dos combustíveis
def clear_and_standardize_prices():
    os.makedirs(SILVER_PATH, exist_ok=True) # Cria o diretório silver se não existir

    all_difs = []

    for file in os.listdir(BRONZE_PATH):
        if file.endswith(".csv"):
            file_path = os.path.join(BRONZE_PATH, file)
            print(f" [] Processando arquivo: {file_path}")

            df = pd.read_csv(file_path, sep=';', encoding="latin1")
            
           # Padronizar nomes das colunas
            df.columns = (
                df.columns.str.strip()
                          .str.lower()
                          .str.replace(" ", "_")
            )

            #converter valor de venda para float
            df["valor_de_venda"] = df["valor_de_venda"].str.replace(",", ".").astype(float)

            #padronizar as datas
            if 'data_da_coleta' in df.columns:
                df['data_da_coleta'] = pd.to_datetime(df['data_da_coleta'], errors='coerce')
            
            df.rename(columns={'estado_-_sigla': 'estado'}, inplace=True)

            #remove linhas vazias 
            df = df.dropna(how='all')

            all_difs.append(df) # Adiciona o DataFrame à lista modificada

            final_df = pd.concat(all_difs, ignore_index=True) # Concatena todos os DataFrames em um único DataFrame
            silver_file_path = os.path.join(SILVER_PATH, f"silver_{file}")
            final_df.to_csv(silver_file_path, index=False) # Salva o DataFrame limpo no diretório silver
            print(f"✔ Arquivo salvo em silver: {silver_file_path}")    

if __name__ == "__main__":
    clear_and_standardize_prices()
    print("Limpeza e padronização dos preços dos combustíveis concluída.")
