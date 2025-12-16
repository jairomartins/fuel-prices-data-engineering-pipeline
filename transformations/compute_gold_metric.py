import os

import pandas as pd

print("Iniciando cálculo da métrica para os preços dos combustíveis  ...")


SILVER_PATH = "../data_lake/silver/"
GOLD_PATH = "../data_lake/gold/"


def compute_gold_preco_medio_mensal():

    dados = {}

    for file in os.listdir(SILVER_PATH):
        if file.endswith(".csv"):

            print(f"Processando arquivo: {file}")

            df = pd.read_csv(os.path.join(SILVER_PATH, file), parse_dates=["data_da_coleta"], low_memory=False)
            df['ano_mes'] = df['data_da_coleta'].dt.to_period('M')  # Extrair ano e mês
            
            # Agrupar por ano_mes e produto, calculando a soma e a contagem dos valores ainda no arquivo 
            grouped = (
                df.groupby(['ano_mes', 'produto'])['valor_de_venda']
                .agg(['sum', 'count'])
                .reset_index()
            )

            for _, row in grouped.iterrows():
                key = (str(row['ano_mes']), row['produto'])
                if key not in dados:
                    dados[key] = {
                        "soma" : row["sum"],
                        "count": row["count"]
                    }
                else:
                    dados[key]["soma"] += row["sum"]
                    dados[key]["count"] += row["count"]

    # Preparar os dados para o DataFrame final
   
    registros = [
        {
            "ano_mes": key[0],
            "produto": key[1],
            "valor_medio": dados[key]["soma"] / dados[key]["count"]
        }
        for key in dados
    ]
    
    #salvar o DataFrame final na camada gold
    resultado_df = pd.DataFrame(registros)
    resultado_df.to_csv(os.path.join(GOLD_PATH, "preco_medio_mensal_produto.csv"), index=False)
    print("Métrica de preço médio mensal calculada e salva na camada gold.")

if __name__ == "__main__":
    compute_gold_preco_medio_mensal()
    print("Cálculo da métrica concluído.")  