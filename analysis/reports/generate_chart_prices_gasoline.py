import os 
import pandas as pd
import matplotlib.pyplot as plt

#ler os dados de precos da camada gold
GOLD_PATH = "../../data_lake/gold/preco_medio_mensal_produto.csv"
OUTPUT_PATH = "../../outputs/charts/"

def load_gold_data():
    df = pd.read_csv(GOLD_PATH, parse_dates=["ano_mes"])
    return df

def generate_monthly_price_report(df):
    # Converter a coluna 'ano_mes' para datetime
    df['ano_mes'] = pd.to_datetime(df['ano_mes'])
    df = df.sort_values('ano_mes')

    for produto in df['produto'].unique():
        df_p = df[df['produto'] == produto]
        plt.plot(df_p['ano_mes'], df_p['valor_medio'], marker='o', label=produto)

    plt.xlabel('Mês')
    plt.ylabel('Preço médio')
    plt.legend()
    plt.grid(True)
    plt.title('Preço médio mensal por produto')
    plt.savefig(f'{OUTPUT_PATH}/preco_medio_mensal.png')
    plt.show()

if __name__ == "__main__":
    mean_price_df = load_gold_data()
    print("Dados carregados com sucesso!")
    generate_monthly_price_report(mean_price_df)    
    print("Análise mensal de preços concluída.")