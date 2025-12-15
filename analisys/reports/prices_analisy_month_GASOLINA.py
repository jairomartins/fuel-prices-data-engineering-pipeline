import os 
import pandas as pd
import matplotlib.pyplot as plt

#ler os dados de precos da camada gold
GOLD_PATH = "../../data_lake/gold/preco_medio_mensal_produto.csv"
OUTPUT_PATH = "../outputs/charts/"

def load_gold_data():
    df = pd.read_csv(GOLD_PATH, parse_dates=["ano_mes"])
    return df

def generate_monthly_price_report(df):
    # Converter a coluna 'ano_mes' para datetime
    df['ano_mes'] = pd.to_datetime(df['ano_mes'].astype(str))

    # Filtrar dados para um produto específico, por exemplo, 'GASOLINA'
    produto_especifico = 'GASOLINA'
    df_produto = df[df['produto'] == produto_especifico]

    # Plotar o gráfico de preços médios mensais
    plt.figure(figsize=(12, 6))
    plt.plot(df_produto['ano_mes'], df_produto['valor_medio'], marker='o')
    plt.title(f'Preço Médio Mensal de {produto_especifico}')
    plt.xlabel('Mês')
    plt.ylabel('Preço Médio (R$)')
    plt.grid(True)
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(f'{OUTPUT_PATH}/{produto_especifico}_preco_medio_mensal.png')
    plt.show()
    print(f"Relatório gerado para {produto_especifico}.")

if __name__ == "__main__":
    mean_price_df = load_gold_data()
    print("Dados carregados com sucesso!")
    generate_monthly_price_report(mean_price_df)    
    print("Análise mensal de preços concluída.")