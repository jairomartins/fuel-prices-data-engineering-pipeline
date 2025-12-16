FONTE: Agência Nacional do Petróleo, Gás Natural e Biocombustíveis 

https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/shpc/dsas/ca/

📂 Fonte de Dados do Projeto

Este projeto utiliza dados públicos oficiais sobre preços de combustíveis no Brasil, disponibilizados pelo Governo Federal por meio do portal dados.gov.br.

Os dados são usados como base para a construção de um pipeline de Engenharia de Dados, seguindo a arquitetura Bronze → Silver → Gold.

🌐 Onde encontrar a base de dados

A base de dados utilizada está disponível no portal oficial de dados abertos do governo:

Portal: dados.gov.br
Conjunto de dados: Série histórica de preços de combustíveis e de GLP

No portal, a base pode ser encontrada pesquisando pelo termo:

"Série histórica de preços de combustíveis"


Ou navegando pela categoria relacionada à Agência Nacional do Petróleo, Gás Natural e Biocombustíveis (ANP).

📊 O que a base de dados contém

A base disponibiliza informações coletadas periodicamente pela ANP, incluindo, entre outros campos:

Região e estado

Município

Identificação da revenda

Produto (gasolina, etanol, diesel, etc.)

Data da coleta

Valor de venda

Valor de compra

Unidade de medida

Bandeira da revenda

Os arquivos são fornecidos no formato CSV, normalmente organizados por período (ano/mês).

📥 Download dos dados

Os dados podem ser baixados diretamente no portal, selecionando o período desejado.
Cada arquivo corresponde a um intervalo específico da série histórica.

Após o download, os arquivos devem ser armazenados inicialmente na camada Bronze do projeto.

Exemplo:

data_lake/
└── bronze/
    ├── ca-2004-01.csv
    ├── ca-2004-02.csv
    ├── ca-2005-01.csv
    └── ...

🔄 Uso dos dados no projeto

O fluxo de uso da base segue as etapas:

Bronze:
Dados brutos, exatamente como foram disponibilizados no portal oficial.

Silver:
Limpeza, padronização de colunas, tipos de dados e tratamento de valores inválidos.

Gold:
Dados agregados e consolidados, prontos para análise e visualização.

Essa separação permite rastrear a origem da informação e reproduzir o pipeline a qualquer momento.

⚠️ Observações importantes

Por se tratar de dados públicos reais, alguns arquivos podem conter:

valores ausentes

inconsistências de formato

arquivos vazios ou incompletos

O pipeline foi desenvolvido para lidar com esses cenários sem interromper a execução.

📌 Objetivo do uso da base

A utilização dessa base tem fins educacionais e demonstrativos, com foco em:

Engenharia de Dados

Processamento de grandes volumes

Boas práticas de pipelines de dados

Análises exploratórias e geração de métricas

🔗 Referência

Agência Nacional do Petróleo, Gás Natural e Biocombustíveis (ANP)
Portal Brasileiro de Dados Abertos – dados.gov.br