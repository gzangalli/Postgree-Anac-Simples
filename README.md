# Postgree Anac Simples
Estudo sobre como popular um banco de dados utilizando python. Foi utilizado um arquivo .json com dados da ANAC para fazer o estudo

fonte: [https://dados.gov.br/dados/organizacoes/visualizar/agencia-nacional-de-aviacao-civil-anac](https://dados.gov.br/dados/conjuntos-dados/ocorrncias-aeronuticas)

Passos do projeto:

- importar .json como df

- Tratar os dados levar apenas colunas ["Numero_da_Ocorrencia", "Classificacao_da_Ocorrência", "Data_da_Ocorrencia","Municipio","UF","Regiao","Nome_do_Fabricante"]

- Tirar acento de nomes de colunas

- Criar Banco de Dados e Tabela 

- Configurar a conexão 

- Enviar dados para o Postgree

- Criar um Delete da tabela para nao ter histórico , mantendo em banco sempre os dados mais rescentes 
