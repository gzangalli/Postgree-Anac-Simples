# %%
import pandas as pd
import psycopg2
from chardet.universaldetector import UniversalDetector

# %%
# 1°: importar json como df

caminho = 'dados/V_OCORRENCIA_AMPLA.json'

detector = UniversalDetector()

with open(caminho, 'rb') as arquivo:
    for linha in arquivo:
        detector.feed(linha)
        if detector.done:
            break

detector.close()
encode = detector.result['encoding']

df = pd.read_json(caminho, encoding=encode)

# %%
# 2°: Tratar os dados levar apenas colunas ["Numero_da_Ocorrencia", "Classificacao_da_Ocorrência", "Data_da_Ocorrencia","Municipio","UF","Regiao","Nome_do_Fabricante"]
colunas = ["Numero_da_Ocorrencia", "Classificacao_da_Ocorrência",
           "Data_da_Ocorrencia", "Municipio", "UF", "Regiao", "Nome_do_Fabricante"]
df = df[colunas]

# 3°: Tirar acento de nomes de colunas
df.rename(columns={
          'Classificacao_da_Ocorrência': 'Classificacao_da_Ocorrencia'}, inplace=True)

# %%
# 4°: Criar Banco de Dados e Tabela
# feito no postgree
"""
CREATE TABLE IF NOT EXISTS Anac (
    Numero_da_Ocorrencia int,
    Classificacao_da_Ocorrencia VARCHAR(50),
    Data_da_Ocorrencia DATE,
    Municipio VARCHAR(50),
    UF VARCHAR(30),
    Regiao VARCHAR(30),
    Nome_do_Fabricante VARCHAR(100)
)
"""

# %%
# 5°: Configurar a conexão

# Parâmetros
dbname = "db"
user = "usuario"
password = "senha"
host = "host"
port = "porta"

# Conectando ao banco de dados
conn = psycopg2.connect(dbname=dbname, user=user,
                        password=password, host=host, port=port)

# Criando um cursor
cur = conn.cursor()

# 7°: Criar um Delete da tabela para nao ter histórico , mantendo em banco sempre os dados mais rescentes
cur.execute("DELETE FROM Anac")

# 6°: Enviar dados para o Postgree
for index, row in df.iterrows():
    cur.execute("""
                INSERT INTO Anac 
                (Numero_da_Ocorrencia, 
                Classificacao_da_Ocorrencia, 
                Data_da_Ocorrencia, 
                Municipio, 
                UF, 
                Regiao, 
                Nome_do_Fabricante) 
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, row)  # row é uma tupla com os valores de cada linha (cada r é uma coluna da linha do df)

# Executando o comando SQL
conn.commit()
# Fechando o cursor
cur.close()
# Fechando a conexão
conn.close()
