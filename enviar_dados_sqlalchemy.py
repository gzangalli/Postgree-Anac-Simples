# %%
import pandas as pd
import sqlalchemy
from chardet.universaldetector import UniversalDetector

# %%
pd.set_option('display.max_columns', None)
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
colunas = ["Numero_da_Ocorrencia", "Classificacao_da_Ocorrência", "Data_da_Ocorrencia","Municipio","UF","Regiao","Nome_do_Fabricante", "Modelo"]
df = df[colunas]

df.rename(columns={'Classificacao_da_Ocorrência':'Classificacao_da_Ocorrencia'}, inplace=True)

# %%
dbname = 'dbname'
user = 'user'
password = 'senha'
host = 'host'
port = 'porta'

# String de conexão
string_conexao = f'postgresql://{user}:{password}@{host}:{port}/{dbname}'

# Criar conexão com o banco de dados
conn = sqlalchemy.create_engine(string_conexao)

nome_tabela = 'anac_sqlalchemy'

# Inserir dados no banco de dados
df.to_sql(nome_tabela, conn, index=False, if_exists='replace', dtype={
    "Numero_da_Ocorrencia": sqlalchemy.types.INTEGER(),
    "Classificacao_da_Ocorrencia": sqlalchemy.types.VARCHAR(length=50),
    "Data_da_Ocorrencia": sqlalchemy.types.DATE(),
    "Municipio": sqlalchemy.types.VARCHAR(length=50),
    "UF": sqlalchemy.types.VARCHAR(length=50),
    "Regiao": sqlalchemy.types.VARCHAR(length=50),
    "Nome_do_Fabricante": sqlalchemy.types.VARCHAR(length=50),
    "Modelo": sqlalchemy.types.VARCHAR(length=50)
    })
# replace -> substitui a tabela caso ela exista
# append -> adiciona os dados na tabela existente
# index -> cria uma coluna de index do df como uma coluna da tabela. Se for False, não cria a coluna de index
# ALTER TABLE anac_sqlalchemy RENAME COLUMN index TO id;
# if_exists -> se a tabela existir, substitui ela


# Fechar conexão
conn.dispose()


