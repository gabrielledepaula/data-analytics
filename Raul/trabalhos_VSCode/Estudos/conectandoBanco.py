import psycopg2 #biblioteca para conectar ao banco
import pandas as pd #biblioteca para criação e gerenciamento de data frame. Também utilizada para exportar o dataframe para csv
import pandas.io.sql as sqlio #utilizado para integrar o sql com pandas, permitindo exportar os dados direto do banco para um dataframe


conn = psycopg2.connect(    #estabelece os parametros a serem utilizados para fazer a conexão ao banco de dados
    database="conectcar",
    user="raul_correa", #seu usuário
    password="Conect@091!", #sua senha
    host="sdlf-cntcar-redshift-dev.capkkn406tig.us-east-2.redshift.amazonaws.com",
    port='5439'
)


sql = " " #código sql utilizado para consultar o banco

dataframe = sqlio.read_sql_query(sql, conn) #exporta os dados do banco, obtidos via a consulta do sql, para um dataframe
print(dataframe) #mostra no terminal as colunas e linhas obtidos na consulta
with pd.ExcelWriter('cteativacao.xlsx') as writer:
        dataframe.to_excel(writer, sheet_name='Analisev1', index = False)
         #exporta para um arquivo xlsm, do excel, o dataframe
         #exporta para um arquivo xlsm, do excel, o dataframe


