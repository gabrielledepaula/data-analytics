#Task: Importação de dados referente a CNPJ

#Step 1: Importar bibliotecas referente a consumo de API(requests, json) e converção/consumo dos dados (Pandas)
import requests
import pandas as pd
import json
from pandas import json_normalize

#step 2: Declaramos uma variavel para usarmos no exemplo com o cnpj da conectcar. Também iremos criar uma variavel chamada url com a url da API.
cnpj = 16577631000299
url = 'https://receitaws.com.br/v1/cnpj/%s' %cnpj #Nesta API, precisamos complementar o endereço com o cnpj que precisamos

#step 3: requisição dos dados de API e atribuição a uma variavel dos resultados da requisição. Após, criamos dataframe com os dados.

response = requests.get(url) #requisição

data = response.json() #atribuição


d = [data] #transforma dados em dicionário
df = pd.DataFrame.from_dict(d) #atribui ao dataframe df o dicionário com os dados

#--------------------
#Task: Importar dados de diversos CNPJ's. Iremos seguir o mesmo processo executado, mas, com os CNPJ's em uma lista e então executamos um loop para realizar diversas consultas.
#Não necessáriamente precisamos inserir uma lista manualmente, podemos ler um arquivo em csv por exemplo.

cnpjs = [41608574000124, 61198164000160]
for cnpj in cnpjs:
    url = 'https://receitaws.com.br/v1/cnpj/%s' %cnpj

    response = requests.get(url)

    data = response.json()


    d = [data]
    df1 = pd.DataFrame.from_dict(d)
    df = df.append(df1, ignore_index = True)

df.to_excel('datav2.xls', sheet_name='Sheet1', index=False, engine='xlsxwriter')

#necessário tratar os dados da API após a extração, pois, a mesma vem com caracteres específicas de arquivos JSON.