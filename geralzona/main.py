import mysql.connector
import pandas as pd
import json
from datetime import datetime

#lendo as configurações de conexão do arquivo json
with open('/home/thiago/Documentos/notebooks/create-plan-total/geralzona/config.json') as config_file:
    dados_conexao = json.load(config_file)
    
#conexão ao banco salva na variavel dbsasi
dbsasi = mysql.connector.connect(
   **dados_conexao
)

#cursor apontando para o banco
sasi_cursor = dbsasi.cursor()

#query com a busca que queremos fazer
query = ("SELECT * FROM user") 

#executando a busca
sasi_cursor.execute(query)

#criando dataframe para receber os dados do banco
COLUNAS = [
    'id',
    'nome',
    'cpf',
    'municipio',
    'cod_cartao',
    'status_cartao',
    'data_entrega'
]

df_dados_da_base = pd.DataFrame(columns=COLUNAS)

#inserindo linhas vindas da base no dataframe
for (linha) in sasi_cursor:
    #crio um dataframe de uma linha só, pois o mysql retorna uma tupla e pra concatenar preciso de 2 df
    novaLinha = pd.DataFrame([linha], columns = COLUNAS)
    
    #concatena
    df_dados_da_base = pd.concat ([df_dados_da_base , novaLinha] )

#limpando os dados
df_dados_da_base['cpf'] = df_dados_da_base['cpf'].str.replace('-','')
df_dados_da_base['cpf'] = df_dados_da_base['cpf'].str.replace('.','')
df_dados_da_base['cpf'] = df_dados_da_base['cpf'].str.replace(' ','')

#salvar em excel ordenando por id
df_dados_da_base.sort_values(by=['id']).to_excel('/home/thiago/Documentos/notebooks/planilha_geral/dados_cartao_total.xlsx',index=False)

#fechando a conexão
sasi_cursor.close()
dbsasi.close()