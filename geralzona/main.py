import mysql.connector
import pandas as pd
import json
from datetime import datetime
from calha import nomeCalha

#lendo as configurações de conexão do arquivo json
with open('./config.json') as config_file:
    dados_conexao = json.load(config_file)
    
#conexão ao banco salva na variavel dbsasi
dbsasi = mysql.connector.connect(
   **dados_conexao
)

#cursor apontando para o banco
sasi_cursor = dbsasi.cursor()

#query com a busca que queremos fazer
query = ("SELECT * FROM custom_110760000100.cartao_associado limit 0,10") 

#executando a busca
sasi_cursor.execute(query)

#criando dataframe para receber os dados do banco
COLUNAS = [
    'id_sasi',
    'municipio',
    'cep',
    'card',
    'status_valecard',
    'cpf',
    'rg',
    'nome',
    'data_de_nascimento',
    'nome_da_mae',
    'telefone_para_contato',
    'cd_cartao',
    'renda_mensal',
    'qual_situacao',
    'numero',
    'tipo_endereco',
    'servido_publico',
    'trabalhador_informal',
    'classificacao_moradia',
    'confirmar_anexo_cartao',
    'count_anexos',
    'foto_rg0',
    'foto_rg1',
    'foto_rg2',
    'foto_rg3',
    'foto_cd_cartao0',
    'foto_cd_cartao1',
    'foto_cd_cartao2',
    'foto_cd_cartao3',
    'rg_cpf_anexo',
    'rg_cpf_anexo1',
    'rg_cpf_anexo2',
    'generatedAt',
    'nome_cadastrador',
    'telefone_cadastrador',
    'email_cadastrador',
    'calha'
]

df_dados_da_base = pd.DataFrame(columns=COLUNAS)
i=1
#inserindo linhas vindas da base no dataframe
for (linha) in sasi_cursor:
    #criando a coluna com nome da calha
    lista=list(linha)
    lista.append(nomeCalha(linha[1]))
    
    #decode pois o campo data nascimento esta como longtext no banco
    lista[8]=lista[8].decode("utf8")
    
    linha=tuple(lista)
    
    #crio um dataframe de uma linha só, pois o mysql retorna uma tupla e pra concatenar preciso de 2 df
    novaLinha = pd.DataFrame([linha], columns = COLUNAS)
    
    #concatena
    df_dados_da_base = pd.concat ([df_dados_da_base , novaLinha] )
    print('Linha ',i)
    i=i+1

#limpando os dados
df_dados_da_base['cpf'] = df_dados_da_base['cpf'].astype(str).str.replace('-','', regex=True)
df_dados_da_base['cpf'] = df_dados_da_base['cpf'].astype(str).str.replace('.','', regex=True)
df_dados_da_base['cpf'] = df_dados_da_base['cpf'].astype(str).str.replace(' ','', regex=True)

#salvar em excel ordenando por id
df_dados_da_base.sort_values(by=['id_sasi']).to_excel('C:/Users/Thiago/Documents/dados_op_enchente_2022/planilha_geral/dados_cartao_total.xlsx',
index=False, 
encoding= 'unicode_escape')

#fechando a conexão
sasi_cursor.close()
dbsasi.close()
