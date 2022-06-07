import mysql.connector
import pandas as pd
import json
from datetime import datetime
from calha import nomeCalha

#lendo as configurações de conexão do arquivo json
with open('/home/dev03/Downloads/create-plan-total-master/geralzona/config.json') as config_file:
    dados_conexao = json.load(config_file)
    
#conexão ao banco salva na variavel dbsasi
dbsasi = mysql.connector.connect(
   **dados_conexao
)

#cursor apontando para o banco
sasi_cursor = dbsasi.cursor()

#query com a busca que queremos fazer
query = ("SELECT * FROM custom_110760000100.vw_cartao_associado") 

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
#tipage dos dados
df_dados_da_base['municipio'] = df_dados_da_base['municipio'].astype(str)
df_dados_da_base['cpf'] = df_dados_da_base['cpf'].astype(str)
df_dados_da_base['rg'] = df_dados_da_base['rg'].astype(str)
df_dados_da_base['nome'] = df_dados_da_base['nome'].astype(str)
df_dados_da_base['data_de_nascimento'] = df_dados_da_base['data_de_nascimento'].astype(str)
df_dados_da_base['cd_cartao'] = df_dados_da_base['cd_cartao'].astype(str)
df_dados_da_base['renda_mensal'] = df_dados_da_base['renda_mensal'].astype(str)
df_dados_da_base['qual_situacao'] = df_dados_da_base['qual_situacao'].astype(str)
df_dados_da_base['numero'] = df_dados_da_base['numero'].astype(str)
df_dados_da_base['tipo_endereco'] = df_dados_da_base['tipo_endereco'].astype(str)
df_dados_da_base['servido_publico'] = df_dados_da_base['servido_publico'].astype(str)
df_dados_da_base['trabalhador_informal'] = df_dados_da_base['trabalhador_informal'].astype(str)
df_dados_da_base['classificacao_moradia'] = df_dados_da_base['classificacao_moradia'].astype(str)
df_dados_da_base['confirmar_anexo_cartao'] = df_dados_da_base['confirmar_anexo_cartao'].astype(str)
df_dados_da_base['foto_rg0'] = df_dados_da_base['foto_rg0'].astype(str)
df_dados_da_base['foto_rg1'] = df_dados_da_base['foto_rg1'].astype(str)
df_dados_da_base['foto_rg2'] = df_dados_da_base['foto_rg2'].astype(str)
df_dados_da_base['foto_rg3'] = df_dados_da_base['foto_rg3'].astype(str)
df_dados_da_base['foto_cd_cartao0'] = df_dados_da_base['foto_cd_cartao0'].astype(str)
df_dados_da_base['foto_cd_cartao1'] = df_dados_da_base['foto_cd_cartao1'].astype(str)
df_dados_da_base['foto_cd_cartao2'] = df_dados_da_base['foto_cd_cartao2'].astype(str)
df_dados_da_base['foto_cd_cartao3'] = df_dados_da_base['foto_cd_cartao3'].astype(str)
df_dados_da_base['rg_cpf_anexo'] = df_dados_da_base['rg_cpf_anexo'].astype(str)
df_dados_da_base['rg_cpf_anexo1'] = df_dados_da_base['rg_cpf_anexo1'].astype(str)
df_dados_da_base['rg_cpf_anexo2'] = df_dados_da_base['rg_cpf_anexo2'].astype(str)
i=1
#inserindo linhas vindas da base no dataframe
for (linha) in sasi_cursor:
    #criando a coluna com nome da calha
    lista=list(linha)
    lista.append(nomeCalha(str(linha[1])))
    linha=tuple(lista)
    
    #crio um dataframe de uma linha só, pois o mysql retorna uma tupla e pra concatenar preciso de 2 df
    novaLinha = pd.DataFrame([linha], columns = COLUNAS)
    #concatena
    df_dados_da_base = pd.concat ([df_dados_da_base , novaLinha] )
    print('Linha ',i)
    i=i+1

#limpando os dados
df_dados_da_base['cpf'] = df_dados_da_base['cpf'].str.replace('-','', regex=True).astype(str)
df_dados_da_base['cpf'] = df_dados_da_base['cpf'].str.replace('.','', regex=True).astype(str)
df_dados_da_base['cpf'] = df_dados_da_base['cpf'].str.replace(' ','', regex=True).astype(str)

#tipage dos dados
df_dados_da_base['municipio'] = df_dados_da_base['municipio'].astype(str)
df_dados_da_base['cpf'] = df_dados_da_base['cpf'].astype(str)
df_dados_da_base['rg'] = df_dados_da_base['rg'].astype(str)
df_dados_da_base['nome'] = df_dados_da_base['nome'].astype(str)
df_dados_da_base['data_de_nascimento'] = df_dados_da_base['data_de_nascimento'].astype(str)
df_dados_da_base['cd_cartao'] = df_dados_da_base['cd_cartao'].astype(str)
df_dados_da_base['renda_mensal'] = df_dados_da_base['renda_mensal'].astype(str)
df_dados_da_base['qual_situacao'] = df_dados_da_base['qual_situacao'].astype(str)
df_dados_da_base['numero'] = df_dados_da_base['numero'].astype(str)
df_dados_da_base['tipo_endereco'] = df_dados_da_base['tipo_endereco'].astype(str)
df_dados_da_base['servido_publico'] = df_dados_da_base['servido_publico'].astype(str)
df_dados_da_base['trabalhador_informal'] = df_dados_da_base['trabalhador_informal'].astype(str)
df_dados_da_base['classificacao_moradia'] = df_dados_da_base['classificacao_moradia'].astype(str)
df_dados_da_base['confirmar_anexo_cartao'] = df_dados_da_base['confirmar_anexo_cartao'].astype(str)
df_dados_da_base['foto_rg0'] = df_dados_da_base['foto_rg0'].astype(str)
df_dados_da_base['foto_rg1'] = df_dados_da_base['foto_rg1'].astype(str)
df_dados_da_base['foto_rg2'] = df_dados_da_base['foto_rg2'].astype(str)
df_dados_da_base['foto_rg3'] = df_dados_da_base['foto_rg3'].astype(str)
df_dados_da_base['foto_cd_cartao0'] = df_dados_da_base['foto_cd_cartao0'].astype(str)
df_dados_da_base['foto_cd_cartao1'] = df_dados_da_base['foto_cd_cartao1'].astype(str)
df_dados_da_base['foto_cd_cartao2'] = df_dados_da_base['foto_cd_cartao2'].astype(str)
df_dados_da_base['foto_cd_cartao3'] = df_dados_da_base['foto_cd_cartao3'].astype(str)
df_dados_da_base['rg_cpf_anexo'] = df_dados_da_base['rg_cpf_anexo'].astype(str)
df_dados_da_base['rg_cpf_anexo1'] = df_dados_da_base['rg_cpf_anexo1'].astype(str)
df_dados_da_base['rg_cpf_anexo2'] = df_dados_da_base['rg_cpf_anexo2'].astype(str)
df_dados_da_base['qual_situacao'] = df_dados_da_base['qual_situacao'].astype(str)
df_dados_da_base['qual_situacao'] = df_dados_da_base['qual_situacao'].astype(str)

#salvar em excel ordenando por id
df_dados_da_base.sort_values(by=['id_sasi']).to_excel('/home/dev03/Downloads/planilhas_op_enchente_2022/planilha_geral/dados_cartao_total.xlsx',index=False)

#fechando a conexão
sasi_cursor.close()
dbsasi.close()