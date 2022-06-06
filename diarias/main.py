import mysql.connector
import pandas as pd
import json
from datetime import date, timedelta
import json
from calha import nomeCalha
#lendo as configurações de conexão do arquivo json
with open('/home/thiago/Documentos/notebooks/create-plan-total/diarias/config.json') as config_file:
    dados_conexao = json.load(config_file)

#conexão ao banco salva na variavel dbsasi
dbsasi = mysql.connector.connect(
   **dados_conexao
)

#cursor apontando para o banco
sasi_cursor = dbsasi.cursor()

#query com a busca que queremos fazer
query = ("SELECT * FROM user WHERE generetedAt LIKE CONCAT (%s,'%');")

#criando as datas para a busca
data_de_operacao = date(2022, 5, 13) #inicio da operacao
hoje = date.today() #data atual


#loop para buscar os dados do banco de acordo com as datas
while(data_de_operacao <= hoje):
    sasi_cursor.execute(query, (data_de_operacao,))
    
    #processo de criar o dataframe
    COLUNAS = [
        'id_sasi',
        'municipio',
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
        'email_cadastrador'
    ]
    df_dados_da_base = pd.DataFrame(columns=COLUNAS)
    
    for (linha) in sasi_cursor:
        #criando a coluna com nome da calha
        lista=list(linha)
        lista.append(nomeCalha(linha[1]))
        linha=tuple(lista)
        
        novaLinha = pd.DataFrame([linha], columns = COLUNAS)
        df_dados_da_base = pd.concat ([df_dados_da_base , novaLinha] )
    
    #limpando os dados
    df_dados_da_base['cpf'] = df_dados_da_base['cpf'].str.replace('-','')
    df_dados_da_base['cpf'] = df_dados_da_base['cpf'].str.replace('.','')
    df_dados_da_base['cpf'] = df_dados_da_base['cpf'].str.replace(' ','')
    
    #salvar o excel com os dados do banco para o dia buscado
    df_dados_da_base.sort_values(by=['id']).to_excel('/home/thiago/Documentos/notebooks/planilhas_diarias/dados_cartao_dia_'+str(data_de_operacao)+'.xlsx',index=False)
    
    
    #próximo dia
    data_de_operacao = data_de_operacao + timedelta(days=1)
    
#fechando a conexão
sasi_cursor.close()
dbsasi.close()