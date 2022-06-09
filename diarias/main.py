import mysql.connector
import pandas as pd
import json
from datetime import date, timedelta
import json
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
query = ("SELECT * FROM custom_110760000100.cartao_associado WHERE generatedAt LIKE CONCAT (%s,'%');")

#criando as datas para a busca
data_de_operacao = date(2022, 5, 13) #inicio da operacao
hoje = date.today() #data atual


#loop para buscar os dados do banco de acordo com as datas
while(data_de_operacao <= hoje):
    sasi_cursor.execute(query, (data_de_operacao.strftime("%Y-%m-%d"),))
    
    #processo de criar o dataframe
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
    for (linha) in sasi_cursor:
        #criando a coluna com nome da calha
        lista=list(linha)
        lista.append(nomeCalha(linha[1]))
        #decode pois o campo data nascimento esta como longtext no banco
        lista[8]=lista[8].decode("utf8")
        
        linha=tuple(lista)
        
        novaLinha = pd.DataFrame([linha], columns = COLUNAS)
        df_dados_da_base = pd.concat ([df_dados_da_base , novaLinha] )
        print('Linha ',i)
        i=i+1
    
    #limpando os dados
    df_dados_da_base['cpf'] = df_dados_da_base['cpf'].astype(str).str.replace('-','', regex=True)
    df_dados_da_base['cpf'] = df_dados_da_base['cpf'].astype(str).str.replace('.','', regex=True)
    df_dados_da_base['cpf'] = df_dados_da_base['cpf'].astype(str).str.replace(' ','', regex=True)    
    
    #salvar o excel com os dados do banco para o dia buscado
    df_dados_da_base.sort_values(by=['id_sasi']).to_excel('C:/Users/DEV-GW-POWER/Documents/dados_op_enchente_2022/planilhas_diarias/dados_cartao_dia_'+str(data_de_operacao)+'.xlsx',
    index=False, 
    encoding= 'unicode_escape')

    print('Finalizou dia :D',data_de_operacao);
    
    #próximo dia
    data_de_operacao = data_de_operacao + timedelta(days=1)
    
#fechando a conexão
sasi_cursor.close()
dbsasi.close()
