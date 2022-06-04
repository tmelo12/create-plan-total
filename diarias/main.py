import mysql.connector
import pandas as pd
import json
from datetime import date, timedelta
import json

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
                'id',
                'nome',
                'cpf',
                'municipio',
                'cod_cartao',
                'status_cartao',
                'data_entrega'
    ]
    df_dados_da_base = pd.DataFrame(columns=COLUNAS)
    
    for (linha) in sasi_cursor:
        novaLinha = pd.DataFrame([linha], columns = COLUNAS)
        df_dados_da_base = pd.concat ([df_dados_da_base , novaLinha] )
        
    #salvar o excel com os dados do banco para o dia buscado
    df_dados_da_base.sort_values(by=['id']).to_excel('/home/thiago/Documentos/notebooks/planilhas_diarias/dados_cartao_dia_'+str(data_de_operacao)+'.xlsx',index=False)
    
    
    #próximo dia
    data_de_operacao = data_de_operacao + timedelta(days=1)
    
#fechando a conexão
sasi_cursor.close()
dbsasi.close()