import pandas as pd
import mysql.connector
import time
import json
import xlsxwriter 
from datetime import date, datetime, timedelta
from calha import nomeCalha

nomearquivo = "C:/Users/DEV-GW-POWER/Documents/dados_op_enchente_2022/planilha_geral/dados_cartao_total.xlsx"
hoje = date.today()

print(time.strftime("%H:%M:%S"),"- Vamo comecar essa parada agr...")
time.sleep(5)
while(1):
    #abrindo arquivo de logs
    arquivo_logs = False
    while(arquivo_logs == False):
        try:
            logs = open('./logs.txt', 'a')
        except:
            print(time.strftime("%H:%M:%S"),"- Erro ao abrir arquivo de logs (¬¬) \n")
        else:
            arquivo_logs = True
            print(time.strftime("%H:%M:%S"),"- Arquivo de logs aberto com sucesso \n")
    
    time.sleep(2)

    #abrindo conexao com o banco
    conexao_banco = False
    while(conexao_banco == False):
        try:   
            #lendo as configuracoes de conexao do arquivo json
            with open('./config.json') as config_file:
                dados_conexao = json.load(config_file)
                
            #conexao ao banco salva na variavel dbsasi
            dbsasi = mysql.connector.connect(
                **dados_conexao
            )

            #cursor apontando para o banco
            sasi_cursor = dbsasi.cursor()
        except:
            print(time.strftime("%H:%M:%S"),"- Erro ao conectar no banco de dados (¬¬) \n")
            logs.write(time.strftime("%H:%M:%S")+"- Erro ao conectar no banco de dados \n")
        else:
            conexao_banco = True
            print(time.strftime("%H:%M:%S"),"- Conexao com o banco de dados realizada com sucesso \n")
            logs.write(time.strftime("%H:%M:%S")+"- Conexao com o banco de dados realizada com sucesso \n")
    
    time.sleep(2)
    #ler o excel geral
    arquivo_lido = False
    while(arquivo_lido == False):
        #verifica se o arquivo foi aberto corretamente
        try:
            #recupera os dados que ja tem no arquivo excel
            df_atual = pd.read_excel(nomearquivo, encoding= 'unicode_escape')

        except FileNotFoundError as error:
            print(error)
            #aguarda 2 minutos
            print(time.strftime("%H:%M:%S"),"- Arquivo geral nao encontrado (¬¬) \n")
            logs.write(time.strftime("%H:%M:%S")+"- Arquivo geral nao encontrado \n")
            time.sleep(2)
            print(time.strftime("%H:%M:%S"),"- Aguardando 1 minuto antes de tentar novamente...\n")
            time.sleep(60)

        except:
            print(time.strftime("%H:%M:%S"),"- Error na leitura do arquivo geral (¬¬) \n")
            logs.write(time.strftime("%H:%M:%S")+"- Erro na leitura do arquivo geral \n")
            #aguarda 2 minutos
            time.sleep(2)
            print(time.strftime("%H:%M:%S"),"- Aguardando 1 minuto antes de tentar novamente...\n")
            time.sleep(60)

        else:
            print(time.strftime("%H:%M:%S"),"- Arquivo geral lido com sucesso :D\n")
            logs.write(time.strftime("%H:%M:%S")+"- Arquivo geral lido com sucesso \n")
            arquivo_lido = True
            
    #preparacao do select
    query = ("SELECT * FROM custom_110760000100.cartao_associado")
    #buscar tud no BD

    time.sleep(2)
    requisicao_feita = False
    while(requisicao_feita == False):
        #previnir erros no momento de realizar a requisicao
        try:
            #busca novos dados
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

            df_novos = pd.DataFrame(columns=COLUNAS)
            
            
            #inserindo linhas vindas da base no dataframe
            for (linha) in sasi_cursor:
                #criando a coluna com nome da calha
                lista=list(linha)
                lista.append(nomeCalha(linha[1]))
                
                #decode pois o campo data nascimento esta como longtext no banco
                lista[8]=lista[8].decode("utf8")
                linha=tuple(lista)
                
                #crio um dataframe de uma linha so, pois o mysql retorna uma tupla e pra concatenar preciso de 2 df
                novaLinha = pd.DataFrame([linha], columns = COLUNAS)

                #concatena
                df_novos = pd.concat ([df_novos , novaLinha] )

        except:
            #aguarda 2 minutos para fazer uma nova requisicao
            print(time.strftime("%H:%M:%S"),"- Houve um erro na requisicao (¬¬) ")
            logs.write(time.strftime("%H:%M:%S")+"- Houve um erro na requisicao \n")
            time.sleep(2)
            print(time.strftime("%H:%M:%S"),"- Aguardando 1 minuto para fazer uma nova requisicao...\n")
            time.sleep(60)
        else:
            print(time.strftime("%H:%M:%S"),"- Requisicao bem sucedida :D\n")
            logs.write(time.strftime("%H:%M:%S")+"- Requisicao bem sucedida\n")
            requisicao_feita = True
            
    #filtrar os que vieram da busca e que nao tem na geral

    #verificar se existe linhas novas vindas do banco
    #uma comparacao entre os ids sera feita, pois ele deve ser unico
    #caso tenha um id vindo da busca que seja diferende dos ids que ja possuimos, entao ele e um novo registro
    novos_registros = df_novos.loc[~df_novos['id_sasi'].isin(df_atual['id_sasi'])].reset_index(drop=True)
    
    #verificar se o municipio dos novos beneficiarios, afim de evitar mostrar de lugares onde não ha operacao
    #mu_operacao = ['MANACAPURU', 'ITACOATIARA']
    #novos_registros = novos_registros[novos_registros.municipio.isin(mu_operacao)]
    
    #concatenar os dados da geral com com os novos encontrados e filtrados

    #adicionar a nova linha ao dataframe com os dados que já possuímos
    df_atual = pd.concat([df_atual, novos_registros])
    
    #ler a planilha diaria
    arquivo_diario_lido = False
    nome_arquivo_diario = 'C:/Users/DEV-GW-POWER/Documents/dados_op_enchente_2022/planilhas_diarias/dados_cartao_dia_'+str(hoje)+'.xlsx'
    time.sleep(5)
    while(arquivo_diario_lido == False):
        #verifica se o arquivo foi aberto corretamente
        try:
            #recupera os dados que já tem no arquivo excel
            df_diario = pd.read_excel(nome_arquivo_diario, encoding= 'unicode_escape')

        except FileNotFoundError as error:
            print(error)
            #aguarda 2 minutos
            print(time.strftime("%H:%M:%S"),"- Arquivo diario nao encontrado (¬¬) \n")
            logs.write(time.strftime("%H:%M:%S")+"- Arquivo diario nao encontrado \n")
            time.sleep(2)
            
            print(time.strftime("%H:%M:%S"),"- Um novo arquivo para o dia", hoje, "sera criado... \n")
            logs.write(time.strftime("%H:%M:%S")+"- Um novo arquivo para o dia"+ hoje+ "foi criado \n")
            nome_arquivo_diario = 'C:/Users/DEV-GW-POWER/Documents/dados_op_enchente_2022/planilhas_diarias/dados_cartao_dia_'+str(hoje)+'.xlsx'
            workbook = xlsxwriter.Workbook(nome_arquivo_diario)
            worksheet = workbook.add_worksheet()
            workbook.close()
            time.sleep(2)
            print(time.strftime("%H:%M:%S"),"- Arquivo diario criado :D \n")

        except:
            print(time.strftime("%H:%M:%S"),"- Error na leitura do arquivo (¬¬) \n")
            logs.write(time.strftime("%H:%M:%S")+"- Erro na leitura do arquivo diario\n")
            #aguarda 2 minutos
            time.sleep(2)
            print(time.strftime("%H:%M:%S"),"- Aguardando 1 minuto antes de tentar novamente...\n")
            time.sleep(60)

        else:
            print(time.strftime("%H:%M:%S"),"- Arquivo diario lido com sucesso :D\n")
            logs.write(time.strftime("%H:%M:%S")+"- Arquivo diario lido com sucesso \n")
            arquivo_diario_lido = True
            
    #concatenar os filtrados com a planilha de hoje
    df_diario = pd.concat([df_diario, novos_registros])

    #removendo pontos e tracos do campo cpf
    df_diario['cpf'] = df_diario['cpf'].astype(str).str.replace('-','')
    df_diario['cpf'] = df_diario['cpf'].astype(str).str.replace('.','')
    df_diario['cpf'] = df_diario['cpf'].astype(str).str.replace(' ','')
    
    
    time.sleep(5)
    #salvar as alteracoes na planilha diaria
    arquivo_diario_salvo = False
    while(arquivo_diario_salvo == False):
        try:
            #salvando o dataframe diario em excel
            df_diario.sort_values(by=['id_sasi']).to_excel(nome_arquivo_diario,index=False, encoding= 'unicode_escape')

        except:
            print(time.strftime("%H:%M:%S"),"- Houve um erro no momento de salvar o excel diario (¬¬) \n")
            logs.write(time.strftime("%H:%M:%S")+"- Houve um erro no momento de salvar o excel diario \n")
            time.sleep(2)
            print(time.strftime("%H:%M:%S"),"- Aguardando 1 minuto para tentar salvar novamente...\n")
            time.sleep(60)

        else:
            print(time.strftime("%H:%M:%S"),"- Arquivo excel diario foi salvo :D\n")
            arquivo_diario_salvo = True
    
    #removendo pontos e tracos do campo cpf
    df_atual['cpf'] = df_atual['cpf'].astype(str).str.replace('-','')
    df_atual['cpf'] = df_atual['cpf'].astype(str).str.replace('.','')
    df_atual['cpf'] = df_atual['cpf'].astype(str).str.replace(' ','')
    
    time.sleep(5)
    #salvar a planilha geralzona com os dados concatenados
    #substituir o arquivo excel geral por um com os novos dados
    arquivo_geral_salvo = False
    while(arquivo_geral_salvo == False):
        try:
            #salvando o dataframe gigante em excel
            df_atual.sort_values(by=['id_sasi']).to_excel('C:/Users/DEV-GW-POWER/Documents/dados_op_enchente_2022/planilha_geral/dados_cartao_total.xlsx',index=False, encoding= 'unicode_escape')

        except:
            print(time.strftime("%H:%M:%S"),"- Houve um erro no momento de salvar o excel total (¬¬) \n")
            time.sleep(2)
            print(time.strftime("%H:%M:%S"),"- Aguardando 1 minuto para tentar salvar novamente...\n")
            time.sleep(60)

        else:
            print(time.strftime("%H:%M:%S"),"- Arquivo excel total foi salvo :D\n")
            arquivo_geral_salvo = True
            
    time.sleep(5)
    #verifica se o dia mudou
    if(date.today() > hoje):
        #mudando a data de hoje
        print("O dia mudou!\n")
        hoje=date.today
        
        #cria um novo arquivo para o novo dia
        print(time.strftime("%H:%M:%S"),"- Um novo arquivo para o dia", hoje, "sera criado...\n")
        nome_arquivo_diario = 'C:/Users/DEV-GW-POWER/Documents/dados_op_enchente_2022/planilhas_diarias/dados_cartao_dia_'+str(hoje)+'.xlsx'
        workbook = xlsxwriter.Workbook(nome_arquivo_diario)
        worksheet = workbook.add_worksheet()
        workbook.close()
        time.sleep(2)
        print(time.strftime("%H:%M:%S"),"- Arquivo diario criado :D \n")

    time.sleep(5)
    #fechando a conexão com o banco
    banco_fechado = False
    while(banco_fechado == False): 
        try:
            sasi_cursor.close()
            dbsasi.close()
        except:
            print(time.strftime("%H:%M:%S"),"- Houve um erro no momento de fechar a conexao com o banco (¬¬) \n")
            time.sleep(2)
            print(time.strftime("%H:%M:%S"),"- Aguardando 1 minuto para tentar novamente...\n")
            time.sleep(60)
        else:
            print(time.strftime("%H:%M:%S"),"- Conexao com o banco encerrada :D\n")
            banco_fechado = True

    #fechando arquivo de logs
    logs_fechado = False
    while(logs_fechado == False): 
        try:
            logs.close()
        except:
            print(time.strftime("%H:%M:%S"),"- Houve um erro no momento de fechar o arquivo de logs (¬¬) \n")
            time.sleep(2)
            print(time.strftime("%H:%M:%S"),"- Aguardando 1 minuto para tentar novamente...\n")
            time.sleep(60)
        else:
            print(time.strftime("%H:%M:%S"),"- Arquivo de logs fechado :D\n")
            logs_fechado = True

    #esperando para repetir o processo
    print(time.strftime("%H:%M:%S"),"- Aguardando intervalo de tempo para realizar novamente o processo ;D \n")
    time.sleep(60)
    print("Iniciando o processo em: ", time.strftime("%H:%M:%S"), "\n");
    time.sleep(2)
    

    
