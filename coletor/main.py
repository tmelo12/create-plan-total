import pandas as pd
import mysql.connector
import time
import requests
import json
import xlsxwriter 
from datetime import date, datetime, timedelta

def nomeCalha(municipio):
    if(municipio == "GUAJARÁ"    or 
       municipio == "IPIXUNA"    or
       municipio == "EIRUNEPÉ"   or
       municipio == "ITAMARATI"  or
       municipio == "JURUÁ"      or
       municipio == "CARAUARI"   or
       municipio == "ENVIRA"     
      ):
        return "JURUÁ"
    
    elif(municipio == "BOCA DO ACRE" or
       municipio == "PAUINI"       or
       municipio == "LÁBREA"       or
       municipio == "CANUTAMA"     or
       municipio == "TAPAUÁ"       or
       municipio == "BERURI"    
      ):
        return "PURUS"
    
    elif(municipio == "HUMAITÁ"              or
       municipio == "APUÍ"                 or
       municipio == "MANICORÉ"             or
       municipio == "NOVO ARIPUANÃ"        or
       municipio == "BORBA"                or
       municipio == "NOVA OLINDA DO NORTE"
      ):
        return "MADEIRA"
    
    elif(municipio == "ATALAIA DO NORTE"      or
       municipio == "BENJAMIN CONSTANT"     or
       municipio == "TABATINGA"             or
       municipio == "SÃO PAULO DE OLIVENÇA" or
       municipio == "AMATURÁ"               or
       municipio == "SANTO ANTÔNIO DO IÇÁ"  or
       municipio == "TONANTINS"
      ):
        return "ALTO SOLIMÕES"
   
    elif(municipio == "JUTAÍ"     or
       municipio == "FONTE BOA" or
       municipio == "JAPURÁ"    or
       municipio == "MARAÃ"     or
       municipio == "UARINI"    or
       municipio == "ALVARÃES"  or
       municipio == "TEFÉ"      or
       municipio == "COARI"
          ):
            return "MÉDIO SOLIMÕES"
        
    elif(municipio == "CODAJÁS"          or
       municipio == "ANORI"            or
       municipio == "ANAMÃ"            or
       municipio == "CAAPIRANGA"       or
       municipio == "MANACAPURU"       or
       municipio == "IRANDUBA"         or
       municipio == "MANAQUIRI"        or
       municipio == "CAREIRO CASTANHO" or
       municipio == "CAREIRO DA VÁRZEA"
      ):
        return "BAIXO SOLIMÕES"
    
    elif(municipio == "ITACOATIARA"           or
       municipio == "PRESIDENTE FIGUEIREDO" or
       municipio == "RIO PRETO DA EVA"      or
       municipio == "SILVES"                or
       municipio == "AUTAZES"               or
       municipio == "URUCURITUBA"           or
       municipio == "ITAPIRANGA"           
      ):
        return "MÉDIO AMAZONAS"
    
    elif(municipio == "BARREIRINHA"             or
       municipio == "BOA VISTA DO RAMOS"      or
       municipio == "NHAMUNDÁ"                or
       municipio == "URUCARÁ"                 or
       municipio == "SÃO SEBASTIÃO DO UATUMÃ" or
       municipio == "PARINTINS"               or
       municipio == "MAUÉS"
      ):
        return "BAIXO AMAZONAS"
    
    elif(municipio == "SÃO GABRIEL DA CACHOEIRA"  or
       municipio == "SANTA ISABEL DO RIO NEGRO" or
       municipio == "BARCELOS"                  or
       municipio == "NOVO AIRÃO"                or
       municipio == "MANAUS" 
      ):
        return "RIO NEGRO"
    
    else:
        return "NOME CALHA"

nomearquivo = "/home/thiago/Documentos/notebooks/planilha_geral/dados_cartao_total.xlsx"
hoje = date.today()

print("Vamo comecar essa parada agr...")
time.sleep(5)
while(1):
    #abrindo conexao com o banco
    conexao_banco = False
    while(conexao_banco == False):
        try:   
            #lendo as configuracoes de conexao do arquivo json
            with open('/home/thiago/Documentos/notebooks/create-plan-total/coletor/config.json') as config_file:
                dados_conexao = json.load(config_file)
                
            #conexao ao banco salva na variavel dbsasi
            dbsasi = mysql.connector.connect(
                **dados_conexao
            )

            #cursor apontando para o banco
            sasi_cursor = dbsasi.cursor()
        except:
            print("Erro ao conectar no banco de dados (¬¬) \n")
        else:
            conexao_banco = True
            print("Conexao com o banco de dados realizada com sucesso \n")
    
    time.sleep(5)
    #ler o excel geral
    arquivo_lido = False
    while(arquivo_lido == False):
        #verifica se o arquivo foi aberto corretamente
        try:
            #recupera os dados que ja tem no arquivo excel
            df_atual = pd.read_excel(nomearquivo)

        except FileNotFoundError as error:
            print(error)
            #aguarda 2 minutos
            print("Arquivo geral nao encontrado (¬¬) \n")
            time.sleep(2)
            print("Aguardando 2 minutos antes de tentar novamente...\n")
            time.sleep(120)

        except:
            print("Error na leitura do arquivo geral (¬¬) \n")
            #aguarda 2 minutos
            time.sleep(2)
            print("Aguardando 2 minutos antes de tentar novamente...\n")
            time.sleep(120)

        else:
            print("Arquivo geral lido com sucesso :D\n")
            arquivo_lido = True
            
    #preparacao do select
    query = ("SELECT * FROM user")
    #buscar no BD tudo

    time.sleep(5)
    requisicao_feita = False
    while(requisicao_feita == False):
        #previnir erros no momento de realizar a requisicao
        try:
            #busca novos dados
            sasi_cursor.execute(query)
            #criando dataframe para receber os dados do banco
            COLUNAS = [
                        'id',
                        'nome',
                        'cpf',
                        'municipio',
                        'cod_cartao',
                        'status_cartao',
                        'data_entrega',
                        'calha'
            ]

            df_novos = pd.DataFrame(columns=COLUNAS)
            
            #inserindo linhas vindas da base no dataframe
            for (linha) in sasi_cursor:
                #criando a coluna com nome da calha
                lista=list(linha)
                lista.append(nomeCalha(linha[3]))
                linha=tuple(lista)
                
                #crio um dataframe de uma linha so, pois o mysql retorna uma tupla e pra concatenar preciso de 2 df
                novaLinha = pd.DataFrame([linha], columns = COLUNAS)

                #concatena
                df_novos = pd.concat ([df_novos , novaLinha] )

        except:
            #aguarda 2 minutos para fazer uma nova requisicao
            print("Houve um erro na requisicao (¬¬) ")
            time.sleep(2)
            print("Aguardando 2 minutos para fazer uma nova requisicao...\n")
            time.sleep(120)
        else:
            print("Requisicao bem sucedida :D\n")
            requisicao_feita = True
            
    #filtrar os que vieram da busca e que nao tem na geral

    #verificar se existe linhas novas vindas do banco
    #uma comparacao entre os ids sera feita, pois ele deve ser unico
    #caso tenha um id vindo da busca que seja diferende dos ids que ja possuimos, entao ele e um novo registro
    novos_registros = df_novos.loc[~df_novos['id'].isin(df_atual['id'])].reset_index(drop=True)
    
    #verificar se o municipio dos novos beneficiarios, afim de evitar mostrar de lugares onde não ha operacao
    #mu_operacao = ['MANACAPURU', 'ITACOATIARA']
    #novos_registros = novos_registros[novos_registros.municipio.isin(mu_operacao)]
    
    #concatenar os dados da geral com com os novos encontrados e filtrados

    #adicionar a nova linha ao dataframe com os dados que já possuímos
    df_atual = pd.concat([df_atual, novos_registros])
    
    
    #ler a planilha diaria
    arquivo_diario_lido = False
    nome_arquivo_diario = '/home/thiago/Documentos/notebooks/planilhas_diarias/dados_cartao_dia_'+str(hoje)+'.xlsx'
    time.sleep(5)
    while(arquivo_diario_lido == False):
        #verifica se o arquivo foi aberto corretamente
        try:
            #recupera os dados que já tem no arquivo excel
            df_diario = pd.read_excel(nome_arquivo_diario)

        except FileNotFoundError as error:
            print(error)
            #aguarda 2 minutos
            print("Arquivo diario nao encontrado (¬¬) \n")
            time.sleep(2)
            
            print("Um novo arquivo para o dia", hoje, "sera criado...\n")
            nome_arquivo_diario = '/home/thiago/Documentos/notebooks/planilhas_diarias/dados_cartao_dia_'+str(hoje)+'.xlsx'
            workbook = xlsxwriter.Workbook(nome_arquivo_diario)
            worksheet = workbook.add_worksheet()
            workbook.close()
            time.sleep(2)
            print("Arquivo diario criad :D \n")

        except:
            print("Error na leitura do arquivo (¬¬) \n")
            #aguarda 2 minutos
            time.sleep(2)
            print("Aguardando 2 minutos antes de tentar novamente...\n")
            time.sleep(120)

        else:
            print("Arquivo diario lido com sucesso :D\n")
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
            df_diario.sort_values(by=['id']).to_excel(nome_arquivo_diario,index=False)

        except:
            print("Houve um erro no momento de salvar o excel diario (¬¬) \n")
            time.sleep(2)
            print("Aguardando 2 minutos para tentar salvar novamente...\n")
            time.sleep(120)

        else:
            print("Arquivo excel diario foi salvo :D\n")
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
            df_atual.sort_values(by=['id']).to_excel('/home/thiago/Documentos/notebooks/planilha_geral/dados_cartao_total.xlsx',index=False)

        except:
            print("Houve um erro no momento de salvar o excel total (¬¬) \n")
            time.sleep(2)
            print("Aguardando 2 minutos para tentar salvar novamente...\n")
            time.sleep(120)

        else:
            print("Arquivo excel total foi salvo :D\n")
            arquivo_geral_salvo = True
            
    time.sleep(5)
    #verifica se o dia mudou
    if(date.today() > hoje):
        #mudando a data de hoje
        print("O dia mudou!\n")
        hoje=date.today
        
        #cria um novo arquivo para o novo dia
        print("Um novo arquivo para o dia", hoje, "sera criado...\n")
        nome_arquivo_diario = '/home/thiago/Documentos/notebooks/planilhas_diarias/dados_cartao_dia_'+str(hoje)+'.xlsx'
        workbook = xlsxwriter.Workbook(nome_arquivo_diario)
        worksheet = workbook.add_worksheet()
        workbook.close()
        time.sleep(2)
        print("Arquivo diario criado :D \n")

    time.sleep(5)
    #fechando a conexão com o banco
    banco_fechado = False
    while(banco_fechado == False): 
        try:
            sasi_cursor.close()
            dbsasi.close()
        except:
            print("Houve um erro no momento de fechar a conexao com o banco (¬¬) \n")
            time.sleep(2)
            print("Aguardando 2 minutos para tentar novamente...\n")
            time.sleep(120)
        else:
            print("Conexao com o banco encerrada :D\n")
            banco_fechado = True

    #esperando 5 minutos pra fazer novamente
    print("Aguardando intervalo de tempo para realizar novamente o processo :D \n")
    time.sleep(60)
    print("Iniciando o processo em: ", time.strftime("%H:%M:%S"), "\n");
    time.sleep(2)

    
