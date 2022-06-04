import mysql.connector
import pandas as pd
import time
import requests
import json
from datetime import date, datetime
import json

#lendo as configurações de conexão do arquivo json
with open('config.json') as config_file:
    dados_conexao = json.load(config_file)

#conexão ao banco salva na variavel dbsasi
dbsasi = mysql.connector.connect(
   **dados_conexao
)

#cursor apontando para o banco
sasi_cursor = dbsasi.cursor()

#query com a busca que queremos fazer
query = ("SELECT * FROM user WHERE generatedAt = %s")

#criando as datas para a busca
hire_start = datetime.date(1999, 1, 1)
hire_end = datetime.date(1999, 12, 31)

sasi_cursor.execute(query, (hire_start, hire_end))