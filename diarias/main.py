import mysql.connector
import pandas as pd
import time
import requests
import json
from datetime import datetime
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