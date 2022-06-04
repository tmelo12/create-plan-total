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
    
#sakdokoadokaodkkapd