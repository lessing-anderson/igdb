#Imports
from api_igdb_handler import get_twitch_token
from api_igdb_handler import ApiIGDBHandler

#Configs
client_id = ""
client_secret = ""

    print('Obtendo token da twitch...')
    token = get_twitch_token(client_id, client_secret)
    print('Ok.\n')

    print('Criando classe de ingestão...')
    ingestor = ApiIGDBHandler(client_id, token, path)
    print('Ok.\n')

    print('Iniciando o processo...')
    ingestor.process(endpoint, **params)
    print('Ok.\n')


#Exec
endpoint = 'games'

path = './'

print('\n############################################')
print('Executando para endpoint:', endpoint)
collect(endpoint=endpoint, path=path)