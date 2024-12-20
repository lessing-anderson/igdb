#Imports
from api_igdb_handler import get_twitch_token
from api_igdb_handler import ApiIGDBHandler

#Configs
client_id = "vbkhb77282t3qo0yzufmrfayla7aiy"
client_secret = "fytfavcv1j29c0q2ccoo7ksbnmvy20"

print('Criando classe de ingestão...')
ingestor = ApiIGDBHandler(client_id, client_secret)
print('Ok.\n')

#Exec
endpoint = 'games'
extract_type = 'full'

print('\n############################################')
print('Executando para endpoint:', endpoint)
ingestor.process(endpoint, extract_type)
