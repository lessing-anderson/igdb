# Databricks notebook source
# DBTITLE 1,Funções
import json
import os
import requests
import datetime

def get_twitch_token(client_id, client_secret):

        url = "https://id.twitch.tv/oauth2/token"

        params = {
            "client_id" : client_id,
            "client_secret" : client_secret,
            "grant_type" : "client_credentials",
            }

        resp = requests.post(url, params=params)
        data = resp.json()

        token = data['access_token']
        return token

class ApiIGDBHandler:

    def __init__(self, client_id, token, path) -> None:
        self.headers = {
            "Client-ID": client_id,
            "Authorization": f"Bearer {token}",
        }
        self.base_url = 'https://api.igdb.com/v4/{sufix}'
        self.path = path
        self.delta_timestamp = datetime.datetime.now() - datetime.timedelta(days=1)
        self.delta_timestamp = int(self.delta_timestamp.timestamp())


    def get_data(self, sufix, body):

        url = self.base_url.format(sufix=sufix)
        data = requests.post(url, headers=self.headers, data=body)
        return data.json()
    
    def save_data(self, data, sufix):

        name = datetime.datetime.now().strftime("%Y%m%d_%H%M%S.%f")

        print(f'Salvando em {self.path}/{sufix}/{name}.json')

        os.makedirs(f'./{sufix}', exist_ok=True)
        with open(f'{self.path}/{sufix}/{name}.json', 'w') as open_file:
            json.dump(data, open_file)
        return True

    def get_and_save(self, sufix, body):
        data = self.get_data(sufix, body)
        self.save_data(data, sufix)
        return data

    def process(self, sufix):
        offset = 0       

        print("Iniciando loop...")
        while True:
        
            body = f"""fields *;
                limit 500;
                offset {offset};
                where updated_at >= {self.delta_timestamp};
                sort updated_at asc;"""
            print("Obtendo dados...")
            data = self.get_and_save(sufix, body)

            if len(data) < 500:
                print("Finalizando loop... ")
                return True

            print("+500 loop...")
            offset += 500

