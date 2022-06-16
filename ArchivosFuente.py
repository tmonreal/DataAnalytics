import os
import datetime
import locale
import requests as req
from requests.exceptions import HTTPError

#get current path and date to save files in spanish (ES)
path = os.path.dirname(os.path.realpath(__file__))
locale.setlocale(locale.LC_TIME,"es_ES")
date = datetime.datetime.now()

#define urls to use
museos_url = 'https://datos.cultura.gob.ar/dataset/37305de4-3cce-4d4b-9d9a-fec3ca61d09f/resource/4207def0-2ff7-41d5-9095-d42ae8207a5d/download/museos_datosabiertos.csv'
salas_de_cine_url = 'https://datos.cultura.gob.ar/dataset/37305de4-3cce-4d4b-9d9a-fec3ca61d09f/resource/392ce1a8-ef11-4776-b280-6f1c7fae16ae/download/cine.csv'
bibliotecas_url = 'https://datos.cultura.gob.ar/dataset/37305de4-3cce-4d4b-9d9a-fec3ca61d09f/resource/01c6c048-dbeb-44e0-8efa-6944f73715d7/download/biblioteca_popular.csv'

print((((museos_url.rsplit('/',1)[-1]).rsplit('.',1)[0]).partition('_')[0]))
print((salas_de_cine_url.rsplit('/',1)[-1]).rsplit('.',1)[0])
print((bibliotecas_url.rsplit('/',1)[-1]).rsplit('.',1)[0].partition('_')[0])

urls = [museos_url, salas_de_cine_url, bibliotecas_url]


for url in urls:
    try:
        response = req.get(url)
        #response was successful, no exception raised
        response.raise_for_status()

    except HTTPError as http_err:
        print(f"HTTP error ocurred: {http_err}")

    except Exception as err:
        print(f"Other error ocurred: {err}")
        
    else:
        #get category name from url and define the directory where the file will be saved
        category = (url.rsplit('/',1)[-1]).rsplit('.',1)[0].partition('_')[0]
        directory = path+ "\\"+ category+ "\\"+ date.strftime("%Y-%B")+ "\\"
        if not os.path.exists(directory):
            os.makedirs(directory)
        with open(directory+ category+ "-"+ date.strftime("%d-%m-%Y")+ ".csv",'wb') as f:
            f.write(response.content)


