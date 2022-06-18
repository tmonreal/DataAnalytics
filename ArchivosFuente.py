import os
import datetime
import locale
import requests as req
from requests.exceptions import HTTPError

#get current path and date to save files in spanish (ES)
path = os.path.dirname(os.path.realpath(__file__))
locale.setlocale(locale.LC_TIME,"es_ES")
date = datetime.datetime.now()
files = []

def GetSourceFiles(urls):
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
            directory = (path
                         +"\\"
                         +category
                         +"\\"
                         +date.strftime("%Y-%B")
                         +"\\")
            files.append(SaveSourceFiles(category,
                                         directory,
                                         response.content))

def SaveSourceFiles(category, directory, data):
    if not os.path.exists(directory):
        os.makedirs(directory)
    with open(directory
              +category
              +"-"
              +date.strftime("%d-%m-%Y")
              +".csv",'wb') as f:
        f.write(data)
    return f.name

