from ArchivosFuente import *
from ProcesamientoDeDatos import *

def main():
    #-----ARCHIVOS FUENTE-----
    #define urls to use
    museos_url = 'https://datos.cultura.gob.ar/dataset/37305de4-3cce-4d4b-9d9a-fec3ca61d09f/resource/4207def0-2ff7-41d5-9095-d42ae8207a5d/download/museos_datosabiertos.csv'
    cine_url = 'https://datos.cultura.gob.ar/dataset/37305de4-3cce-4d4b-9d9a-fec3ca61d09f/resource/392ce1a8-ef11-4776-b280-6f1c7fae16ae/download/cine.csv'
    bibliotecas_url = 'https://datos.cultura.gob.ar/dataset/37305de4-3cce-4d4b-9d9a-fec3ca61d09f/resource/01c6c048-dbeb-44e0-8efa-6944f73715d7/download/biblioteca_popular.csv'
    urls = [
            museos_url, 
            cine_url, 
            bibliotecas_url
           ]
    GetSourceFiles(urls)
    print(files)

    #-----PROCESAMIENTO DE DATOS-----
    #create engine and session
    session, engine= GetSession()
    
    #read csv files, normalize column names, keep certain columns and export to PostgreSQL
    all_csv_data= []
    for file in files:
        dataframe = NormalizeColumnName(file)
        dataframe = SelectColumns(dataframe)
        all_csv_data.append(dataframe)
    df_all= pd.concat(all_csv_data,axis=0)
    ExportToSQL(df_all,name= "Normalizada", engine= engine)
    
    #close session and close all currently checked-in sessions
    session.close()
    engine= session.get_bind()
    engine.dispose()

if __name__ == "__main__":
    main()