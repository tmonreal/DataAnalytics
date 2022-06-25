import logging
from ArchivosFuente import *
from ProcesamientoDeDatos import *

logging.basicConfig(filename='db.log')
logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)

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
    #download and save csv in folder
    files = GetSourceFiles(urls)

    #-----PROCESAMIENTO DE DATOS-----
    #create engine and session
    session, engine= GetSession()

    #read csv files, normalize column names, keep certain columns and export to PostgreSQL
    all_csv_data = []
    for file in files:
        dataframe = NormalizeColumnName(file)
        dataframe_selected = SelectColumns(dataframe, ["cod_localidad",
                                                       "id_provincia",
                                                       "id_departamento",
                                                       "categoría",
                                                       "provincia",
                                                       "localidad",
                                                       "nombre",
                                                       "domicilio",
                                                       "código postal",
                                                       "número de telefono",
                                                       "mail",
                                                       "web",
                                                       "fuente",
                                                       "fecha de carga"])
        all_csv_data.append(dataframe_selected) 
        if "cine" in file:
            df_movies = SelectColumns(dataframe, ["provincia",
                                                  "Pantallas",
                                                  "Butacas",
                                                  "espacio_INCAA",
                                                  "fecha de carga"])
    df_all = pd.concat(all_csv_data, axis=0)

    #export normalized table
    df_normalized = df_all.drop(columns=["fuente"])
    ExportToSQL(df_normalized, name= "Normalizada", engine= engine)

    #export number of values per category
    df_category = CountValues(df_all, "categoría", "categoría")
    ExportToSQL(df_category, name= "Categoría", engine= engine)

    #export number of values per source
    df_source = CountValues(df_all, "fuente", "fuente")
    ExportToSQL(df_source, name= "Fuente", engine= engine)

    #export number of values per province and category
    df_province = CountValues(df_all, "provincia", "categoría")
    ExportToSQL(df_province, name= "Provincia y Categoría", engine= engine)

    #movies table 
    ExportToSQL(df_movies, name= "Cine", engine= engine)

    #close session and close all currently checked-in sessions
    session.close()
    engine= session.get_bind()
    engine.dispose()

if __name__ == "__main__":
    main()