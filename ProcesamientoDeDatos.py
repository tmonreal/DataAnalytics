import pandas as pd
from sqlalchemy import *
from sqlalchemy_utils import *
from sqlalchemy.orm import *
from local_settings import postgresql as settings

def GetEngine(user, password, host, port, database):
    #create connection with database
    url= f"postgresql://{user}:{password}@{host}:{port}/{database}"
    if not database_exists(url):
        create_database(url, echo=False)
    engine= create_engine(url)
    return engine

def GetEngineFromSettings():
    #get settings for database connection from external file
    keys= ["pguser","pgpassword","pghost","pgport","pgdb"]
    if not all(key in keys for key in settings.keys()):
        raise Exception("Bad configuration file")
    return GetEngine(settings['pguser'],
                     settings['pgpassword'],
                     settings['pghost'],
                     settings['pgport'],
                     settings['pgdb'])

def GetSession():
    engine= GetEngineFromSettings()
    session= sessionmaker(bind=engine)()
    return session, engine

def NormalizeColumnName(filename):
    #change all alias column names with same datatype to one same target name
    df = pd.read_csv(filename)
    for target, aliases in (
        ("cod_localidad", ("Cod_Loc",)),
        ("id_provincia", ("IdProvincia",)),
        ("id_departamento", ("IdDepartamento",)),
        ("categoría", ("Categoría","categoría","categoria")),
        ("provincia", ("Provincia","provincia")),
        ("localidad", ("Localidad","localidad")),
        ("nombre", ("Nombre","nombre")),
        ("domicilio", ("Dirección","Domicilio","direccion")),
        ("código postal", ("CP",)),
        ("número de telefono", ("Teléfono","telefono")),
        ("mail", ("Mail",)),
        ("web", ("Web",)),
        ("fuente", ("fuente","Fuente"))
    ):
        for alias in aliases:
            if alias in df.columns:
                df = df.rename(columns= {alias: target})
    #add a column with upload date
    df["fecha de carga"] = pd.Timestamp.today().strftime("%d-%m-%Y")
    return df

def SelectColumns(dataframe, columns):
    #select the columns to keep in new database
    try:
        dataframe_columns = dataframe[columns]
    except KeyError as key_err:
        print(f"Key error ocurred: {key_err}")
    except Exception as err:
        print(f"Other error ocurred: {err}")
    else:
        return dataframe_columns

def CountValues(dataframe, group, column):
    #count number of values in 'column' grouped by 'group'
    dataframe = dataframe.groupby(group, as_index= False)[column].value_counts(ascending=False)
    df = dataframe.rename(columns= {"count": "cantidad de registros"})
    #add a column with upload date
    df["fecha de carga"] = pd.Timestamp.today().strftime("%d-%m-%Y")
    return df


def ExportToSQL(dataframe, name, engine):
    #export datafram to sql database
    dataframe.to_sql(name= name, 
                     con= engine,
                     index= False,
                     if_exists= "replace"                
                    )
    print(f"Success! Exported {name} to database")