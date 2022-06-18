import pandas as pd
from sqlalchemy import *
from sqlalchemy_utils import *
from sqlalchemy.orm import *
from local_settings import postgresql as settings

def GetEngine(user, password, host, port, database):
    url= f"postgresql://{user}:{password}@{host}:{port}/{database}"
    if not database_exists(url):
        create_database(url)
    engine= create_engine(url)
    return engine

def GetEngineFromSettings():
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
        ("codigo postal", ("CP",)),
        ("numero de telefono", ("Teléfono","telefono")),
        ("mail", ("Mail",)),
        ("web", ("Web",))
    ):
        for alias in aliases:
            if alias in df.columns:
                df = df.rename(columns= {alias: target})
    return df

def SelectColumns(dataframe):
    #select the columns to keep in new database
    try:
        dataframe_columns = dataframe[["cod_localidad",
                                    "id_provincia",
                                    "id_departamento",
                                    "categoría",
                                    "provincia",
                                    "localidad",
                                    "nombre",
                                    "domicilio",
                                    "codigo postal",
                                    "numero de telefono",
                                    "mail",
                                    "web"]]
    except KeyError as key_err:
        print(f"Key error ocurred: {key_err}")
    except Exception as err:
        print(f"Other error ocurred: {err}")
    else:
        return dataframe_columns

def ExportToSQL(dataframe, name, engine):
    dataframe.to_sql(name= name, 
                     con=engine,
                     index=False,
                     if_exists="replace",
                    )

