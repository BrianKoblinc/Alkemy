"""
Challenge Data Analytics - Brian Koblinc

1- Obtener los 3 archivos de fuente utilizando la librería requests y
almacenarse en forma local:
"""

# Se importan las librerias request y csv
import requests
import csv

# Se descargan los archivos a partir la URLs que les correponden y se vuelca su contenido en un string
cine = requests.get('https://datos.cultura.gob.ar/dataset/37305de4-3cce-4d4b-9d9a-fec3ca61d09f/resource/392ce1a8-ef11-4776-b280-6f1c7fae16ae/download/cine.csv')
museo = requests.get('https://datos.cultura.gob.ar/dataset/37305de4-3cce-4d4b-9d9a-fec3ca61d09f/resource/4207def0-2ff7-41d5-9095-d42ae8207a5d/download/museo.csv')
biblioteca = requests.get('https://datos.cultura.gob.ar/dataset/37305de4-3cce-4d4b-9d9a-fec3ca61d09f/resource/01c6c048-dbeb-44e0-8efa-6944f73715d7/download/biblioteca_popular.csv')

cine_content = cine.content
museo_content = museo.content
biblioteca_content = biblioteca.content

c = cine_content.decode("utf-8")
m = museo_content.decode("utf-8")
b = biblioteca_content.decode("utf-8")

# Se importa la libreria pandas y se carga la informacion de los strings en 
# dataframes separando el contenido de las filas de los titulos de la columnas (headers)

import pandas as pd

cine_csv_reader = csv.reader(c.splitlines(), delimiter=',')
museo_csv_reader = csv.reader(m.splitlines(), delimiter=',')
biblioteca_csv_reader = csv.reader(b.splitlines(), delimiter=',')

cine_list = list(cine_csv_reader)
museo_list = list(museo_csv_reader)
biblioteca_list = list(biblioteca_csv_reader)

c_headers = cine_list[0]
data_cine = cine_list[1:]
m_headers = museo_list[0]
data_museo = museo_list[1:]
b_headers = biblioteca_list[0]
data_biblioteca = biblioteca_list[1:]

cine_df = pd.DataFrame(data=data_cine, columns=c_headers)
museo_df = pd.DataFrame(data=data_museo, columns=m_headers)
biblioteca_df = pd.DataFrame(data=data_biblioteca, columns=b_headers)

"""
2- Organizar los archivos en rutas siguiendo la siguiente estructura:
“categoría\año-mes\categoria-dia-mes-año.csv”
"""

# Se importa la fecha actual y se crea una funcion que guarda los dataframes cumpliendo segun los requerimientos exigidos

from datetime import date

today = date.today()
year = today.strftime("%Y")
month = today.strftime("%m")
day = today.strftime("%d")
month_name = {"01":"enero", "02":"febrero", "03":"marzo", "04":"abril", "05":"mayo", "06":"junio", 
              "07":"julio", "08":"agosto", "09":"septiembre", "10":"octubre", "11":"noviembre", "12":"diciembre"}

def save_csv(df, cat):
  df.to_csv(r"C:\Users\Brian\Desktop\Programacion\Alkemy\{cat}\{year}-{month_name}\{cat}-{today}\{cat}.csv".format(cat = cat, year = year, month = month, month_name = month_name[month], today = today), index = True)

save_csv(cine_df, "cine")
save_csv(museo_df, "museo")
save_csv(biblioteca_df, "biblioteca")

"""
3- Normalizar toda la información de Museos, Salas de Cine y Bibliotecas
Populares, para crear una única tabla para crear una única tabla que contenga:
o cod_localidad
o id_provincia
o id_departamento
o categoría
o provincia
o localidad
o nombre
o domicilio
o código postal
o número de teléfono
o mail
o web
"""

# Se toman los tres dataframes y se modificacan para saleccionar/crear las columnas de interes con el mismo orden y nombre

import numpy as np

cine_df_norm = cine_df[["Cod_Loc", "IdProvincia", "IdDepartamento", "Categoría", "Provincia", "Localidad", "Departamento", "Nombre", "Dirección", "Piso", "CP", "Teléfono", "Mail", "Web", "Fuente"]]
cine_df_norm["Domicilio"] = np.where(cine_df_norm["Piso"] == 's/d', cine_df_norm["Dirección"], cine_df_norm["Dirección"] + ", " + cine_df_norm["Piso"])
cine_df_norm = cine_df_norm.drop(["Dirección", "Piso"], 1)
cols_cine_df_norm = cine_df_norm.columns.tolist()
cols_cine_df_norm = cols_cine_df_norm[:8] + [cols_cine_df_norm[13]] +cols_cine_df_norm[8:13]
cine_df_norm = cine_df_norm[cols_cine_df_norm]
cine_df_norm.set_axis(["cod_localidad", "id_provincia", "id_departamento", "categoría", "provincia", "localidad", "departamento", "nombre", "domicilio", "código postal", "número de teléfono", "mail", "web", "fuente"], axis=1, inplace=True)

museo_df_norm = museo_df[["Cod_Loc", "IdProvincia", "IdDepartamento", "categoria", "provincia", "localidad", "nombre", "direccion", "CP", "telefono", "Mail", "Web", "fuente"]]
museo_df_norm.set_axis(["cod_localidad", "id_provincia", "id_departamento", "categoría", "provincia", "localidad", "nombre", "domicilio", "código postal", "número de teléfono", "mail", "web", "fuente"], axis=1, inplace=True)
museo_df_norm["departamento"] = np.nan
museo_df_norm = museo_df_norm[["cod_localidad", "id_provincia", "id_departamento", "categoría", "provincia", "localidad", "departamento", "nombre", "domicilio", "código postal", "número de teléfono", "mail", "web", "fuente"]]

biblioteca_df_norm = biblioteca_df[["Cod_Loc", "IdProvincia", "IdDepartamento", "Categoría", "Provincia", "Localidad", "Departamento", "Nombre", "Domicilio", "CP", "Teléfono", "Mail", "Web", "Fuente"]]
biblioteca_df_norm.set_axis(["cod_localidad", "id_provincia", "id_departamento", "categoría", "provincia", "localidad", "departamento", "nombre", "domicilio", "código postal", "número de teléfono", "mail", "web", "fuente"], axis=1, inplace=True)

# Se crea una dataframe que contiene la informacion de espacios culturales a partir de la concatenacion de los dataframes de cines, museos y bibliotecas 

esp_cult_df = pd.concat([cine_df_norm, museo_df_norm,biblioteca_df_norm], ignore_index=True)
esp_cult_info_df = esp_cult_df[["cod_localidad", "id_provincia", "id_departamento", "categoría", "provincia", "localidad", "departamento", "nombre", "domicilio", "código postal", "número de teléfono", "mail", "web"]]

#Se remueven los espacios a la derecha e izquierda de cada dato cargado y se reemplazan los string s/d por el objeto np.nan

def remueve_espacio(x):
  if type(x)==str:
    return x.lstrip().rstrip()
  else:
    return x

def remueve_sd(x):
  if x == 's/d' or x == "":
    return np.nan
  else:
    return x

esp_cult_info_df = esp_cult_info_df.applymap(remueve_espacio)
esp_cult_info_df = esp_cult_info_df.applymap(remueve_sd)

# Se normaliza la informacion de museos, salas de cine y bibliotecas populares

provincia = esp_cult_info_df[["id_provincia", "provincia"]].drop_duplicates(subset=["id_provincia"]).set_index(["id_provincia"]).sort_values(by=["id_provincia"])
provincia['provincia'].replace("Tierra del Fuego","Tierra del Fuego, Antártida e Islas del Atlántico Sur", inplace = True)

departamento = esp_cult_info_df[["id_departamento", "departamento"]].dropna().drop_duplicates(subset=["id_departamento"]).set_index(["id_departamento"]).sort_values(by=["id_departamento"])

localidad = esp_cult_info_df[["cod_localidad", "localidad"]].drop_duplicates(subset=["cod_localidad"]).set_index(["cod_localidad"]).sort_values(by=["cod_localidad"])

def sumar_uno(x):
  if type(x)==int:
    return x+1
  else:
    return x

categoria = pd.DataFrame(data=esp_cult_info_df["categoría"].unique(), columns= ["categoría"]).reset_index(level=0).rename(columns={"index": "id_categoría"})
categoria['id_categoría'] = categoria['id_categoría'].apply(sumar_uno)
esp_cult_info_df = esp_cult_info_df.merge(categoria, on="categoría")
categoria = categoria.set_index(["id_categoría"])

espacios_culturales = esp_cult_info_df[["id_provincia", "id_departamento", "cod_localidad", "id_categoría", "nombre", "domicilio", "código postal", "número de teléfono", "mail", "web"]]

# Se crea la tabla solicitada a partir de la interseccion de las tablas espacios_culturales, provincia, departamento, localidad y categoría

espacios_culturales_info = espacios_culturales.merge(provincia, on = "id_provincia").merge(departamento, on = "id_departamento").merge(localidad, on = "cod_localidad").merge(categoria, on = "id_categoría")
espacios_culturales_info = espacios_culturales_info[["cod_localidad", "id_provincia", "id_departamento", "categoría", "provincia", "localidad", "departamento", "nombre", "domicilio", "código postal", "número de teléfono", "mail", "web"]]

"""
4- Procesar los datos conjuntos para poder generar una tabla con la siguiente
información:
o Cantidad de registros totales por categoría
o Cantidad de registros totales por fuente
o Cantidad de registros por provincia y categoría
"""

# Se reemplazan valores de la tabla esp_cult_df para agruparlos en categorias mas amplias

esp_cult_df['fuente'].replace(["INCAA / SInCA", "CNMLH - Enlace SInCA", "SInCA"],"INCAA", inplace = True)
esp_cult_df['fuente'].replace("RCC- Córdoba","RCC", inplace = True)
esp_cult_df['fuente'].replace(["Gobierno de la Provincia", "Secretaria de Cultura Provincial",
                               "Dirección Provincial de Patrimonio Cultural - Enlace SInCA",
                               "Gobierno de la Provincia de Mendoza" ,"Gobierno de la Provincia de Jujuy",
                               "Gobierno de la Provincia de San Juan", "Gobierno de la Provincia de La Pampa",
                                "Gobierno de la Provincia de La Rioja", "Gobierno de la provincia",
                                "Gobierno de la Provincia de Neuquén", "Gobierno de la Provincia de Salta", 
                               "CNMLH - Ente Cultural de Tucumán", "Gob. Pcia.", "Gobierno de la Provincia de Chubut / SInCA 2013" ], "Provincial", inplace = True)
esp_cult_df['fuente'].replace(["Municipalidad de Santa Fe - Red de Ciudades Creativas" ,"Red de Espacios Culturales Vicente López",
                               "Área Gestión Cultural de la Municipalidad de Colonia Caroya", "Municipalidad de Río Gallegos - Red de Ciudades Creativas",
                                "Municipalidad de Puerto Madryn", "Dirección de Cultura de Villa Allende",
                                "Secretaria de Cultura del Municipio de Esteban Echeverría", "UNLA", 
                               "Dirección de Cultura y de Educación de la Municipalidad de Unquillo" ], "Municipal", inplace = True)
esp_cult_df['provincia'].replace("Neuquén\xa0","Neuquén", inplace = True)
esp_cult_df['provincia'].replace("Santa Fe","Santa Fé", inplace = True)
esp_cult_df['provincia'].replace("Tierra del Fuego","Tierra del Fuego, Antártida e Islas del Atlántico Sur", inplace = True)
esp_cult_df['categoría'].replace("Espacios de Exhibición Patrimonial","Museos", inplace = True)

# Se crean tablas pivot categoria vs provincia y fuente vs provincia

esp_cult_df["Clasificación por"] = "Categoría"
esp_cult_prov_cat_df = esp_cult_df.pivot_table(index=['Clasificación por','categoría'], columns='provincia', aggfunc='size', fill_value=0)
esp_cult_df["Clasificación por"] = "Fuente"
esp_cult_prov_fuen_df = esp_cult_df.pivot_table(index=['Clasificación por','fuente'], columns='provincia', aggfunc='size', fill_value=0)

# Se agrega una ultima fila que contiene los registros totales para cada fuente y categoria

esp_cult_prov_cat_df['Total'] = esp_cult_prov_cat_df[esp_cult_prov_cat_df.columns.values].sum(axis=1)
esp_cult_prov_fuen_df['Total'] = esp_cult_prov_fuen_df[esp_cult_prov_fuen_df.columns.values].sum(axis=1)

# Se modifica el formato de las tablas para mejorar su presentacion

esp_cult_prov_cat_df.index.set_names('', level = 1, inplace = True)
esp_cult_prov_fuen_df.index.set_names('', level = 1, inplace = True)

# Se concatena las tablas pivot
esp_cult_prov_cat_df = esp_cult_prov_cat_df.T
esp_cult_prov_fuen_df = esp_cult_prov_fuen_df.T
esp_cult_prov_df = pd.concat([esp_cult_prov_fuen_df, esp_cult_prov_cat_df], axis=1)

# Se modifica el formato de tabla para que se corresponda con el necesario para ser cargado a una tabla SQL
esp_cult_prov_df = esp_cult_prov_df.reset_index(level=0)
nom_col_cat_fuen = ['provincia','CONABIP','DNPyM','INCAA','Municipal','Provincial','RCC','Bibliotecas Populares','Museos','Salas de cine']
cat_fuen_vs_prov = pd.DataFrame(data=esp_cult_prov_df.values.tolist(), columns=nom_col_cat_fuen)

"""
5- Procesar la información de cines para poder crear una tabla que contenga:
o Provincia
o Cantidad de pantallas
o Cantidad de butacas
o Cantidad de espacios INCAA
"""

# Se crea una tabla con la informacion requerida sobre las salas de cine

salas_de_cine = cine_df[["Provincia",  "Pantallas", "Butacas", "espacio_INCAA"]]

salas_de_cine["espacio_INCAA"].replace("SI",1, inplace = True)
salas_de_cine["espacio_INCAA"].replace("si",1, inplace = True)
salas_de_cine["espacio_INCAA"].replace("0",0, inplace = True)
salas_de_cine["espacio_INCAA"].replace("",0, inplace = True)
salas_de_cine.astype({"Butacas": 'int64', "Pantallas": 'int64'})
salas_de_cine = salas_de_cine.groupby("Provincia").agg({lambda x: pd.to_numeric(x, errors='coerce').sum()})
salas_de_cine.set_axis(["Cantidad de pantallas", "Cantidad de butacas", "Cantidad de espacios INCAA"], axis=1, inplace=True)
salas_de_cine = salas_de_cine.reset_index(level=0)


# Se guardan como csv las 3 tablas de interes

save_csv(espacios_culturales_info, "espacios_culturales")
save_csv(cat_fuen_vs_prov, "cat_fuen_vs_prov")
save_csv(salas_de_cine, "salas_de_cine")

"""
6- Guardar la informacion en un base de datos que cumple con los siguientes requisitos:
● La base de datos debe ser PostgreSQL
● Se deben crear los scripts .sql para la creación de las tablas.
● Se debe crear un script .py que ejecute los scripts .sql para facilitar el deploy.
● Los datos de la conexión deben poder configurarse fácilmente para facilitar
el deploy en un nuevo ambiente de ser necesario.
"""

# Se crean las tablas en la base de datos "Alkemy" y se cargan con los datos con los archivos .csv

import psycopg2
from psycopg2 import DatabaseError

def create_tables():
    """ create tables in the PostgreSQL database"""
    create = [
        """
        CREATE TABLE IF NOT EXISTS public.espacios_culturales(
            cod_localidad integer NOT NULL,
            id_provincia integer NOT NULL,
            id_departamento integer NOT NULL,
            categoria text COLLATE pg_catalog."default" NOT NULL,
            provincia text COLLATE pg_catalog."default" NOT NULL,
            localidad text COLLATE pg_catalog."default" NOT NULL,
            departamento text COLLATE pg_catalog."default" NOT NULL,
            nombre text COLLATE pg_catalog."default",
            domicilio text COLLATE pg_catalog."default",
            codigo_postal text COLLATE pg_catalog."default",
            numero_de_telefono text COLLATE pg_catalog."default",
            web text COLLATE pg_catalog."default"
            )
        """,
        """
        CREATE TABLE IF NOT EXISTS public.cat_fuen_vs_prov(
            provincia text COLLATE pg_catalog."default" NOT NULL,
            CONABIP integer NOT NULL,
            DNPyM integer NOT NULL,
            INCAA integer NOT NULL,
            Municipal integer NOT NULL,
            Provincial integer NOT NULL,
            RCC integer NOT NULL,
            Bibliotecas populares integer NOT NULL,
            Museos integer NOT NULL,
            Salas de cine integer NOT NULL
            )
        """,
        """
        CREATE TABLE IF NOT EXISTS public.salas_de_cine(
            provincia text COLLATE pg_catalog."default" NOT NULL,
            cantidad de pantallas integer NOT NULL,
            cantidad de butacas integer NOT NULL,
            cantidad de espacios INCAA boolean NOT NULL
            )
        """
        ]
    importCSV = [
        r"""
        COPY espacios_culturales(
            cod_localidad,
            id_provincia,
            id_departamento,
            provincia,
            localidad,
            departamento,
            categoria,
            nombre,
            domicilio,
            codigo_postal,
            numero_de_telefono,
            web)
        FROM r"C:\Users\Brian\Desktop\Programacion\Alkemy\{cat}\{year}-{month_name}\{cat}-{today}\{cat}.csv".format(cat = espacios_culturales, year = year, month = month, month_name = month_name[month], today = today)
        DELIMITER ','
        CSV HEADER;
        """,
        r"""
        COPY public.cat_fuen_vs_prov(
            provincia,
            CONABIP,
            DNPyM,
            INCAA,
            Municipal,
            Provincial,
            RCC,
            Bibliotecas populares,
            Museos,
            Salas de cine,)
        FROM r"C:\Users\Brian\Desktop\Programacion\Alkemy\{cat}\{year}-{month_name}\{cat}-{today}\{cat}.csv".format(cat = cat_fuen_vs_prov, year = year, month = month, month_name = month_name[month], today = today)
        DELIMITER ','
        CSV HEADER;
        """,
        r"""
        COPY salas_de_cine(
            provincia,
            cantidad de pantallas,
            cantidad de butacas,
            cantidad de espacios INCAA
            )
        FROM r"C:\Users\Brian\Desktop\Programacion\Alkemy\{cat}\{year}-{month_name}\{cat}-{today}\{cat}.csv".format(cat = salas_de_cine, year = year, month = month, month_name = month_name[month], today = today)
        DELIMITER ','
        CSV HEADER;
        """
        ]
    connection = None
    try:
        connection = psycopg2.connect(
            host='localhost',
            user='postgres',
            password='AlkemyBrianKoblinc',
            database='Alkemy'
        )
        print("Conexión exitosa.")
        cursor = connection.cursor()
        # create table one by one
        for command in create:
            cursor.execute(command)
        for command in importCSV:
            cursor.execute(command)
        # close communication with the PostgreSQL database server
        cursor.close()
        # commit the changes
        connection.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if connection is not None:
            connection.close()

if __name__ == '__main__':
    create_tables()