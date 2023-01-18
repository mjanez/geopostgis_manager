#!/usr/bin/env python3
## File: shp_to_postgis.py
## Coding: UTF-8
## Author: Manuel Ángel Jáñez García (mjanez@tragsa.es)
## Institution: Tragsatec
## Project: OPEN-IACS
## Goal: The goal of this script is to unzip, merge and load to PostGIS spatial warehouse the IACS data from FEGA.
## Parent: -
""" Changelog:
    v1.2 - 30 May 2022: Add IT data and year parameter
    v1.1 - 14 Dec 2021: Add LT and ES data
    v1.0 - 23 Jul 2021: Functional script with parallelization of enclosures (rec) and statements (ld)
    v0.1 - 29 Jun 2021: Create the first version
"""
# Update the version when aplly changes
version = "1.2"

##~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~##
##           shp_to_postgis.py          ##
##~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~##

# Call this script as "python3 shp_to_postgis.py --config_folder 'config_folder'". Put the config.yml inside and check that the parameters are correct.

## Import libraries                                                          
from datetime import datetime
import argparse as ap
import logging
from subprocess import Popen, PIPE
import yaml
import glob
from functools import reduce
from pathlib import Path
import os
import shutil
from joblib import Parallel, delayed
import shapely
from shapely.geometry.polygon import Polygon
from shapely.geometry.multipolygon import MultiPolygon
from geoalchemy2 import Geometry, WKTElement
import sqlalchemy
from sqlalchemy.ext.declarative import declarative_base 
from sqlalchemy import MetaData
import pandas as pd
import geopandas as gpd                                         # Requires fiona, pyproj, shapely and rtree
import psycopg2

shapely.speedups.disable()

# Logging
def log_file(config_folder):
    """Starts the logger --config_folder parameter entered
    
    Required Parameters:
        --config_folder: Folder where config.yml is located 
    """
    logger = logging.getLogger()
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)

    log_folder = config_folder + "/log/"
    if not os.path.exists(log_folder):
        os.makedirs(log_folder)

    logging.basicConfig(filename= log_folder + "shp_to_postgis-" + datetime.now().strftime("%Y-%m-%d") + ".log",
                            format="%(asctime)s %(levelname)s::%(message)s",
                            datefmt="%Y-%m-%d %H:%M:${0}", 
                            level=logging.INFO
                            )
    return logger

# Parse global var config_folder
def parser():
    """Read the --config_folder parameter entered
    
    Required Parameters:
        --config_folder: Folder where config.yml is located 
    """
    parser = ap.ArgumentParser(description="Creation of RDF Triplets based on Valors (INES) stored in CSV files")
    parser.add_argument(
        "--config_folder",
        action="store",
        help="Folder where config.yml is located",
        )
    args = parser.parse_args()

    if args.config_folder is None:
        config_folder = input("Enter the config folder path: ")
        log_file(config_folder)
        logging.info("// OPEN-IACS/shp_to_postgis.py // Version:" + version)
        logging.info("-----------CLI MODE----------")
        logging.info("Config folder: " + str(Path(config_folder)))
        return config_folder

    elif args.config_folder is not None:
        config_folder = args.config_folder
        log_file(config_folder)
        logging.info("// OPEN-IACS/shp_to_postgis.py // Version:" + version)
        logging.info("-----------CLI MODE----------")
        logging.info("Config folder: " + str(Path(config_folder)))
        return config_folder

# Define the required parameters
def parameters(config_folder):
    """Read the --config_folder parameter entered and return the required parameters from the YAML 
    
    Required Parameters:
        --config_folder: Folder where config.yml is located 
    """
    # Import config.yml parameters
    def get_config_Valor(key, cfg):
        """Read the YAML 
    
        Optional Parameters:
            --key: Key
            --cfg: Config element
        """
        return reduce(lambda c, k: c[k], key.split('.'), cfg)

    with open(config_folder + "/config.yml") as stream:
        config = yaml.safe_load(stream)
        shp_folder = get_config_Valor('default.shp_folder', config)
        processes = get_config_Valor('shp_to_postgis.processes', config) 
        host = get_config_Valor('db_dsn.host', config) 
        port = get_config_Valor('db_dsn.port', config) 
        username = get_config_Valor('db_dsn.username', config)
        password = get_config_Valor('db_dsn.password', config) 
        dbname = get_config_Valor('db_dsn.dbname', config)
        dbschema = get_config_Valor('shp_to_postgis.dbschema', config) 
        countries_upload = get_config_Valor('shp_to_postgis.countries_upload', config)
        carto_upload = get_config_Valor('shp_to_postgis.carto_upload', config)
        db_updates = get_config_Valor('db_updates', config)

    '''
    # Delete all tables in schema py_shp
    met_engine = sqlalchemy.create_engine('postgresql://' + username + ':' + password + '@'+ host + ':' + port + '/' + dbname)
    metadata = MetaData(schema=dbschema)
    metadata.reflect(bind=met_engine)
    metadata.drop_all(met_engine, checkfirst=True)
    logging.info(f'Deleting all tables in schema ' + dbschema)
    '''
    
    return shp_folder, processes, host, port, username, password, dbschema, dbname, countries_upload, carto_upload, db_updates

def shp2pgsql(file, tbl_name, dbschema, dbtypes, engine):
    """Write the ESRI Shapefile files from ZIP and store into a PostGIS Database 
    
    Required Parameters:
        --file: Path file
        --tbl_name: Table name from PostGIS Database
        --dbschema: PostGIS Database Schema
        --dbtypes: Data Types
        --engine: SLQAlchemy engine object based on a URL. These URLs follow RFC-1738, and usually can include username, password, hostname, database name
    """
    logger = log_file(config_folder)
    file_name = os.path.split(file)[1]
    print("Writing: "+ file_name + " into a table: " + dbschema + "." + tbl_name)
    logging.info("Writing: "+ file_name + " into a table: " + dbschema + "." + tbl_name)
    map_data = gpd.GeoDataFrame.from_file(file)
    map_data["geometry"] = [MultiPolygon([feature]) if type(feature) == Polygon \
    else feature for feature in map_data["geometry"]]                   # Convert to Multipolygon to avoid the Shapefiles mix POLYGONs and MULTIPOLYGON
    spatial_ref = map_data.crs.srs.split(':')[-1]                       # Read the spatial reference of shp
    map_data['geom'] = map_data['geometry']
    map_data.drop('geometry', 1, inplace=True)
    map_data = map_data.set_geometry('geom')
    # The to_sql() method of geopandas inherits from pandas and writes the data in GeoDataFrame to the database
    map_data.to_postgis(
        name=tbl_name,
        schema=dbschema,
        index=False, 
        con=engine,
        if_exists='append',                                             # If the table exists, append to the original table
        chunksize=10000,                                                # Set the storage size once to prevent the data from being too large
        dtype= dbtypes
    )
    return engine


def shp2pgsql_batch(dir_name, username, password,
                    host, port, dbschema, dbname,
                    year, countries_upload, carto_upload):
    """Create batch task to store into a PostGIS Database all ESRI Shapefiles ZIPs from a directory
    
    Required Parameters:
        --dir_name: Path to directory
        --username: Database user from the YAML
        --password: Database user password from the YAML
        --host: Database host from the YAML
        --port: Database port from the YAML
        --dbschema: Database schema from the YAML
        --dbname: Database name from the YAML
        --year: Year of the data
        --countries_upload: Countries to upload
        --carto_upload: Cartography types
    """
    # Logginfo
    logger = log_file(config_folder)
    start = datetime.now()
    os.chdir(dir_name)                                                  
    # Change the current working directory to the specified path

    # Create SQL engine and iterate over subfolder files  ##if not insp.has_table(tbl_name, schema=dbschema):
    engine = sqlalchemy.create_engine('postgresql://' + username + ':' + password + '@'+ host + ':' + port + '/' + dbname)

    # Check Country
    if 'ES' in dir_name and countries_upload["ES"] == True:
        print("ES Dataset - Starts batch task: " + str(Path((dir_name))))
        logging.info("ES Dataset - Starts batch task: " + str(Path((dir_name))))
        file_list = os.listdir(dir_name)
        for file in file_list:
            if file.split('.')[-1] == 'zip' and ('rec') in file and carto_upload["LPIS"] == True:
                file = os.path.abspath(file)
                tbl_name = 'es_' + year + '_lpis_' + Path(str(dir_name)).stem
                try:
                    drop_table(tbl_name, engine, dbschema)
                except:
                    None
                dbtypes={                                                   
                    # Explicit dataTypes for specific attributes of the SHP
                    'pend_media': sqlalchemy.types.FLOAT(),
                    'coef_rega': sqlalchemy.types.FLOAT(),
                    'uso_sigpac': sqlalchemy.types.VARCHAR(length=2),
                    'incidencia': sqlalchemy.types.VARCHAR(length=50),
                    'region': sqlalchemy.types.VARCHAR(length=4),
                    'grp_cult': sqlalchemy.types.VARCHAR(length=3)
                    }
                shp2pgsql(file, tbl_name, dbschema, dbtypes, engine)

            elif file.split('.')[-1] == 'zip' and ('ld') in file and carto_upload["GSAA"] == True:
                file = os.path.abspath(file)
                tbl_name = 'es_' + year + '_gsaa_' + Path(str(dir_name)).stem
                try:
                    drop_table(tbl_name, engine, dbschema)
                except:
                    None
                dbtypes={                                                  
                    # Explicit dataTypes for specific attributes of the SHP
                    'exp_num': sqlalchemy.types.VARCHAR(length=30),
                    'exp_ano': sqlalchemy.types.VARCHAR(length=4),
                    'exp_ca': sqlalchemy.types.VARCHAR(length=2),
                    'exp_prov': sqlalchemy.types.VARCHAR(length=2),
                    'ld_recinto': sqlalchemy.types.FLOAT(),
                    'par_produc': sqlalchemy.types.INTEGER(),
                    'par_supcul': sqlalchemy.types.FLOAT(),
                    'par_sisexp': sqlalchemy.types.VARCHAR(length=1),
                    'par_sie': sqlalchemy.types.VARCHAR(length=1),
                    'par_ayusol': sqlalchemy.types.VARCHAR(length=254),
                    'pdr_rec': sqlalchemy.types.VARCHAR(length=20)
                    }
                shp2pgsql(file, tbl_name, dbschema, dbtypes, engine)
        
        # Outputinfo
        end = datetime.now()
        diff =  end - start
        logging.info("Finish: " +  dbschema + "." + tbl_name + " with province folder: " + Path(str(dir_name)).stem + " | Time elapsed: " + str(diff))
        
        return None
    
    elif 'LT' in dir_name and countries_upload["LT"] == True:
        print("LT Dataset - Starts batch task: " + str(Path((dir_name))))
        logging.info("LT Dataset - Starts batch task: " + str(Path((dir_name))))
        file_list = os.listdir(dir_name)
        for file in file_list:
            if file.split('.')[-1] == 'shp' and ('LPIS_LC') in file and carto_upload["LPIS"] == True:
                file = os.path.abspath(file)
                tbl_name = 'lt_' + year + '_lpis_lc'                                     # National layer
                try:
                    drop_table(tbl_name, engine, dbschema)
                except:
                    None
                dbtypes={                                                   
                    # Explicit dataTypes for specific attributes of the SHP
                    'OBJECTID': sqlalchemy.types.BigInteger(),
                    'BLOKAS_ID': sqlalchemy.types.VARCHAR(length=11),
                    'GKODAS': sqlalchemy.types.VARCHAR(length=6),
                    'AZ_ID': sqlalchemy.types.VARCHAR(length=50),
                    }
                shp2pgsql(file, tbl_name, dbschema, dbtypes, engine)

            elif file.split('.')[-1] == 'shp' and ('LPIS_RP') in file and carto_upload["LPIS"] == True:
                file = os.path.abspath(file)
                tbl_name = 'lt_' + year + '_lpis_rp'                                     # National layer
                try:
                    drop_table(tbl_name, engine, dbschema)
                except:
                    None
                dbtypes={                                                   
                    # Explicit dataTypes for specific attributes of the SHP
                    'METAI': sqlalchemy.types.REAL(),
                    'NUMERIS': sqlalchemy.types.VARCHAR(length=18),
                    'DATA': sqlalchemy.types.Date(),
                    'GEO_KODAS': sqlalchemy.types.VARCHAR(length=50),
                    }
                shp2pgsql(file, tbl_name, dbschema, dbtypes, engine)

            # LT GSAA
            elif file.split('.')[-1] == 'shp' and ('GSAA_LD') in file and carto_upload["GSAA"] == True:
                file = os.path.abspath(file)
                tbl_name = 'lt_' + year + '_gsaa'                                   # National layer
                try:
                    drop_table(tbl_name, engine, dbschema)
                except:
                    None
                dbtypes={                                                   # Explicit dataTypes for specific attributes of the SHP
                    'Unique_ID': sqlalchemy.types.VARCHAR(254),
                    'Agricultur': sqlalchemy.types.VARCHAR(254),
                    'LandUseTyp': sqlalchemy.types.VARCHAR(254),
                    'LandUseT_1': sqlalchemy.types.VARCHAR(254),
                    }
                shp2pgsql(file, tbl_name, dbschema, dbtypes, engine)


    elif 'IT' in dir_name and countries_upload["IT"] == True:
        print("IT Dataset - Starts batch task: " + str(Path((dir_name))))
        logging.info("IT Dataset - Starts batch task: " + str(Path((dir_name))))
        file_list = os.listdir(dir_name)
        for file in file_list:
            # IT LPIS
            if file.split('.')[-1] == 'shp' and ('LPIS') in file and carto_upload["LPIS"] == True:
                file = os.path.abspath(file)
                suffix = str(Path(str(file)).stem[-2:])
                tbl_name = 'it_' + year + '_lpis_' + suffix.lower()                                     # National layer
                try:
                    drop_table(tbl_name, engine, dbschema)
                except:
                    None
                dbtypes={                                                   
                    # Explicit dataTypes for specific attributes of the SHP
                    'row_id': sqlalchemy.types.BigInteger(),
                    'lpis_id': sqlalchemy.types.VARCHAR(length=18),
                    'codi_vari': sqlalchemy.types.INTEGER(),
                    'desc_vari': sqlalchemy.types.VARCHAR(length=80),
                    }
                print(file)
                shp2pgsql(file, tbl_name, dbschema, dbtypes, engine)
            
            # IT GSAA
            elif file.split('.')[-1] == 'shp' and ('GSAA') in file and carto_upload["GSAA"] == True:
                file = os.path.abspath(file)
                suffix = str(Path(str(file)).stem[-2:])
                tbl_name = 'it_' + year + '_gsaa_' + suffix.lower()                                      # National layer
                try:
                    drop_table(tbl_name, engine, dbschema)
                except:
                    None
                dbtypes={                                                   
                    # Explicit dataTypes for specific attributes of the SHP
                    'row_id': sqlalchemy.types.BigInteger(),
                    'lpis_id': sqlalchemy.types.VARCHAR(length=18),
                    'id_atto': sqlalchemy.types.BigInteger(),
                    'codi_inte': sqlalchemy.types.VARCHAR(length=6),
                    'codi_occu': sqlalchemy.types.VARCHAR(length=4),
                    'supe_ammi': sqlalchemy.types.BigInteger()
                    }
                print(file)
                shp2pgsql(file, tbl_name, dbschema, dbtypes, engine)

        # Outputinfo
        end = datetime.now()
        diff =  end - start
        logging.info("Finish Italian tables in schema: " +  dbschema  + " with layer folder: " + Path(str(dir_name)).stem + " | Time elapsed: " + str(diff))
        
        return None


# Postgres functions
def create_es_tables(conn, year):
    """Create the spanish tables, before adding the spatial data
    
    Required Parameters:
        --conn: Connection details
        --year: Year of the data
    """
    rv = True
    cur = conn.cursor()
    conn.set_client_encoding('UTF8')
    cur.execute(f'DROP TABLE IF EXISTS es_data.es_{year}_gsaa;')
    cur.execute(f'DROP TABLE IF EXISTS es_data.es_{year}_lpis;')
    cur.execute(f"CREATE TABLE IF NOT EXISTS es_data.es_{year}_gsaa (dn_oid int8 NULL, provincia int8 NULL, municipio int8 NULL, agregado int8 NULL, zona int8 NULL, poligono int8 NULL, parcela int8 NULL, recinto int8 NULL, dn_surface float8 NULL, dn_perim float8 NULL, exp_ano varchar(4) NULL, exp_ca varchar(2) NULL, exp_prov varchar(2) NULL, exp_num varchar(30) NULL, ld_recinto float8 NULL, par_produc int4 NULL, par_sisexp varchar(1) NULL, par_supcul float8 NULL, par_sie varchar(1) NULL, par_ayusol varchar(254) NULL, pdr_rec varchar(20) NULL, geom geometry(multipolygon, 4326) NULL);")
    cur.execute(f"CREATE TABLE IF NOT EXISTS es_data.es_{year}_lpis (dn_oid int8 NULL, provincia int8 NULL, municipio int8 NULL, agregado int8 NULL, zona int8 NULL, poligono int8 NULL, parcela int8 NULL, recinto int8 NULL, dn_surface float8 NULL, dn_perim float8 NULL, pend_media float8 NULL, coef_admis float8 NULL, coef_rega float8 NULL, uso_sigpac varchar(2) NULL, incidencia varchar(50) NULL, region varchar(4) NULL, grp_cult varchar(3) NULL, geom geometry(multipolygon, 4326) NULL);")   
    conn.commit()
    return(rv)

def create_it_tables(conn, year):
    """Create the italian tables, before adding the spatial data
    
    Required Parameters:
        --conn: Connection details
        --year: Year of the data
    """
    rv = True
    cur = conn.cursor()
    conn.set_client_encoding('UTF8')
    cur.execute(f'DROP TABLE IF EXISTS it_data.it_{year}_gsaa;')
    cur.execute(f'DROP TABLE IF EXISTS it_data.it_{year}_lpis;')
    cur.execute(f"CREATE TABLE IF NOT EXISTS it_data.it_{year}_lpis (row_id bigint NULL, lpis_id varchar(18) NULL, nuts_3_cod varchar NULL, lau_code varchar NULL, lau_name varchar NULL, codi_vari int8 NULL, desc_vari varchar(80) NULL, shape_leng double precision NULL, shape_area double precision NULL, mea_area double precision NULL, geom geometry(multipolygon, 4326) NULL);")
    cur.execute(f"CREATE TABLE IF NOT EXISTS it_data.it_{year}_gsaa (row_id bigint NULL, lpis_id varchar(18) NULL, nuts_3_cod varchar NULL, lau_code varchar NULL, lau_name varchar NULL, id_atto bigint NULL, codi_inte varchar(6) NULL, codi_occu varchar(4) NULL, supe_ammi bigint NULL, shape_leng double precision NULL, shape_area double precision NULL, geom geometry(multipolygon, 4326) NULL);") 
    conn.commit()
    return(rv)


def create_schema(conn, country):
    """Create the national schema in the database, before adding the spatial data
    
    Required Parameters:
        --conn: Connection details
        --country: ISO 3166-1 alpha-2 code (https://www.iso.org/obp/ui/#search)
    """
    rv = True
    cur = conn.cursor()
    conn.set_client_encoding('UTF8') 
    print("CREATE SCHEMA IF NOT EXISTS " + country + "_data AUTHORIZATION iacsz")
    cur.execute("CREATE SCHEMA IF NOT EXISTS " + country + "_data AUTHORIZATION iacsz")
    cur.execute("GRANT ALL ON SCHEMA " + country + "_data TO iacsz")
    conn.commit()
    return(rv)


# Delete tables before create function
def drop_table(tbl_name, engine, dbschema):
    print('Check if ' + tbl_name + ' exists')
    base = declarative_base()
    metadata = MetaData(schema=dbschema)
    metadata.reflect(bind=engine)
    table = metadata.tables[dbschema + '.' + tbl_name]
    if table is not None:
        logging.info(f'Deleting {tbl_name} table')
        print('Deleting ' + tbl_name + ' table')
        base.metadata.drop_all(engine, [table], checkfirst=True)

def db_getConnection(host, port, username, password, dbname):
  conn = psycopg2.connect(host=host, port=port, user=username, password=password, dbname=dbname)
  return(conn)

# Perform task plan
if __name__ == '__main__':
    config_folder = parser()
    shp_folder, processes, host, port, username, password, dbschema, dbname, countries_upload, carto_upload, db_updates = parameters(config_folder)
    conn = db_getConnection(host, port, username, password, dbname)
    year_list = glob.glob(shp_folder + "/*/")
    year_list_str = [Path(str(elem)).stem for elem in year_list]
    countries_list = []     
    logging.info("--------INPUT SUMMARY--------")
    logging.info("Number of processes: " + str(processes))   
    for year in year_list_str:
        countries_list = glob.glob(shp_folder + "/" + year + "/*/")
        while int(year) not in range(1900, 3000):
            year = input("The data folder must be the Year of the data. Enter it: ")
            logging.info("The data folder was to be a year, it has been entered manually:  " + str(year))

        # List to string for logging
        countries_list_str = ', '.join([Path(str(elem)).stem for elem in countries_list])
        carto_upload_str = ', '.join([Path(str(elem)).stem for elem in carto_upload])
        logging.info("Input folder: " + str(Path(shp_folder))) 
        logging.info("Year: " + str(year)) 
        logging.info("Countries avalaibles: " + countries_list_str) 
        logging.info("Carto avalaible: " + carto_upload_str)       

        for cntr_folder in countries_list:
            cntr = Path(str(cntr_folder)).stem.lower()
            print("Country: " + cntr)
            # Spanish datasets
            if 'ES' == cntr.upper():
                country_elements = glob.glob(cntr_folder + "/*/")
                logging.info(cntr_folder[-3:] + " number of elements: " + str((len(country_elements))))    

            # Other national datasets
            else:
                create_schema(conn, cntr)
                country_elements = glob.glob(cntr_folder + "/*/")
                logging.info(cntr_folder[-3:] + " number of elements: " + str((len(country_elements))))

            logging.info("--------JOBS SUMMARY--------")
            Parallel(n_jobs=processes)(delayed(shp2pgsql_batch)(dir_name=i, username=username, password=password, host=host, port=port, dbschema=dbschema, dbname=dbname, year=year, countries_upload=countries_upload, carto_upload=carto_upload) for i in country_elements)


