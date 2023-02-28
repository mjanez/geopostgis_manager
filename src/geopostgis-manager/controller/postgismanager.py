#!/usr/bin/env python3
## Coding: UTF-8
## Author: mjanez@tragsa.es
## Institution: -
## Project: -
# inbuilt libraries
import logging
from typing import Optional

# custom functions
from model.Db import get_query, get_connection

# third-party libraries
import geopandas as gpd
import shapely
from shapely.geometry.polygon import Polygon
from shapely.geometry.multipolygon import MultiPolygon



log_module = f"[{__name__}]"

def shp_to_postgis(dataset, db_engine):
    """
    Store into a PostGIS Database the ESRI Shapefiles from a dataset object info.

    Parameters
    ----------
        - dataset: Dataset object to upload into PostGIS.
        - db_engine: SQLAlchemy database engine.

    Return
    ----------
    Dataset object
    """

    try:
        gdf = gpd.GeoDataFrame(gpd.read_file(dataset.file_path)).rename_geometry('geom')

        # Convert to Multipolygon to avoid the Shapefiles mix POLYGONs and MULTIPOLYGON
        gdf["geom"] = [MultiPolygon([feature]) if type(feature) == Polygon else feature for feature in gdf["geom"]]

        # Store native SRID
        dataset.set_file_srid(gdf.crs.to_epsg())

        # Column lowercase
        gdf.columns = map(str.lower, gdf.columns)

        gdf.to_postgis(
            name=dataset.table,
            schema=dataset.schema,
            index=False, 
            con=db_engine,
            if_exists='replace',              
            chunksize=10000,                                                # Set the storage size once to prevent the data from being too large
        )

        logging.info(log_module + ":" + "Write: "+ dataset.identifier + " into a table: " + dataset.schema + "." + dataset.table)
        gdf = gpd.GeoDataFrame(gpd.read_file(dataset.file_path))
        dataset.set_status('db_uploaded')
        dataset.set_status_info('Upload to: ' + dataset.schema + "." + dataset.table)
        
    except:
        logging.error(log_module + ":" + "The dataset: " + dataset.identifier + " has no path, it will not be loaded.")
        dataset.set_status('error')
        dataset.set_status_info('Error reading the ESRI Shapefile')

    return dataset

def update_srid(dataset, db_params, new_srid:Optional[int] = 3857, geom_col:Optional[str] = 'geom'):
    """
    Update SRID of the spatial table. Default EPSG:3857.

    Parameters
    ----------
        - dataset: Dataset object to upload into PostGIS.
        - db_params: Database connection details.
        - new_srid: Spatial reference identifier (SRID), an EPSG Code (https://spatialreference.org/ref/epsg/).
        - geom_col: Name of the geometry field.


    Return
    ----------
    Dataset object
    """

    try:
        conn = get_connection(db_params)
        cur = conn.cursor()
        query = "SELECT UpdateGeometrySRID('{schema}', '{table}', '{geom}', {new_srid})".format(
                            schema=dataset.schema,
                            table=dataset.table,
                            geom=geom_col,
                            new_srid=new_srid
                        )
        cur.execute(query)
        logging.info(f"{log_module}:Update table: '{dataset.schema }.{dataset.table}' with SRID: EPSG:{new_srid}")
        dataset.set_status_info(f"Update table: '{dataset.schema }.{dataset.table}' with SRID: EPSG:{new_srid}")

        dataset.set_file_srid(new_srid)
        conn.commit()
        conn.close()
        conn = None


    except:
        logging.error(f"{log_module}:The dataset: '{dataset.identifier}' fail when transform to EPSG: {new_srid}")
        dataset.set_status('error')
        dataset.set_status_info(f"Error transforming: '{dataset.schema}.{dataset.table}' when transform to EPSG: {new_srid}")

    return dataset

def get_srid(dataset, db_params, geom_col:Optional[str] = 'geom'):
    """
    Returns the integer SRID of the specified geometry column by searching through the PostGIS DB.

    Parameters
    ----------
    dataset : object
    db_params : object
    geom_col : str, optional

    Returns
    -------
    dataset : object
    """
    try:
        conn = get_connection(db_params)
        query = "SELECT Find_SRID('{schema}', '{table}', '{geom}')".format(
                    schema=dataset.schema,
                    table=dataset.table,
                    geom=geom_col
                )
        cur = conn.cursor()
        cur.execute(query)

        if isinstance(cur.fetchone()[0], int):
            srid = cur.fetchone()[0]
        else:
            srid = 0
        dataset.set_file_srid(srid)
        conn.commit()
        conn.close()
        conn = None

    except:
        logging.error(f"{log_module}:The dataset: '{dataset.identifier}' fail when check SRID")
        dataset.set_status('error')
        dataset.set_status_info(f"Error checking: '{dataset.schema}.{dataset.table}' SRID")

    return dataset

def check_table_exists(dataset, db_params):
    """
    Chek if a dataset table exists in a database.

    Parameters
    ----------
    dataset : object
    db_params : object

    Returns
    -------
    dataset : object
    """
    try:
        conn = get_connection(db_params)
        query = "SELECT EXISTS ( SELECT FROM pg_tables WHERE schemaname='{schema}' AND tablename='{table}');".format(
                    schema=dataset.schema,
                    table=dataset.table
                )
        cur = conn.cursor()
        cur.execute(query)

        if cur.fetchone()[0] == True:
             dataset.set_status('db_uploaded')
        conn.commit()
        conn.close()
        conn = None

    except Exception as e:
        logging.error(f"{log_module}:The dataset: '{dataset.identifier}' does not exists in the dbname: {db_params.dbname} and schema: {dataset.schema}")
        dataset.set_status('error')
        dataset.set_status_info(f"The dataset: '{dataset.identifier}' does not exists in the dbname: {db_params.dbname} and schema: {dataset.schema}")
        raise Exception(e)

    return dataset
    

def create_index(dataset, db_params):
    """
    Update/Create Geometry Index and clustering table

    Parameters
    ----------
        - dataset: Dataset object to upload into PostGIS
        - db_params: Database connection details

    Return
    ----------
    Dataset object
    """

    try:
        conn = get_connection(db_params)
        cur = conn.cursor()
        query_geom_idx = 'CREATE INDEX gidx_{table} ON {schema}."{table}" USING GIST(geom)'.format(
                            schema=dataset.schema,
                            table=dataset.table,
                        )
        query_cluster = 'CLUSTER {schema}."{table}" USING gidx_{table}'.format(
                            schema=dataset.schema,
                            table=dataset.table,
                        )
        cur.execute(query_geom_idx)
        logging.info(log_module + ":" + "Create geom index of table: " + dataset.schema + "." + dataset.table)
        dataset.set_status_info('Create geom index of table: ' + dataset.schema + "." + dataset.table)

        cur.execute(query_cluster)
        logging.info(log_module + ":" + "Clustering table: " + dataset.schema + "." + dataset.table)
        dataset.set_status_info('Clustering table: ' + dataset.schema + "." + dataset.table)

        conn.commit()
        conn.close()
        conn = None

    except:
        logging.error(log_module + ":" + "The dataset: " + dataset.identifier + " fail when cluster the geom index")
        dataset.set_status('error')
        dataset.set_status_info('Error clustering: ' + dataset.schema + "." + dataset.table)

    return dataset


