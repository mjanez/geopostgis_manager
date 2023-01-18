import geopandas as gpd
import logging
from model.db import get_query, get_connection
import shapely
from shapely.geometry.polygon import Polygon
from shapely.geometry.multipolygon import MultiPolygon

log_module = "[" + __name__ + "]"

def shp_to_postgis(dataset, db_engine):
    """
    Store into a PostGIS Database the ESRI Shapefiles from a dataset object info.

    Parameters:
        - dataset -- Dataset object to upload into PostGIS.
        - db_engine -- SQLAlchemy database engine.

    Return:
    Dataset object
    """

    try:
        gdf = gpd.GeoDataFrame(gpd.read_file(dataset.path)).rename_geometry('geom')

        # Convert to Multipolygon to avoid the Shapefiles mix POLYGONs and MULTIPOLYGON
        gdf["geom"] = [MultiPolygon([feature]) if type(feature) == Polygon else feature for feature in gdf["geom"]]

        gdf.to_postgis(
            name=dataset.table,
            schema=dataset.schema,
            index=False, 
            con=db_engine,
            if_exists='replace',              
            chunksize=10000,                                                # Set the storage size once to prevent the data from being too large
        )

        logging.info(log_module + ":" + "Write: "+ dataset.identifier + " into a table: " + dataset.schema + "." + dataset.table)
        gdf = gpd.GeoDataFrame(gpd.read_file(dataset.path))
        dataset.set_status('done')
        dataset.set_status_info('Upload to: ' + dataset.schema + "." + dataset.table)
        
    except:
        logging.error(log_module + ":" + "The dataset: " + dataset.identifier + " has no path, it will not be loaded.")
        dataset.set_status('error')
        dataset.set_status_info('Error reading the ESRI Shapefile')

    return dataset

def update_srid(dataset, db_params, new_srid='3857', geom_col='geom'):
    """
    Update SRID of the spatial table. Default EPSG:3857.

    Parameters:
        - dataset -- Dataset object to upload into PostGIS.
        - db_params -- Database connection details.
        - new_srid -- Spatial reference identifier (SRID), an EPSG Code (https://spatialreference.org/ref/epsg/).
        - geom_col -- Name of the geometry field.


    Return:
    Dataset object
    """

    try:
        conn = get_connection(db_params)
        cur = conn.cursor()
        query = "SELECT UpdateGeometrySRID('{schema}', '{table}', '{geom}', {srid})".format(
                            schema=dataset.schema,
                            table=dataset.table,
                            geom=geom_col,
                            srid=new_srid
                        )
        cur.execute(query)
        logging.info(log_module + ":" + "Update table: " + dataset.schema + "." + dataset.table + " with SRID: " + new_srid)
        dataset.set_status_info('Upload: ' + dataset.schema + "." + dataset.table + " to SRID: " + new_srid)

        conn.commit()
        conn.close()
        conn = None


    except:
        logging.error(log_module + ":" + "The dataset: " + dataset.identifier + " fail when transform to SRID: " + new_srid)
        dataset.set_status('error')
        dataset.set_status_info('Error transforming: ' + dataset.schema + "." + dataset.table + " to SRID: " + new_srid)

    return dataset

def create_index(dataset, db_params):
    """
    Update/Create Geometry Index and clustering table

    Parameters:
        - dataset -- Dataset object to upload into PostGIS
        - db_params -- Database connection details

    Return:
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


