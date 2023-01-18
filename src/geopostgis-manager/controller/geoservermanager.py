from geo.Geoserver import Geoserver
from osgeo import gdal
import glob
import os
"""
geo = Geoserver('http://localhost:8080/geoserver', username='admin', password='geoserver')

rst_files = glob.glob(r'C:\Users\gic\Desktop\etlIa\*.rst')
# geo.create_workspace('geonode')

for rst in rst_files:
    file_name = os.path.basename(file_name)
    src = gdal.Open(rst)
    tiff = r'C:\Users\tek\Desktop\try\{}'.format(file_name)
    gdal.Translate(tiff, src)
    geo.create_coveragestore(lyr_name=file_name, path=tiff, workspace='geonode')    #, overwrite=True
    """

def create_workspaces():
    print("TODO")

def create_datastores():
    print("TODO")

def create_layers(dataset, db_engine):
    """
    Store into a PostGIS Database the ESRI Shapefiles from a dataset object info.

    Parameters:
        - dataset -- Dataset object to upload into PostGIS.
        - db_engine -- SQLAlchemy database engine.

    Return:
    Dataset object
    """
    print("TODO")