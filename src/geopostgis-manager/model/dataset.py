#!/usr/bin/env python3
## Coding: UTF-8
## Author: mjanez@tragsa.es
## Institution: -
## Project: -
# inbuilt libraries
import urllib.parse
import json
import unicodedata
import hashlib
import re
from datetime import datetime


class Dataset:
    """
    Dataset object.

    Attributes:
    name -- Name of the dataset.
    identifier -- Unique identifier of the dataset.
    path -- Path of the dataset file.
    dbname -- Database.
    schema -- DB schema.
    table -- DB table.
    sld_path -- Path of the SLD file.
    description -- Abstract about the dataset.
    status -- Status code.
    status_info -- Status abstract.
    metadata_url -- URL of the metadata element.
    creator -- Name of the creator of the dataset.
    file_format -- Format of the file describe in path.
    srid -- spatial reference identifier (SRID). int
    declared_srid -- Output Geoserver declared spatial reference identifier (SRID). int
    ogc_layer -- Output Standarised Geoserver Layer name. str
    ogc_workspace -- Ouput Geoserver Layer workspace. str
    """
    def __init__(self, name, identifier, schema):
        self.identifier = identifier
        self.name = name
        self.file_path = None
        self.dbname = None
        self.schema = schema
        self.table = None
        self.sld_path = None
        self.description = None
        self.status = None
        self.status_info = None
        self.metadata_url = None
        self.creator = None
        self.file_format = None
        self.file_srid = None
        self.carto_type = None
        self.declared_srid = None
        self.ogc_workspace = None
        self.ogc_layer = None

    def set_name(self, name):
        self.name = name

    def set_identifier(self, identifier):
        self.identifier = identifier

    def set_file_path(self, file_path):
        self.file_path = file_path

    def set_dbname(self, dbname):
        self.dbname = dbname

    def set_schema(self, schema):
        self.schema = schema

    def set_description(self, description):
        self.description = description

    def set_sld_path(self, sld_path):
        self.sld_path = sld_path
    
    def set_status(self, status):
        """
        Parameters
        ----------
        status : str

        Notes
        -----
        db_to-load: default. Dataset ready to be uploaded to DB.
        db_uploaded. Dataset uploaded into DB.
        geo_to-load: Dataset table ready to be published in Geoserver.
        geoserver_uploaded. Dataset FeatureType published in Geoserver.
        review. Dataset pending review for possible errors in documentation.
        ignore. Dataset to be ignored because it is not necessary to be uploaded or published or because of formal errors.
        error. 
        """
        status_codelist = ['db_to-load', 'db_uploaded', 'geo_to-load', 'geoserver_uploaded', 'review', 'ignore', 'error']

        if status.lower() not in status_codelist:
            self.status = 'unknown'
        else:
            self.status = status.lower()

    def set_status_info(self, status_info):
        if self.status_info is not None:
            self.status_info = self.status_info
            self.status_info += "\n" + str(datetime.now()).split(".")[0] + " | " + status_info
        else:
            self.status_info = str(datetime.now()).split(".")[0] + " | " + status_info 

    def set_metadata_url(self, metadata_url):
        self.metadata_url = metadata_url
    
    def set_creator(self, creator):
        self.creator = creator
    
    def set_file_format(self, file_format):
        self.file_format = file_format

    def set_file_srid(self, srid):
        if isinstance(srid, str):
            srid = re.findall('\d+', srid)[0] 
        self.file_srid = int(srid)

    def set_carto_type(self, carto_type):
        self.carto_type = str(carto_type)

    def set_declared_srid(self, declared_srid):
        self.declared_srid = int(declared_srid)

    def set_ogc_workspace(self, ogc_workspace):
        self.ogc_workspace = ogc_workspace

    def set_ogc_layer(self, ogc_layer):
        self.ogc_layer = ogc_layer

    def set_table_name(self, identifier):
        # the name of a Postgis dataset, must be between 2 and 63 characters long and contain only lowercase
        # alphanumeric characters, - and _, e.g. 'warandpeace'
        normal = str(unicodedata.normalize('NFKD', identifier).encode('ASCII', 'ignore'))[2:-1]
        normal = normal.lower()
        table_name = ''
        for c in normal:
            if ('0' <= c <= '9') or ('a' <= c <= 'z') or (c == '_'):
                table_name += c
            else:
                table_name += '_'

        # If name is longer than 60 characters, a hash function is applied
        if len(table_name) >= 60:
            table_name = hashlib.sha1(self.identifier.encode("utf-8")).hexdigest()

        self.table = table_name

    def dataset_dict(self):
        return {'identifier': self.identifier,
                'name': self.name,
                'creator': self.creator,
                'description': self.description,
                'status': self.status,
                'status_info': self.status_info,
                'metadata_url': self.metadata_url,
                'file_carto_type': self.carto_type,
                'file_format': self.file_format,
                'file_srid': self.file_srid,
                'file_path': self.file_path,
                'sld_path': self.sld_path,
                'db_database': self.dbname,
                'db_schema': self.schema,
                'db_table': self.table,
                'ogc_srid': self.declared_srid,
                'ogc_workspace': self.ogc_workspace,
                'ogc_layer': self.ogc_layer        
                }

    def generate_json(self):
        dataset_dict = self.dataset_dict()
        # Use the json module to dump the dictionary to a string for posting.
        quoted_data = urllib.parse.quote(json.dumps(dataset_dict))
        byte_data = quoted_data.encode('utf-8')
        return byte_data

    def check_dataset_file():
        #TODO
        print("Testing")