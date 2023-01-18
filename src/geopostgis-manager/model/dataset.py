import urllib.parse
import json
import unicodedata
import hashlib

class Dataset:
    # Dataset contents
    def __init__(self, name, identifier, schema):
        self.name = name
        self.identifier = identifier
        self.path = None
        self.schema = schema
        self.table = None
        self.sld_path = None
        self.description = None
        self.status = None
        self.status_info = None
        self.metadata_url = None
        self.ogc_workspace = None
        self.creator = None
        self.file_format = None

    def set_name(self, name):
        self.name = name

    def set_identifier(self, identifier):
        self.identifier = identifier

    def set_path(self, path):
        self.path = path

    def set_schema(self, schema):
        self.schema = schema

    def set_description(self, description):
        self.description = description

    def set_sld_path(self, sld_path):
        self.sld_path = sld_path
    
    def set_status(self, status):
        status_codelist = ['to load', 'done', 'review', 'ignore', "failed"]

        if status.lower() not in status_codelist:
            self.status = 'unknown'
        else:
            self.status = status.lower()

    def set_status_info(self, status_info):
        self.status_info = status_info     

    def set_metadata_url(self, metadata_url):
        self.metadata_url = metadata_url

    def set_ogc_workspace(self, ogc_workspace):
        self.ogc_workspace = ogc_workspace
    
    def set_creator(self, creator):
        self.creator = creator
    
    def set_file_format(self, file_format):
        self.file_format = file_format

    def get_table_name(self):
        # the name of a Postgis dataset, must be between 2 and 63 characters long and contain only lowercase
        # alphanumeric characters, - and _, e.g. 'warandpeace'
        normal = str(unicodedata.normalize('NFKD', self.identifier).encode('ASCII', 'ignore'))[2:-1]
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
        return {'name': self.name,
                'identifier': self.identifier,
                'path': self.path,
                'schema': self.schema,
                'table': self.table,
                'sld_path': self.sld_path,
                'description': self.description,
                'status': self.status,
                'metadata_url': self.metadata_url,
                'ogc_workspace': self.ogc_workspace,
                'creator': self.creator,
                'file_format': self.file_format,      
                }

    def generate_data(self):
        dataset_dict = self.dataset_dict()
        # Use the json module to dump the dictionary to a string for posting.
        quoted_data = urllib.parse.quote(json.dumps(dataset_dict))
        byte_data = quoted_data.encode('utf-8')
        return byte_data

    def check_dataset_file():
        print("Testing")