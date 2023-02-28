#!/usr/bin/env python3
## Coding: UTF-8
## Author: mjanez@tragsa.es
## Institution: -
## Project: -
# inbuilt libraries
import yaml
from functools import reduce
import os
from tempfile import mkstemp
from typing import Dict
from zipfile import ZipFile

# Applies to Python-3 Standard Library
class Struct(object):
    def __init__(self, data):
        for name, value in data.items():
            setattr(self, name, self._wrap(value))

    def _wrap(self, value):
        if isinstance(value, (tuple, list, set, frozenset)): 
            return type(value)([self._wrap(v) for v in value])
        else:
            return Struct(value) if isinstance(value, dict) else value

# Configuration data
def config_get_parameters():
    """
    Read the config_file var and return the required parameters from the YAML 
    
    Parameters
    ----------
    - config_file: config.yml

    Return
    ----------
    Parameters from config.yml
    """
    # Config default path
    config_file = os.path.abspath(__file__ + "/../../../../config.yml")

    # Import config_shp.yml parameters
    def get_config_valor(key, cfg):
        """
        Read the YAML 
    
        Parameters
        - key: Key
        - cfg: Config element

        Return
        ----------
        Config element
        """
        return reduce(lambda c, k: c[k], key.split('.'), cfg)

    def config_to_object(config_element):
        object_list = []
        if type(config_element) == list:
            for element in config_element:
                object_list.append(Struct(element))
        else: 
            object_list = Struct(config_element)
            
        return object_list

    with open(config_file, encoding="utf-8") as stream:
        config = yaml.safe_load(stream)
        geopostgis_bundles = config_to_object(get_config_valor('geopostgis_bundles', config))
        datasets_doc = config_to_object(get_config_valor('datasets_doc', config))
        default_config = config_to_object(get_config_valor('default', config))

    return geopostgis_bundles, datasets_doc, default_config

# Prepare ZIPs
def prepare_zip_file(name: str, data: Dict) -> str:
    """Creates a zip file from

    GeoServer's REST API uses ZIP archives as containers for file formats such
    as Shapefile and WorldImage which include several 'boxcar' files alongside
    the main data.  In such archives, GeoServer assumes that all of the relevant
    files will have the same base name and appropriate extensions, and live in
    the root of the ZIP archive.  This method produces a zip file that matches
    these expectations, based on a basename, and a dict of extensions to paths or
    file-like objects. The client code is responsible for deleting the zip
    archive when it's done.

    Parameters
    ----------
    name : name of files
    data : dict

    Returns:
    str
    """
    fd, path = mkstemp()
    zip_file = ZipFile(path, "w", allowZip64=True)
    print(fd, path, zip_file, data)
    for ext, stream in data.items():
        fname = "{}.{}".format(name, ext)
        if isinstance(stream, str):
            zip_file.write(stream, fname)
        else:
            zip_file.writestr(fname, stream.read())
    zip_file.close()
    os.close(fd)
    return path
