#!/usr/bin/env python3
## File: config.py
## Coding: UTF-8
## Author: Manuel Ángel Jáñez García (mjanez@tragsa.es)
## Institution: -
## Project: -
## Goal: The goal of this script is is to provide the connection details to the PostGIS database and Geoserver.
## Parent: ogc_ckan/ckan_config.py
""" Changelog:
    v1.0 - 12 Dec 2022: Create the first version
"""
# Update the version when apply changes
version = "1.0"

##~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~##
##               config.py              ##
##~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~##

# Connection details to the ckan database

## Import libraries   
import yaml
from functools import reduce
import os

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
    
    Parameters:
    - config_file -- config.yml

    Return:
    Parameters from config.yml
    """
    # Config default path
    config_file = os.path.abspath(__file__ + "/../../../../config.yml")

    # Import config_shp.yml parameters
    def get_config_valor(key, cfg):
        """
        Read the YAML 
    
        Parameters
        - key -- Key
        - cfg -- Config element

        Return:
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
        geoserver_servers = config_to_object(get_config_valor('geoserver_servers', config))
        db_servers = config_to_object(get_config_valor('db_servers', config))
        datasets_doc = config_to_object(get_config_valor('datasets_doc', config))
        default_config = config_to_object(get_config_valor('default', config))

    return geoserver_servers, db_servers, datasets_doc, default_config

