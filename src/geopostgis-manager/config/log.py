#!/usr/bin/env python3
## Coding: UTF-8
## Author: mjanez@tragsa.es
## Institution: -
## Project: -
# inbuilt libraries
import logging
import os

# Logging
def log_file(log_folder):
    '''
    Starts the logger --log_folder parameter entered
    
    Parameters
    ----------
    - log_folder: Folder where log is stored 

    Return
    ----------
    Logger object
    '''
    logger = logging.getLogger()
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)

    if not os.path.exists(log_folder):
        os.makedirs(log_folder)

    logging.basicConfig(
                        handlers=[logging.FileHandler(filename=log_folder + "/geopostgis-manager.log", encoding='utf-8', mode='a+')],
                        format="%(asctime)s %(levelname)s::%(message)s",
                        datefmt="%Y-%m-%d %H:%M:%S", 
                        level=logging.INFO
                        )
    return logger