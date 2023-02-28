<h1 align="center">geopostgis-manager</h1>
<p align="center">
<a href="https://github.com/mjanez/geopostgis_manager-ckan"><img src="https://camo.githubusercontent.com/e7482049f34711b5200ecc70004690f13f2a0fae49a9f48e96dc2cf7dbac483b/68747470733a2f2f696d672e736869656c64732e696f2f62616467652f446f636b6572253230434b414e2d322e392e352d627269676874677265656e" alt="CKAN Versions"></a><a href="https://opensource.org/licenses/MIT"><img src="https://img.shields.io/badge/License-MIT-yellow.svg" alt="License: MIT"></a>


<p align="center">
    <a href="#configuration">Configuration</a> •
    <a href="#getting-started">Getting started</a> •
    <a href="#extensions">Extensions</a> •
    <a href="#license">License</a>
</p>

This file explains how to create Geoserver layers ([FeatureTypes](https://docs.geoserver.org/2.22.x/en/user/rest/api/featuretypes.html) & [Coverages](https://docs.geoserver.org/2.22.x/en/user/rest/api/coverages.html)) for publishing map services and their metadata according to OGC standards using a library for spatial data management in Geoserver and [`sqlalchemy`](https://www.sqlalchemy.org/)/[`geoalchemy2`](https://geoalchemy-2.readthedocs.io/en/latest/) libraries for loading into PostGIS database.

**Requirements**:
* Linux/Windows 64 bit system
* Map server [Geoserver](https://docs.geoserver.org/) available.
* [PostgreSQL-PostGIS](https://postgis.net/) database available. [**Optional**]
* The code compiles with [Python 3](https://www.python.org/downloads/). The required libraries can be found in `requirements.txt`.

>**Note**:<br>
> Tested successfully with [Python 3.7.9](https://www.python.org/downloads/release/python-379/)


## Configuration
The necessary steps to configure the environment and install the libraries are as follows. First create the virtual environment ([`venv`](https://docs.python.org/3/library/venv.html)) where it will run (or replace `whoami` with the desired user name if you create for example one for `ckan`):

```bash
# Linux
cd /my/path
git clone https://github.com/mjanez/geopostgis_manager.git 
cd /geopostgis_manager
sudo chown `whoami` /geopostgis_manager
python3 -m venv .env    # sudo apt-get install python3-venv
. .env/bin/activate
python3 -m pip install -r requirements.txt

# Windows (CMD)
cd /my/path
git clone https://github.com/mjanez/geopostgis_manager.git 
cd /geopostgis_manager/
python -m venv .env
.env/Scripts/activate.bat  # CMD || .env\Scripts\Activate.ps1  # Powershell
pip install  install -r requirements.txt
```

>**Note**:<br>
> You can use the following code to generate a `requirements.txt`
> 
>```python
># Into virtualenv
>(.env) cd /path/to/project
>(.env) pip3 freeze > requirements.txt
>```


## Getting started
>**Warning**:<br>
> Before running the harvester, located in `/src/geopostgis-manager/run.py`, the parameters of the configuration file [`/config.yml`](/config.yml) must be updated as described in the following section.

**Sample `config.yml`**:
```yaml
geopostgis_bundles:
  # Bundle ID [Mandatory]
  - bundle_id: 'test_bundle'
    # DB connection parameters [Mandatory]
    db_endpoint: PostGIS Test
    db_type: postgres
    db_host: localhost
    db_port: '5432'
    db_dbname: testdb
    db_username: user
    db_password: password
    db_active: True
    
    # Geoserver Parameters [Mandatory]
    geo_endpoint: Localhost Server Test
    geo_datastore: testdb
    geo_url: http://127.0.0.1:8080/geoserver/
    # geo_workspace Overrides ogc_workspace in dataset table. If not put: Null
    geo_workspace: test
    geo_username: admin
    geo_password: password
    geo_srid: 3857
    geo_active: True

# Dataset documentation details by bundle [Mandatory]
datasets_doc:
  # Bundle ID [Mandatory]
  - bundle_id: 'test_bundle'
    db_datasets_mode: False
    db_datasets_doc_table: 'public.datasets'
    datasets_doc_path: '/srv/data/datasets_doc.csv'
    date: 2002-12-12
    # Schema in which the tables with the spatial data will be stored. Default: public
    output_schema: public
    # Field names [Mandatory]
    field_name: titulo
    field_publisher: distribuidor
    field_identifier: nombre_capa
    field_path: ruta_p
    field_srid: srid
    field_carto_type: carto_type
    # Field names [Optional]
    field_sld: sld
    field_metadata_url: url_metadatos
    field_description: resumen
    field_ogc_workspace: workspace_ogc
    field_creator: propietario
    # Loader publisher
    publisher: Tragsatec

# Config  [Mandatory]
default:
  # Execute software with multicore parallel processing
  parallelization: True
  # If needed, proxy socks5/http (http-https)
  proxy_socks5: 'socks5://user:pass@host:port'
  proxy_http: 'http://user:pass@host:port'
  # Loading modes
  load_to_db: False
  load_to_geoserver: True
```

### `geopostgis_bundles`
Basic information about the map server (Geoserver) on which the new layers are to be created, and the DB that will be used to store them, in the case of vector data (`FeatureTypes`). For raster data (`Coverages`), these can be created as layers in Geoserver, but are stored locally in directories on the server.

**Mandatory**

`bundle_id`, *str*: Bundle identifier, one per geoserver-postgis info (ej. `test_bundle`).

* Database:
    * `db_endpoint`, *str*: Descriptive name of the server. (ej. `PostGIS Test`)
    * `db_type`, *str*: Type of the database. Currently supported: `postgres`.
    * `db_host`, *str*: URL of the Database (ej. `localhost`)
    * `db_port`, *str*: Port of the Database (ej. `5432`)
    * `db_dbname`, *str*
    * `db_username`, *str*
    * `db_password`, *str*
    * `geo_active`, *bool*: Whether the endpoint is active for data upload to the endpoint. Value: `True` or `False`.

* Geoserver:
    * `geo_endpoint`, *str*: Descriptive name of the server. (ej. `Geoserver IEPNB`)
    * `geo_datastore`, *str*: Name of the Geoserver datastore ([`datastore`](https://docs.geoserver.org/latest/en/user/data/app-schema/data-stores.html)) from which the elements will be requested.
    * `geo_url`, *url*: URL del Geoserver (ej. `http://127.0.0.1:8080/geoserver/`).
    * `geo_workspace`, *str*: Name of the Geoserver workspace ([`workspace`](https://docs.geoserver.org/stable/en/user/data/webadmin/workspaces.html)) to which the elements will be uploaded.
    * `geo_username`, *str*: Geoserver admin username.
    * `geo_password`, *str*: Geoserver admin password.
    * `geo_srid`, *int*: Geoserver default [EPSG code](https://spatialreference.org/ref/epsg/).
    * `geo_active`, *bool*: Whether the endpoint is active for data upload to the endpoint. Value: `True` or `False`.

### `datasets_doc`
Parameters needed to define the `database table`/`CSV` containing the basic information about the datasets to be loaded into the DB and/or Geoserver.

**Mandatory**

`bundle_id`, *str*: Bundle identifier, one per geoserver-postgis info (ej. `test_bundle`).

* Database:
    * `db_datasets_doc_mode`, *bool*: Source of the dataset table. `False`: Physical file in a directory (`CSV`), `True`: Database table.
    * `db_datasets_doc_table`, *str*: `schema`.`table` containing the dataset information. (ej. `public.datasets`)
    * `datasets_doc_path`, *str*: Filepath of the `CSV` datasets documentation.
    * `date`, *date*: Date of the documentation.
    * `output_schema`, *str*: Base schema where the files loaded into the database will be stored.
    * `field_name`, *str*: Dataset field name of the dataset name.
    * `field_publisher`, *str*: Dataset field name of the dataset publisher.
    * `field_identifier`, *str*: Dataset field name of the dataset identifier.
    * `field_path`, *str*: Dataset field name of the dataset filepath.
    * `field_srid`, *str*: Dataset field name of the dataset SRID.
    * `field_carto_type`, *str*: Dataset field name of the dataset carto type. `vector` or `raster`.
    * `field_sld`, *str*: Dataset field name of the dataset sld filepath.
    * `field_metadata_url`, *str*: Dataset field name of the dataset metadata url.
    * `field_description`, *str*: Dataset field name of the dataset description.
    * `field_ogc_workspace`, *str*: Dataset field name of the dataset Geoserver workspace.
    * `field_creator`, *str*: Dataset field name of the dataset creator.
    * `publisher`, *str*: Name of the Datasets publisher.

### `default`
* `parallelization`, *bool*: Execute software with multicore parallel processing. Value: `True` or `False`
* `proxy_socks5`, *str*: SOCKS5 proxy URL loads only. `socks5://user:pass@host:port`
* `proxy_http`, *str*: HTTP proxy URL loads only. `http://user:pass@host:port`
* `load_to_db`, *bool*: Load datasets into the database. Value: `True` or `False`
* `load_to_geoserver`, *bool*: Load datasets into the Geoserver. Value: `True` or `False`

## Execution
Example of CKAN harvester execution:
```shell
# Linux
. /my/path/geopostgis_manager/.env/bin/activate
export PYTHONPATH=/my/path/geopostgis_manager/src
python3 /my/path/geopostgis_manager/src/geopostgis-manager/run.py

# Windows
.env\Scripts\activate.bat  # CMD || env\Scripts\Activate.ps1  # Powershell
PYTHONHOME=/my/path/geopostgis_manager/src
python /my/path/geopostgis_manager/src/geopostgis-manager/run.py
```

## Debug
1. Generate the `virtualenv` and select the Python interpreter from its path with `CTRL+Shift+P`>`Python: Select interpeter` (`/.env/Scripts/python.exe`).

2. Modify the `debugger` configuration:
**Visual Studio Code debug configuration:**
```json
{
    // Use IntelliSense to learn about possible attributes.
    // Hover to view descriptions of existing attributes.
    // For more information, visit: https://go.microsoft.com/fwlink/?linkid=830387
    "version": "0.2.0",
    "configurations": [
    

        {
            "name": "Python: Current File",
            "type": "python",
            "request": "launch",
            "program": "${file}",
            "console": "integratedTerminal",
            "justMyCode": true,
            "cwd": "${fileDirname}",
            "env": {"PYTHONPATH": "${workspaceFolder}${pathSeparator}${env:PYTHONPATH}"}
        }
    ]
}
```

## License
This material is open and licensed under the MIT License whose full text may be found at:

https://mit-license.org/