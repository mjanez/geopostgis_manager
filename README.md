<h1 align="center">geopostgis-manager</h1>
<p align="center">
<a href="https://github.com/mjanez/geopostgis_manager-ckan"><img src="https://img.shields.io/badge/EIKOS%20CKAN-version%202.9.5-brightgreen" alt="CKAN Versions"></a><a href="https://opensource.org/licenses/MIT"><img src="https://img.shields.io/badge/License-MIT-yellow.svg" alt="License: MIT"></a>


<p align="center">
    <a href="#overview">Overview</a> •
    <a href="#configuration">Configuration</a> •
    <a href="#extensions">Extensions</a> •
•
    <a href="#license">License</a>
</p>

This file explains how to create the Geoserver type server for publishing map services, and their metadata, according to OGC standards by using the [`geoserver-rest`](https://geoserver-rest.readthedocs.io/en/latest/about.html) library for spatial data management in Geoserver and [`sqlalchemy`](https://www.sqlalchemy.org/)/[`geoalchemy2`](https://geoalchemy-2.readthedocs.io/en/latest/) libraries to load into PostGIS database.

**Requirements**:
* Linux/Windows 64 bit system
* Map server [Geoserver](https://docs.geoserver.org/) available.
* [PostgreSQL-PostGIS](https://postgis.net/) database available.
* The code compiles with [Python 3](https://www.python.org/downloads/). The required libraries can be found in `requirements.txt`.


## Configuration
The necessary steps to configure the environment and install the libraries are as follows. First create the `venv` directory where it will run (or replace `whoami` with the desired user name if you create for example one for `ckan`):

```bash
# Linux
cd /my/path
git clone https://github.com/mjanez/geopostgis_manager.git 
cd /geopostgis_manager
sudo chown `whoami` /geopostgis_manager
python3 -m venv .env    # sudo apt-get install python3-venv
. /.env/bin/activate
python3 -m pip install  install -r requirements.txt

# Windows
cd /my/path
git clone https://github.com/mjanez/geopostgis_manager.git 
cd /geopostgis_manager/
python -m venv .env
.env\Scripts\activate.bat  # CMD || env\Scripts\Activate.ps1  # Powershell
pip install  install -r requirements.txt
```

## Requirements
You can use the following code to generate a `requirements.txt`

```python
# Into virtualenv
(.env) cd path/to/project
(.env) pip3 freeze > requirements.txt
```







===


## Launch
Before running the harvester, located in `ckan.ogc_ckan.run.py`, the parameters of the configuration file `config.yml` must be updated as described in [ckan-harvester repository](https://github.com/mjanez/geopostgis_manager/tree/main/ckan-harvester#configuraci%C3%B3n-1)

### `harvest_servers`
Información básica de los servidores que se van a cosechar, pueden ser de dos tipos: `ogc` o `csw`. Se añaden como elementos nuevos a partir de la `url` en el propio fichero de configuración, ejemplo:
```yaml
harvest_servers:
  - url: 'https://geoservicios.iepnb.es/geoserver/ows'
    name: 'Geoserver IEPNB'
    groups: ['geoserver-eikos']
    active: True
    type: 'ogc'
    organization: 'iepnb'
    workspaces: ['ENP','inenp','RN2000'] 
    default_keywords: ['iepnb', 'environment', 'biota']
    inspireid_theme: 'PS'
    inspireid_nutscode: 'ES' 
    inspireid_versionid: ''
    default_provenance: 'Los datos espaciales se han generado a partir del Banco de Datos de la Naturaleza bajo la responsabilidad del Inventario Español del Patrimonio Natural y de la Biodiversidad, en el marco del proyecto EIKOS.'
  - url: 'https://www.idee.es/csw-codsi-idee/srv/spa/csw'
    name: 'CODSI'
    active: True
    type: 'csw'
    organization: 'codsi'
    default_keywords: ['iepnb', 'environment', 'biota']
    inspireid_theme: 'HB'
    constraints: 
      keywords: ['iepnb', 'inventario', 'patrimonio', 'miteco', 'natura']
      mails: ['brfranco@miteco.es', 'buzon-bdatos@miteco.es']
    default_bbox: '{"type": "Polygon", "coordinates": [[[-19.0, 27.0], [4.57, 27.0], [4.57, 44.04], [-19.0, 44.04], [-19.0, 27.0]]]}'
```

**Servidores OGC**
* Obligatorios:
    * `url`: URL pública del endpoint OGC. (ej. `https://geoservicios.iepnb.es/geoserver/ows`)
    * `name`: Nombre informativo del servidor. (ej. `Geoserver IEPNB`)
    * `active`: Sí se permite la cosecha del servidor en el programa. Valor: `True` ó `False`
    * `type`: Tipo de servidor. Valor: `ogc`
    * `organization`: El nombre de la organización que será propietaria de los conjuntos de datos cosechados. **Debería haber sido creada previamente.** Puede ver las organizaciones en su sitio CKAN en http://localhost:5000/api/action/organization_list. (ej. `iepnb`)
    * `inspireid_theme`: Siglas del [tema INSPIRE](https://inspire.ec.europa.eu/theme) por defecto, sí no viene indicado en los metadatos recolectados del servidor OGC. (ej. `PS`)
    * `default_provenance`: Una declaración de procedencia por defecto (ej. `Los datos espaciales se han generado a partir del Banco de Datos de la Naturaleza bajo la responsabilidad del Inventario Español del Patrimonio Natural y de la Biodiversidad, en el marco del proyecto EIKOS.`).

* Opcionales: 
    * `groups`: Grupos a los que pertenece el dataset dentro de `{ckan_site_url}/group/ `
    * `workspaces`: Listado de espacios de trabajo usados como filtro en el servidor a cosechar, sólo se ingieren los datasets que empiezan por los valores almacenados. (ej. `['ENP','inenp','RN2000']`)
    * `default_keywords`: Las palabras claves por defecto para identificar los datasets cosechados en el servidor. (ej. `['iepnb', 'environment', 'biota']`)
    * `inspireid_nutscode`: Código de país de la [Unión Europea](https://ec.europa.eu/eurostat/statistics-explained/index.php?title=Glossary:Country_codes), por defecto se usa: `ES`
    * `inspireid_versionid`: Código opcional (**puede quedarse vacío**) de la versión que debe asignarse al inspireId generado. (ej. `''` o cualquiera como: `2022`)

    >**Note**<br>
    > Pueden dejarse vacíos o rellenarse con valores a discreción.

**Servidores CSW**

### Servidores CSW
* Obligatorios:
    * `url`: URL pública del endpoint CSW. (ej. `[https://geoservicios.iepnb.es/geoserver/ows](https://www.mapama.gob.es/ide/metadatos/srv/spa/csw)`)
    * `name`: Nombre informativo del servidor. (ej. `Catalogo MITECO`)
    * `active`: Sí se permite la cosecha del servidor en el programa. Valor: `True` ó `False`
    * `type`: Tipo de servidor. Valor: `csw`
    * `organization`: El nombre de la organización que será propietaria de los conjuntos de datos cosechados. **Debería haber sido creada previamente.** Puede ver las organizaciones en su sitio CKAN en http://localhost:5000/api/action/organization_list. (ej. `iepnb`)
    * `inspireid_theme`: Siglas del [tema INSPIRE](https://inspire.ec.europa.eu/theme) por defecto, sí no viene indicado en los metadatos recolectados del servidor OGC. (ej. `PS`)

* Opcionales:
    * `groups`: Grupos a los que pertenece el dataset dentro de `{ckan_site_url}/group/ `
    * `default_keywords`: Las palabras claves por defecto para identificar los datasets cosechados en el servidor (en CKAN, `dct:keyword`). (ej. `['iepnb', 'environment', 'biota']`)
    * `constraints`: Las palabras que determinarán la restricción al cosechamiento de registros de metadatos en el endpoint CSW, así pues, solo aquellos que las contengan serán cosechados. Pueden ser de dos tipos: palabras clave (`keywords`) o correos electrónicos (`mails`). Ej. 
        ```yaml
        constraints:
            keywords: ['iepnb', 'inventario', 'patrimonio', 'miteco', 'natura'] 
            mails: ['brfranco@miteco.es', 'buzon-bdatos@miteco.es']
        ```
    * `default_bbox`: GeoJSON con el bounding box por defecto para aquellos registros que no lo contengan. (Por defecto se usa el de España: `{"type": "Polygon", "coordinates": [[[-19.0, 27.0], [4.57, 27.0], [4.57, 44.04], [-19.0, 44.04], [-19.0, 27.0]]]}`)

    >**Note**<br>
    > Pueden dejarse vacíos o rellenarse con valores a discreción.


### `ckan_info`
Información sobre la configuración interna del software de cosecha.

* Obligatorios:
    * `ckan_site_url`: URL pública del servicio CKAN. (ej. `https://iepnb.es/catalogodatos`)
    * `authorization_key`: API Key del usuario que ingiere los conjuntos de datos. Por ejemplo, la clave de autorización del usuario `admin` se muestra en http://{ckan_site_url}:5000/user/admin

* Opcionales:
    * `default_license`: Valor de la `url` de la Licencia para la propiedad `dct:license` en base a la lista de licencias disponibles: http://{ckan_site_url}:5000/api/3/action/license_list
    * `license_id`: Valor del `id` de la licencia usada para la visualización del dataset en CKAN, en base a la lista de licencias disponibles: http://{ckan_site_url}:5000/api/3/action/license_list
  
    >**Note**<br>
    > Pueden dejarse vacíos o rellenarse con valores a discreción.


### Configuración interna
* `ckan_harvester`
  * `name`: Nombre informativo del tipo de cosechador.
  * `type`: Tipo de servidor. Valor: `ogc` ó `csw`
  * `active`  Sí se permite la cosecha de este tipo de cosechador en el programa. Valor: `True` ó `False`

  >**Note**<br>
  > Permite activar o desactivar los tipos de cosechador del programa a discreción.

### `default`
* `log_folder`: Ruta del fichero de Log, por defecto toma `../../../log/`
* `parallelization`: Sí desea ejecutar el cosechador de forma paralela, utilizará todos los núcleos disponibles menos uno. **Puede omitir conjuntos de datos la paralelización si se cosechan dos servidores que puedan contener los mismos registros de metadatos (UUID)** Valor: `True` ó `False`


### `db_dsn` [`Sin uso`]
  * `host`: Nombre o IP del host.
  * `port`: Puerto del host del que hace uso la BBDD. Por defecto: `5432`
  * `dbname`: Nombre de la base de datos.
  * `username`: Nombre del usuario.
  * `password`: Contraseña del usuario.

  >**Note**<br>
  > **Actualmente si uso** | Datos de conexión a la base de datos.

## Execution
Example of CKAN harvester execution:
```shell
# Linux
. /my/path/geopostgis_manager/.env/bin/activate
export PYTHONPATH=/my/path/geopostgis_manager/src
python3 /my/path/geopostgis_manager/src/ckan/ogc_ckan/run.py

# Windows
.env\Scripts\activate.bat  # CMD || env\Scripts\Activate.ps1  # Powershell
PYTHONHOME=/my/path/geopostgis_manager
python /ckan/ogc_ckan/run.py
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