<h1 align="center" style="margin: 0">
Publicación de servicios OGC mediante Geoserver
</h1>

<p align="center" style="line-height:1" style="margin: 1">
    <a href="#configuración-inicial">Configuración inicial</a> •
    <a href="#flujo-de-trabajo">Flujo de trabajo</a> •
    <a href="#documentación">Documentación</a>
</p>

Este archivo explica cómo crear el servidor de tipo Geoserver para la publicación de los servicios cartográficos, y sus metadatos, conforme a los estándares del OGC mediante el uso de la librería [`geoserver-rest`](https://geoserver-rest.readthedocs.io/en/latest/about.html) para la gestión de datos espaciales en Geoserver.

Requisitos:
* Sistema Windows/Linux 64 bit
* Servidor de mapas [Geoserver](https://docs.geoserver.org/latest/en/user/installation/index.html) instalado.


## Configuración inicial
1. Instalar Apache Tomcat tal y como se detalla en la [documentación de CKAN](/geoservices/ckan/README.md#tomcat)

2. Descargar el `WAR` con la última versión estable de [Geoserver](http://geoserver.org/release/stable/)

3. Desplegar en el gestor de aplicaciones de Tomcat.

### Configuración en producción
Ver más información en la documentación de [Geoserver](https://docs.geoserver.org/stable/en/user/production/config.html) y [GeoSolutions](https://geoserver.geo-solutions.it/multidim/adv_gsconfig/gsproduction.html).

- Configuración de [optimización de servicios de coberturas](https://geoserver.geo-solutions.it/multidim/enterprise/raster.html).

### Extensiones
[Extensiones](http://geoserver.org/release/stable/) recomendadas:
1. Pregeneralized Features
2. Importer
3. **INSPIRE**
4. Image Pyramid
5. NetCDF (Coverage Formats & Output Formats)
6. **JPEG Turbo**
7. OGR (WFS, WPS)
8. Vector Tiles
9. **CSW**
10. **GeoStyler**


## TODO: Flujo de trabajo
Flujo de trabajo de carga de fenómenos discretos (vectoriales) y coberturas (ráster) en Geoserver para la publicación como servicios OGC.


## [`geoserver-pyapi`](https://github.com/mjanez/geopostgis_manager)
### Creación de los espacios de trabajo
El siguiente paso se utiliza para inicializar la biblioteca. Toma parámetros como url geoserver, nombre de usuario, contraseña.
```python
from geopostgis-manager.model.Geoserver import Geoserver
geo = Geoserver('http://127.0.0.1:8080/geoserver', username='admin', password='geoserver')
```

### Creación de los espacios de trabajo
```python
geo.create_workspace(workspace='demo')
```

### Creación de los coveragestores
Es útil para publicar los datos ráster en el geoservidor. Aquí, si no pasas el parámetro `lyr_name`, tomará el nombre del archivo raster como nombre de la capa.
```python
geo.create_coveragestore(layer_name='layer1', path=r'path\to\raster\file.tif', workspace='demo')
```

>**Note:**<br>
> Si su raster no se carga correctamente, por favor asegúrese de asignar el sistema de coordenadas para su archivo raster.
>
> Si el `layer_name` ya existe en geoserver, sobrescribirá automáticamente el anterior.

### Carga de fenómenos discretos
Creación y publicación de `featurestores` y `featurestore layers`.
Se utiliza para conectar el PostGIS con geoserver y publicar esto como una capa. Sólo es útil para datos vectoriales. Los parámetros de conexión PostGIS deben ser pasados como parámetros. Para publicar las tablas PostGIS, el parámetro `pg_table` representa el nombre de la tabla en postgres
```python
geo.create_featurestore(store_name='geo_data', workspace='demo', db='postgres', host='localhost', pg_user='postgres', pg_password='admin')
geo.publish_featurestore(workspace='demo', store_name='geo_data', pg_table='geodata_table_name')
```

La nueva función `publish_featurestore_sqlview` se puede ejecutar utilizando el siguiente comando,
```python
sql = 'SELECT name, id, geom FROM post_gis_table_name'
geo.publish_featurestore_sqlview(store_name='geo_data', name='view_name', sql=sql, key_column='name', workspace='demo')
```

### Carga de coberturas
> The following code will first convert all the ``.rst`` data format inside ``C:\Users\gic\Desktop\etlIa\`` folder, into ``tiff`` format and then upload all the ``tiff`` files to the GeoServer. 
> [Ejemplo automatización GeoTIFF](https://geoserver-rest.readthedocs.io/en/latest/advance_use.html)

Optimizar el código para más variantes ráster.

```py
from geo.Geoserver import Geoserver
from osgeo import gdal
import glob
import os

geo = Geoserver('http://localhost:8080/geoserver', username='admin', password='geoserver')

rst_files = glob.glob(r'C:\Users\gic\Desktop\etlIa\*.rst')
# geo.create_workspace('geonode')

for rst in rst_files:
    file_name = os.path.basename(file_name)
    src = gdal.Open(rst)
    tiff = r'C:\Users\tek\Desktop\try\{}'.format(file_name)
    gdal.Translate(tiff, src)
    geo.create_coveragestore(lyr_name=file_name, path=tiff, workspace='geonode')    #, overwrite=True
```

### Cargar ESRI Shapefiles
La función `create_shp` datastore será útil para cargar el shapefile y publicar el shapefile como una capa. Esta función cargará los datos en el `data_dir` de geoserver en la estructura de base de datos `h2` y los publicará como una capa. El nombre de la capa será el mismo que el nombre del `.shp`.
```python
geo.create_shp_datastore(path=r'path/to/zipped/shp/file.zip', store_name='store', workspace='demo')
```

### Crear layers
La función `create_datastore` creará el almacén de datos (`datastore`) para los datos específicos. Después de crear el almacén de datos, es necesario publicarlo como una capa mediante la función `publish_featurestore`. Puede tomar el siguiente tipo de ruta de datos:

* Ruta al archivo shapefile (`.shp)`;
* Ruta al archivo GeoPackage (`.gpkg`);
* url WFS (por ejemplo, http://localhost:8080/geoserver/wfs?request=GetCapabilities) o;
* Directorio que contiene shapefiles.


**ESRI Shapefile**
```python
geo.create_datastore(name="ds", path=r'path/to/shp/file_name.shp', workspace='demo')
geo.publish_featurestore(workspace='demo', store_name='ds', pg_table='file_name')
```

**WFS**
```python
geo.create_datastore(name="ds", path='http://localhost:8080/geoserver/wfs?request=GetCapabilities', workspace='demo')
geo.publish_featurestore(workspace='demo', store_name='ds', pg_table='wfs_layer_name')
```

[**PostGIS**](#carga-de-fenómenos-discretos)


### Cargar simbologías
Se utiliza para cargar archivos `SLD` y publicar estilos. Si el nombre del estilo ya existe, puede pasar el parámetro `overwrite=True` para sobrescribirlo. El nombre del estilo será el nombre del fichero subido.

Antes de subir el archivo `SLD`, por favor compruebe la versión de su archivo sld. Por defecto la versión de sld será `1.0.0`. Como he notado, por defecto QGIS proporcionará el fichero .sld de la versión `1.0.0 `para datos rasterizados y de la versión `1.1.0` para datos vectoriales.
```python
geo.upload_style(path=r'path\to\sld\file.sld', workspace='demo')
geo.publish_style(layer_name='geoserver_layer_name', style_name='sld_file_name', workspace='demo')
```

## Documentación
### Optimización
1. Configurar el tomcat para [Geoserver](https://optimizacion-de-geoserver.readthedocs.io/es/latest/) y optimizar su funcionamiento.
2. Configurar Geoserver para una gestión correcta de ráster [documentacion](https://geoserver.geo-solutions.it/multidim/install_run/jai_io_install.html#geoserver-jai-io-install).
3. Configurarlo para [INSPIRE](https://docs.geoserver.org/stable/en/user/extensions/inspire/installing.html) 

### Generación de estilos SLD
Crear estilos SLD personalizados para cargar en el visor a través de [GeoStyler](https://geostyler.github.io/geostyler-demo/)

### Servicios de Teselas IGN
[Servicios XYZ de Teselas](https://www.idee.es/web/idee/servicios-teselas)
