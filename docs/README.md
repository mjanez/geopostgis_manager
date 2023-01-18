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


## Flujo de trabajo
Flujo de trabajo de carga de fenómenos discretos (vectoriales) y coberturas (ráster) en Geoserver para la publicación como servicios OGC.


### Creación de los espacios de trabajo


### Creación de los coveragestores


### Carga de fenómenos discretos
> [Getting started with geoserver-rest](https://geoserver-rest.readthedocs.io/en/latest/how_to_use.html#getting-started-with-geoserver-rest)


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


## Documentación
### Optimización
1. Configurar el tomcat para [Geoserver](https://optimizacion-de-geoserver.readthedocs.io/es/latest/) y optimizar su funcionamiento.
2. Configurar Geoserver para una gestión correcta de ráster [documentacion](https://geoserver.geo-solutions.it/multidim/install_run/jai_io_install.html#geoserver-jai-io-install).
3. Configurarlo para [INSPIRE](https://docs.geoserver.org/stable/en/user/extensions/inspire/installing.html) 

### Generación de estilos SLD
Crear estilos SLD personalizados para cargar en el visor a través de [GeoStyler](https://geostyler.github.io/geostyler-demo/)

### Servicios de Teselas IGN
[Servicios XYZ de Teselas](https://www.idee.es/web/idee/servicios-teselas)
