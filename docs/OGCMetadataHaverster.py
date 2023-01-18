#!/usr/bin/env python3
## File: OGCMetadataHarvester.py
## Coding: UTF-8
## Author: Manuel Ángel Jáñez García (mjanez@tragsa.es)
## Institution: Tragsatec
## Project: EIKOS
## Goal: The goal of this script is to provide the Class to harvest datasets from OGC services. Modified OGCMetadataHarvester.py to harvest CSW endpoints from ogc_ckan.
## Parent: ogc_ckan/OGCMetadataHarvester.py
""" Changelog:
    v2.0 - 30 Ago 2022: Upgraded with process parallelization, management of different harvester types (OGC, CSW) and with expansion possibilities.
    v1.0 - 16 Ago 2022: Create the first version (Compatible GeoDCAT-AP schema (ckanext-scheming))
"""
# Update the version when apply changes 
version = "2.0"

##~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~##
##        OGCMetadataHarvester.py       ##
##~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~##

# CKAN harvester functions. 

## Import libraries
from Dataset import Dataset, Distribution   
from owslib.csw import CatalogueServiceWeb
from owslib.wms import WebMapService
from owslib.wfs import WebFeatureService
from owslib.wcs import WebCoverageService
from owslib.wmts import WebMapTileService
from owslib.fes import PropertyIsEqualTo, PropertyIsLike, SortBy, SortProperty
from geojson import Polygon, dumps
import hashlib
import unicodedata
from pyproj import Proj, transform
from pprint import pprint
import re
import uuid
import requests
from bs4 import BeautifulSoup
import html
import html5lib
import pandas as pd

class CswError(Exception):
    pass

class OGCMetadataHarvester:
    def __init__(self, url, csw_url=None, wms_url=None, wfs_url=None, wcs_url=None, wmts_url=None):
        """
        Constructor of the OGCMetadataHarvester class
        :param url: OGC services main url, after which the services parameters come

        :param csw_url: None if inferred from url param. WMTS service url otherwise if it cannot be inferred from url
        param.
        :param wms_url: None if inferred from url param. WMS service url otherwise if it cannot be inferred from url
        param.
        :param wfs_url: None if inferred from url param. WFS service url otherwise if it cannot be inferred from url
        param.
        :param wcs_url: None if inferred from url param. WCS service url otherwise if it cannot be inferred from url
        param.
        :param wmts_url: None if inferred from url param. WMTS service url otherwise if it cannot be inferred from url
        param.
        """
        self.url = url
        self.csw_url = csw_url  
        self.wms_url = wms_url
        self.wfs_url = wfs_url
        self.wcs_url = wcs_url
        self.wmts_url = wmts_url  

    def set_url(self, url):
        self.url = url

    def set_csw_url(self, csw_url):
        self.csw_url = csw_url

    def set_wms_url(self, wms_url):
        self.wms_url = wms_url

    def set_wfs_url(self, wfs_url):
        self.wfs_url = wfs_url

    def set_wcs_url(self, wcs_url):
        self.wcs_url = wcs_url

    def set_wmts_url(self, wmts_url):
        self.wmts_url = wmts_url

    def get_csw_url_value(self):
        return self.csw_url

    def get_wms_url_value(self):
        return self.wms_url

    def get_wfs_url_value(self):
        return self.wfs_url

    def get_wcs_url_value(self):
        return self.wcs_url

    def get_wmts_url_value(self):
        return self.wmts_url

    def get_csw_url(self):
        return self.csw_url if self.csw_url is not None else (self.url + "?service=CSW")

    def get_wms_url(self):
        return self.wms_url if self.wms_url is not None else (self.url + "?service=wms")

    def get_wfs_url(self):
        return self.wfs_url if self.wfs_url is not None else (self.url + "?service=wfs")

    def get_wcs_url(self):
        return self.wcs_url if self.wcs_url is not None else (self.url + "?service=WCS")

    def get_wmts_url(self):
        return self.wmts_url if self.wmts_url is not None else (self.url.replace('/ows', '/gwc') + "/service/wmts")

    def connect_csw(self):
        return CatalogueServiceWeb(self.get_csw_url())

    def connect_wms(self):
        return WebMapService(self.get_wms_url())

    def connect_wfs(self):
        return WebFeatureService(self.get_wfs_url())

    def connect_wcs(self):
        return WebCoverageService(self.get_wcs_url())

    def connect_wmts(self):
        return WebMapTileService(self.get_wmts_url())


    def get_csw_records(self, csw_constraints, csw_mails, csw_records, typenames="csw:Record", limit=None,
                      esn="full", outputschema="http://www.isotc211.org/2005/gmd",
                      page=30, startposition=0, qtype=None):
        """
        getrecords2()
        Construct and process a  GetRecords request
        Parameters
        ----------
        - constraints: the list of constraints (OgcExpression from owslib.fes module)
        - sortby: an OGC SortBy object (SortBy from owslib.fes module)
        - typenames: the typeNames to query against (default is csw:Record)
        - esn: the ElementSetName 'full', 'brief' or 'summary' (default is 'summary')
        - outputschema: the outputSchema (default is 'http://www.opengis.net/cat/csw/2.0.2')
        - format: the outputFormat (default is 'application/xml')
        - startposition: requests a slice of the result set, starting at this position (default is 0)
        - maxrecords: the maximum number of records to return. No records are returned if 0 (default is 10)
        - cql: common query language text.  Note this overrides bbox, qtype, keywords
        - xml: raw XML request.  Note this overrides all other options
        - resulttype: the resultType 'hits', 'results', 'validate' (default is 'results')
        - distributedsearch: `bool` of whether to trigger distributed search
        - hopcount: number of message hops before search is terminated (default is 1)
        """
        csw = self.connect_csw()
        csw.sortby = SortBy([SortProperty('dc:identifier')])

        if csw_constraints is not None:
            csw_constraints.append(PropertyIsLike("csw:anyText", qtype))
    
        kwa = {
            #"constraints": csw_constraints,
            "typenames": typenames,
            "esn": esn,
            "maxrecords": page,
            "outputschema": outputschema,
            "startposition": startposition,
            "sortby": csw.sortby
            }
        print('Making CSW request: getrecords2 %r', kwa)
        i = 0
        matches = 0
        while True:
            csw.getrecords2(**kwa)
            [csw_records.append(r) for r in list(csw.records.values())]
            if csw.exceptionreport:
                err = 'Error getting identifiers: %r' % \
                        csw.exceptionreport.exceptions
                #log.error(err)
                raise CswError(err)

            if matches == 0:
                matches = csw.results['matches']
            
            identifiers = list(csw.records.keys())
            if limit is not None:
                identifiers = identifiers[:(limit-startposition)]

            if len(identifiers) == 0:
                print("csw.records matches: ", csw.results['matches'])
                break

            i += len(identifiers)
            if limit is not None and i > limit:
                print("csw.records matches: ", csw.results['matches'])
                break

            startposition += page
            if startposition >= (matches + 1):
                print("csw.records matches: ", csw.results['matches'])
                break

            kwa["startposition"] = startposition

        # Filter in x.contact[0].email for existing elements in constraints.mails
        csw_records = [x for x in csw_records if x.contact[0].email.lower().replace(' ','') in csw_mails]

        return csw_records

    @staticmethod
    def set_publisher_info(dataset, publisher_uri=None, default_dcat_info=None):
        dataset.set_publisher_uri(publisher_uri)
        if default_dcat_info is not None:
            dataset.set_publisher_type(default_dcat_info["publisher_type"])
            dataset.set_publisher_name(default_dcat_info["publisher_name"])
            dataset.set_publisher_url(default_dcat_info["publisher_url"])
            dataset.set_publisher_email(default_dcat_info["publisher_email"])
            dataset.set_publisher_identifier(default_dcat_info["publisher_identifier"])

    @staticmethod
    def get_ckan_format(**kwargs):
        try:
            informat = ' '.join(map(str, kwargs.values())).replace(' ', '').lower()
        except:
            informat = kwargs['url']

        if "wms" in informat:
            format_type="WMS"
            media_type=None
            conformance="http://www.opengeospatial.org/standards/wmts"
        elif "zip" in informat:
            format_type="ZIP"
            media_type="https://www.iana.org/assignments/media-types/application/zip"
            conformance="https://www.iso.org/standard/60101.html"
        elif "rar" in informat:
            format_type="RAR"
            media_type="https://www.iana.org/assignments/media-types/application/vnd.rar"
            conformance="https://www.rarlab.com/technote.htm"
        elif "wfs" in informat:
            format_type="WFS"
            media_type=None
            conformance="http://www.opengeospatial.org/standards/wfs"
        elif "wcs" in informat:
            format_type="WCS"
            media_type=None
            conformance="http://www.opengeospatial.org/standards/wcs"
        elif "wmts" in informat:
            format_type="WMTS"
            media_type=None
            conformance="http://www.opengeospatial.org/standards/wmts"
        elif "kml" in informat:
            format_type="KML"
            media_type="https://www.iana.org/assignments/media-types/application/vnd.google-earth.kml+xml"
            conformance="http://www.opengeospatial.org/standards/kml"
        elif "kmz" in informat:
            format_type="KMZ"
            media_type="https://www.iana.org/assignments/media-types/application/vnd.google-earth.kmz+xml"
            conformance="http://www.opengeospatial.org/standards/kml"
        elif "gml" in informat:
            format_type="GML"
            media_type="https://www.iana.org/assignments/media-types/application/gml+xml"
            conformance="http://www.opengeospatial.org/standards/gml"
        elif "geojson" in informat:
            format_type="GeoJSON"
            media_type="https://www.iana.org/assignments/media-types/application/geo+json"
            conformance="https://www.rfc-editor.org/rfc/rfc7946"
        elif "atom" in informat:
            format_type="ATOM"
            media_type="https://www.iana.org/assignments/media-types/application/atom+xml"
            conformance="https://validator.w3.org/feed/docs/atom.html"
        elif "xml" in informat:
            format_type="XML"
            media_type="https://www.iana.org/assignments/media-types/application/xml"
            conformance="https://www.w3.org/TR/REC-xml/"
        elif "shp" in informat or "shapefile" in informat or "esri" in informat:
            format_type="SHP"
            media_type="https://www.iana.org/assignments/media-types/application/vnd.shp"
            conformance="https://www.esri.com/library/whitepapers/pdfs/shapefile.pdf"
        elif "visor" in informat or "enlace" in informat:
            format_type="HTML"
            media_type="https://www.iana.org/assignments/media-types/text/html"
            conformance="https://www.w3.org/TR/2011/WD-html5-20110405/"
        elif "pdf" in informat:
            format_type="PDF"
            media_type="https://www.iana.org/assignments/media-types/application/pdf"
            conformance="https://www.iso.org/standard/75839.html"
        elif "csv" in informat:
            format_type="CSV"
            media_type="https://www.iana.org/assignments/media-types/text/csv"
            conformance="https://www.rfc-editor.org/rfc/rfc4180"
        elif "NetCDF" in informat:
            format_type="NetCDF"
            media_type="https://www.iana.org/assignments/media-types/text/csv"
            conformance="http://www.opengeospatial.org/standards/netcdf"


        return format_type, media_type, conformance

    @staticmethod
    def get_dir3_uri(dir3_soup, uri_default, organization=None):
        """
        Gets org URI based on DIR3 identifiers

        :param dir3_soup: BeautifulSoup data of datos.gob.es
        :param: uri_default: Default URI if nothing is found, used config.yml
        :param: organization: Organization string from record contact object

        :return: dataset value
        """
        if '.' in organization:
            organization = organization.split('.')[0]
        try:
            dir3_df = pd.read_html(
                    str(dir3_soup.find('table', class_='table table-bordered table-condensed table-hover')
                    )
                )[0]
            dir3_df = dir3_df[dir3_df['Organismo'].str.contains(organization.split('.')[0], case=False, regex=True)]
            uri_value= dir3_df.iloc[0]['URI']
        except:
            if uri_default is not None:
                uri_value=uri_default
            uri_value=None

        return uri_value

    @staticmethod
    def get_ckan_name(name, organization_name):
        # the name of a CKAN dataset, must be between 2 and 100 characters long and contain only lowercase
        # alphanumeric characters, - and _, e.g. 'warandpeace'
        normal = str(unicodedata.normalize('NFKD', name).encode('ASCII', 'ignore'))[2:-1]
        normal = normal.lower()
        ckan_name = ''
        for c in normal:
            if ('0' <= c <= '9') or ('a' <= c <= 'z') or (c == '-') or (c == '_'):
                ckan_name += c
            else:
                ckan_name += '_'
        ckan_name = organization_name + '_' + ckan_name
        # If name is longer than 100 characters, a hash function is applied
        if len(ckan_name) >= 100:
            ckan_name = hashlib.sha1(name.encode("utf-8")).hexdigest()
        return ckan_name

    @staticmethod
    def set_min_max_coordinates(dataset, minx, maxx, miny, maxy):
        '''
        '''
        #print(str(minx) + " " + str(maxx) + " " + str(miny) + " " + str(maxy))
        #transformer = pyproj.Transformer.from_crs("epsg:3857", "epsg:4326")
        #print("transform")
        #print(transformer.transform(minx, miny))
        dataset_bb = dumps(Polygon([[
            (minx, miny),
            (maxx, miny),
            (maxx, maxy),
            (minx, maxy),
            (minx, miny)
        ]]))

        dataset.set_spatial(dataset_bb)

    def set_bounding_box(self, dataset, bounding_box):
        '''
        Extracts bounding box from array with this format: [{'nativeSrs': 'https://www.opengis.net/def/crs/EPSG/0/4326',
        'bbox': (42.85220261509768, -8.578697981248412, 42.90184661509768, -8.511959981248413)}]
        '''
        # 0: minx, 1: miny, 2: maxx, 3: maxy
        self.set_min_max_coordinates(dataset, bounding_box[0], bounding_box[2], bounding_box[1], bounding_box[3])

    def set_bounding_box_from_iso(self, dataset, bounding_box):
        # Need to convert a string to float
        self.set_min_max_coordinates(dataset, float(bounding_box.minx), float(bounding_box.maxx), float(bounding_box.miny), float(bounding_box.maxy))

    def set_bounding_box_from_bounding_box(self, dataset, bounding_box):
        if bounding_box[4].id:
            projection = Proj(bounding_box[4].id)

            miny, maxy = transform(projection,"EPSG:4326",bounding_box[0],bounding_box[2])
            minx, maxx = transform(projection,"EPSG:4326",bounding_box[2],bounding_box[3])

            self.set_min_max_coordinates(dataset, minx, maxx, miny, maxy)

    @staticmethod
    def set_title(dataset, title, alternate_title):
        if title is not None:
            dataset.set_title(title)
        else:
            dataset.set_title(alternate_title)

    @staticmethod
    def set_conformance(dataset, code=None, epsg_text=None):
        # Add INSPIRE conformance
        dataset.set_conformance(["https://inspire.ec.europa.eu/documents/inspire-metadata-regulation","https://inspire.ec.europa.eu/documents/commission-regulation-eu-no-13122014-10-december-2014-amending-regulation-eu-no-10892010-0","https://semiceu.github.io/GeoDCAT-AP/releases/2.0.0/"])
        # We only add coordinate reference system for those layers having coordinates
        if dataset.spatial is not None:
            if code is not None:
                dataset.conformance.append(code)
            else:
                # Check reference_system
                try:
                    epsg = re.findall(r'((?i)epsg\S[^\s]+)', str(epsg_text))[0].replace("EPSG:", "")
                    if len(epsg) <= 5 and epsg.isdigit():
                       dataset.conformance.append("https://www.opengis.net/def/crs/EPSG/0/" + epsg)
                except:
                    dataset.conformance.append("https://www.opengis.net/def/crs/EPSG/0/4326")

    @staticmethod
    def set_themes(dataset, themes):
        inspire_themes = []
        for theme in themes:
            if "http" in theme or "inspire" in theme:
                inspire_themes.append(theme.replace('http:', 'https:'))
            else:
                inspire_themes.append("https://inspire.ec.europa.eu/theme/" + theme.lower())

        dataset.set_theme(inspire_themes)

    @staticmethod
    def set_lineage_source(dataset, lineage_sources):
            lineage_sources= [w.replace(',', ';') for w in lineage_sources]
            dataset.set_lineage_source(lineage_sources)

    @staticmethod
    def set_lineage_process_steps(dataset, lineage_process_steps):
            lineage_process_steps= [w.replace(',', ';') for w in lineage_process_steps]
            dataset.set_lineage_process_steps(lineage_process_steps)

    @staticmethod
    def set_keywords_uri(dataset, keywords_uri):
            keywords_uri= [w.replace(',', ';') for w in keywords_uri]
            dataset.set_keywords_uri(keywords_uri)

    @staticmethod
    def add_wfs_keywords(keywords_array, keywords_text):
        for keyword in keywords_text.split(","):
            keywords_array.append({'name': keyword.lower()})

    def get_csw_record_dataset(self, csw_url, uuid_identifier, ckan_site_url, groups, organization_name, default_dcat_info, default_keywords, default_license, license_id, inspireid_theme, default_bbox, dir3_soup):
        """
        Gets Dataset from CSW service. In case the layer is published either as WFS, WCS or WMTS layer, or another format, an additional RDF and ISO19139 XML distributions are included

        :param csw_url: CSW endpoint url
        :param uuid_identifier: Dataset UUID identifier
        :param ckan_site_url: CKAN site url
        :param groups: Name of CKAN groups
        :param organization_name: Owner organization name
        :param default_dcat_info: Default publisher info
        :param default_keywords: Default keyword
        :param default_license: Default distribution license
        :param license_id: CKAN license_id 
        :param inspireid_theme: INSPIRE Theme code
        :param default_bbox: Default Bounding Box GeoJSON of Spain
        :param dir3_soup: DIR3 info table

        :return: Dataset object
        """
        # Metadata info
        ## CKAN name: {workspace}_{recordname})
        # if self.record.identification.alternatetitle is not None:
        #     csw_name = self.record.identification.alternatetitle
        # else:
        #     csw_name = self.record.identification.title

        ## CKAN name: UUID)
        ckan_name = uuid_identifier

        record_id = self.record.identification
        record_dists = self.record.distribution.online
        record_ql = self.record.dataquality
        
        dataset = Dataset(uuid_identifier, ckan_name, organization_name, license_id)

        # Set basic info of MD
        self.set_title(dataset, record_id.title, record_id.alternatetitle)
        if record_id.abstract is not None:
            dataset.set_description(record_id.abstract)
        else:
            dataset.set_description("Conjuntos de datos espaciales del registro de metadatos: " + record_id.title)

        # Set UUID (identifier)
        dataset.set_identifier(uuid_identifier)

        # Set CKAN groups
        ckan_groups = []
        if groups is not None:
            for g in groups:
                ckan_groups.append({'name': g.lower()})
        dataset.set_groups(ckan_groups)

        # Set inspireId (identifier)
        try: 
            inspireid_identifier = record_id.uricode[0]
            dataset.set_alternate_identifier(inspireid_identifier.upper())
        except:
            pass

        # creation-publication/revision dates
        #issued_date = None
        try:
            issued_date = next((x for x in record_id.date if x.type == "creation" or x.type == "publication"), None).date
            dataset.set_issued(issued_date)
        except:
            issued_date = None

        ## Modified, when update in CKAN
        try:
            modified_date = next((x for x in record_id.date if x.type == "revision"), None).date
        except:
            modified_date = None

        dataset.set_resource_type("https://inspire.ec.europa.eu/metadata-codelist/ResourceType/series")

        # dcat_type (dataset/series/service)
        is_series = record_id.identtype is not None
        if record_id.identtype == "series":
            is_series = True
            dataset.set_resource_type("https://inspire.ec.europa.eu/metadata-codelist/ResourceType/series")
        elif record_id.identtype == "service":
            is_series = False
            dataset.set_resource_type("https://inspire.ec.europa.eu/metadata-codelist/ResourceType/service")
        else:
            is_series = False
            dataset.set_resource_type("https://inspire.ec.europa.eu/metadata-codelist/ResourceType/dataset")

        # Set SpatialRepresentationType
        try:
            dataset.set_representation_type("https://inspire.ec.europa.eu/metadata-codelist/SpatialRepresentationType/" + record_id.spatialrepresentationtype[0])
        except:
            pass

        # Set access rights (Dataset)
        default_rights = "https://inspire.ec.europa.eu/metadata-codelist/LimitationsOnPublicAccess/noLimitations"
        dataset.set_access_rights(default_rights)

        # Set SpatialResolutionInMeters
        try: 
            dataset.set_spatial_resolution_in_meters(record_id.denominators[0])
        except:
            pass

        # Set SpatialResolutionInMeters
        try: 
            dataset.set_spatial_resolution_in_meters(record_id.denominators[0])
        except:
            pass

        # Set language
        try:
            language = "https://publications.europa.eu/resource/authority/language/" + record_id.resourcelanguagecode[0].upper()
            dataset.set_languages(language)
        except:
            language = "https://publications.europa.eu/resource/authority/language/SPA"
            dataset.set_languages(language)

        # Set spatial coverage
        try:
            bb = record_id.bbox
        except:
            bb = None
        if bb is not None:
            self.set_bounding_box_from_iso(dataset, bb)
        else:
            dataset.set_spatial(default_bbox)

        # Set temporal coverage (only series)
        if is_series:
            try:
                dataset.set_temporal_start(record_id.temporalextent_start)
                dataset.set_temporal_end(record_id.temporalextent_end)
            except IndexError:
                dataset.set_temporal_start("1900-01-01")
                dataset.set_temporal_end(None)
        
        # Set provenance (INSPIRE Lineage)
        dataset.set_provenance(record_ql.lineage)

        # Set source (INSPIRE quality) & lineage_source (INSPIRE Lineage sources)
        dataset.set_source(None)
        try:
            soup_source = BeautifulSoup((html.unescape(self.record.xml.decode("ascii"))), "xml").find_all("LI_Source")
            lineage_sources = [sources.text.replace("\n", "") for sources in soup_source]
            if not lineage_sources: 
                dataset.set_lineage_source([])
            else:
                self.set_lineage_source(dataset, lineage_sources)
        except:
            dataset.set_lineage_source([])

        # Set process steps (INSPIRE quality)
        try:
            soup_process = BeautifulSoup((html.unescape(self.record.xml.decode("ascii"))), "xml").find_all("LI_ProcessStep")
            lineage_process_steps = [sources.text.replace("\n", "") for sources in soup_process]
            if not lineage_process_steps: 
                dataset.set_lineage_process_steps([])
            else:
                self.set_lineage_process_steps(dataset, lineage_process_steps)
        except:
            dataset.set_lineage_process_steps([])
        
        # Set conformance (INSPIRE regulation + EPSG)
        try:
            reference_system = self.record.referencesystem.code
            if "http" in reference_system:
                self.set_conformance(dataset, code=reference_system)
            else:
                try:        
                    self.set_conformance(dataset, epsg_text=reference_system)
                except:
                    self.set_conformance(dataset)
        except:
            self.set_conformance(dataset)


        # Point of contact (Metadata) 
        ## contact_name (pointOfContact Metadata)
        if self.record.contact[0]:
            contact_name = self.record.contact[0].name
            contact_mail = self.record.contact[0].email.lower().replace(' ','')
            dataset.set_contact_name(contact_name)
            dataset.set_contact_email(contact_mail)
            try:
                dataset.set_contact_url(self.record.contact[0].onlineresource.url)
            except:
                dataset.set_contact_url(default_dcat_info["publisher_url"])

            contact_uri= self.get_dir3_uri(dir3_soup, default_dcat_info["contact_uri"], self.record.contact[0].organization)
            dataset.set_contact_uri(contact_uri)


        # Responsible Party (Resource publisher)
        publisher_uri = (ckan_site_url + "/organization/" + organization_name).lower()
        self.set_publisher_info(dataset, publisher_uri, default_dcat_info)

        # pointOfContact/maintainer dataset Responsible Party (Resource contact/maintainer)
        if next((x for x in record_id.contact if x.role == "pointOfContact" or x.role == "custodian" or x.role == "resourceProvide"), None):
            maintaner_info = next((x for x in record_id.contact if x.role == "pointOfContact" or x.role == "custodian" or x.role == "resourceProvide"), None)
            try:
                maintainer_name = maintaner_info.name
                maintainer_email = maintaner_info.email
            except:
                maintainer_name = None
                maintainer_email = None
            
            maintainer_uri= self.get_dir3_uri(dir3_soup, default_dcat_info["maintainer_uri"], self.record.contact[0].organization)
            
            dataset.set_maintainer_name(maintainer_name)
            dataset.set_maintainer_email(maintainer_email)
            dataset.set_maintainer_uri(maintainer_uri)  

        # Responsible Party (Resource creator)
        if next((x for x in record_id.contact if x.role == "creator"), None):
            author_info = next((x for x in record_id.contact if x.role == "creator"), None)
            try:
                author_name = author_info.name
                author_email = author_info.email
            except:
                author_name = None
                author_email = None
            
            author_uri=self.get_dir3_uri(dir3_soup, None, self.record.contact[0].organization)

            dataset.set_author_name(author_name)
            dataset.set_author_email(author_email)
            dataset.set_author_uri(author_uri)

        # Set license
        dataset.set_license(default_license)

        # Add distributions
        dataset.set_distributions([])
        for r in record_dists:
            try:
                # Set distribution format from MD
                kwargs = {}
                if r.protocol is not None:
                    kwargs["protocol"] = r.protocol
                if r.url is not None:
                    kwargs["url"] = r.url
                if r.description is not None:
                    kwargs["description"] = r.description                   
                format_type, media_type, conformance = self.get_ckan_format(**kwargs)
            except:
                format_type = None
                media_type = None
                conformance = []

            dataset.add_distribution(Distribution(
                url=r.url,
                name=r.name,
                format=format_type,
                media_type=media_type,
                description=r.description,
                license=default_license,
                license_id=license_id,
                rights=default_rights,
                language=language,
                conformance=conformance
            )) 

        # Add GeoDCAT-AP Metadata distribution
        dataset.add_distribution(Distribution(
            url=ckan_site_url + "/dataset/" + ckan_name + ".rdf", # http://localhost:5000/dataset/{ckan_name}.rdf
            name="Metadatos GeoDCAT-AP 2.0",
            format="RDF",
            description="Metadatos conforme a la extensión GeoDCAT-AP (https://semiceu.github.io/GeoDCAT-AP/releases/) del perfil de metadatos de portales de datos abiertos en Europa (DCAT-AP) para conjuntos de datos y servicios espaciales. GeoDCAT-AP proporciona un vocabulario RDF y el correspondiente enlace de sintaxis RDF para la unión de los elementos de metadatos del perfil básico de la norma ISO 19115:2003 y los definidos en el marco de la Directiva INSPIRE.",
            license=default_license,
            license_id=license_id,
            rights=default_rights,
            media_type = "https://www.iana.org/assignments/media-types/application/rdf+xml",
            language=language,
            conformance= ["https://semiceu.github.io/GeoDCAT-AP/releases/2.0.0/"]
        )) 

        # Add INSPIRE ISO19139 Metadata distribution
        dataset.add_distribution(Distribution(
            url=self.get_csw_url() + "&version=2.0.2&request=GetRecordById&id=" + uuid_identifier + "&elementsetname=full&outputSchema=http://www.isotc211.org/2005/gmd",
            name="Metadatos INSPIRE ISO-19139",
            format="XML",
            media_type = "https://www.iana.org/assignments/media-types/application/xml",
            description="Metadatos para los conjuntos de datos y servicios INSPIRE en ISO/TS 19139 (https://inspire.ec.europa.eu/id/document/tg/metadata-iso19139).",
            license=default_license,
            license_id=license_id,
            rights=default_rights,
            language=language,
            modified=modified_date,
            issued=issued_date,
            conformance= [
                "https://inspire.ec.europa.eu/documents/inspire-metadata-regulation",
                "https://inspire.ec.europa.eu/documents/commission-regulation-eu-no-13122014-10-december-2014-amending-regulation-eu-no-10892010-0"
                ]
                
            )) 

        # include default keywords
        keywords = []
        themes = []
        keywords_uri = []
        if default_keywords is not None:
            for k in default_keywords:
                if '/theme/' in k["uri"]:
                    # INSPIRE Theme
                    keywords.append({'name': k["name"].lower()})
                    themes.append(k.attrs['xlink:href'])
                else: 
                    # Keyword
                    keywords.append({'name': k["name"].lower()})
                keywords_uri.append(k["uri"])

        # Set keywords (INSPIRE quality) and INSPIRE Themes
        try:
            soup_process = BeautifulSoup((html.unescape(self.record.xml.decode("ascii"))), "xml").find_all("gmd:keyword")
            inspire_keywords = [sources.contents[1] for sources in soup_process]
            for k in inspire_keywords:
                try:
                    # Theme/Keyword             
                    if k.text is not None and k.text.replace(' ', '').isalnum() and k.attrs['xlink:href'] is not None:
                        if '/theme/' in k.attrs['xlink:href']:
                            keywords.append({'name':  k.attrs['xlink:href'].rsplit('/', 1)[-1], 'display_name': k.text.lower()})
                            themes.append(k.attrs['xlink:href'])
                        else:
                            keywords.append({'name':  k.attrs['xlink:href'].rsplit('/', 1)[-1]})
                            keywords_uri.append(k.attrs['xlink:href']) 
                except:
                    pass
        except:
            # Insert inspireid_theme (default) as theme/keyword
            keywords.append({'name': inspireid_theme.lower()})
            keywords_uri.append("https://inspire.ec.europa.eu/theme/" + inspireid_theme.lower())                
            themes = [inspireid_theme]

        if themes is None:
            # Insert inspireid_theme (default) as theme/keyword
            keywords.append({'name': inspireid_theme.lower()})
            keywords_uri.append("https://inspire.ec.europa.eu/theme/" + inspireid_theme.lower())                
            themes = [inspireid_theme]

        self.set_themes(dataset, list(set(themes)))
        dataset.set_keywords(keywords)
        self.set_keywords_uri(dataset, list(set(keywords_uri)))

        return dataset

    def get_coverage_dataset(self, name, ckan_site_url, groups, organization_name, default_dcat_info, default_keywords, default_license, license_id, inspireid_theme, inspireid_nutscode, inspireid_versionid, workspaces, default_provenance, dir3_soup, wms=None, wcs=None, wmts=None):
        """
        Gets Dataset from WCS service. In case the layer is also published as WMS layer, the distribution is also included

        :param name: Dataset name to retrieve
        :param ckan_site_url: CKAN site url
        :param groups: Name of CKAN groups
        :param organization_name: Owner organization name
        :param default_dcat_info: Default publisher info
        :param default_keywords: Default keyword
        :param default_license: Default distribution license
        :param workspace: Geoserver workspace
        :param default_provenance: Default provenance statement
        :param wms: Should not be filled, inferred from url. It will only be filled by getAllDatasets to reuse the same connection
        :param wcs: Should not be filled, inferred from url. It will only be filled by getAllDatasets to reuse the same connection
        :param wmts: Should not be filled, inferred from url. It will only be filled by getAllDatasets to reuse the same connection

        :return: Dataset object
        """
        # CONNECT
        if wms is None:
            wms = self.connect_wms()

        if wcs is None:
            wcs = self.connect_wcs()

        if wmts is None:
            wmts = self.connect_wmts()

        # Metadata info
        wms_name = name.replace("__", ":")

        # Create UUID
        uuid_identifier = str(uuid.uuid1())

        ## TODO:Usar UUID en vez de name | OGC name: WMS layer name ({workspace}_{layername})
        ckan_name = self.get_ckan_name(wms_name, organization_name)

        layer_info = wcs.contents[name]
        wms_layer_info = None
        wmts_layer_info = None
        if wms_name in wms.contents:
            wms_layer_info = wms.contents[wms_name]
        if name in wmts.contents:
            wmts_layer_info = wmts.contents[name]

        dataset = Dataset(uuid_identifier, ckan_name, organization_name, license_id)

        # Set basic info of MD 
        self.set_title(dataset, layer_info.title, wms_name)
        if layer_info.abstract is not None:
            dataset.set_description(layer_info.abstract)
        else:
            dataset.set_description(default_provenance)

        # Set UUID (identifier)
        dataset.set_identifier(uuid_identifier)

        # Set CKAN groups
        ckan_groups = []
        if groups is not None:
            for g in groups:
                ckan_groups.append({'name': g.lower()})
        dataset.set_groups(ckan_groups)

        # Set inspireId (identifier)
        inspire_id = ".".join(filter(None,[inspireid_nutscode, inspireid_theme, name.replace(':', '.'), inspireid_versionid])).upper()
        dataset.set_alternate_identifier(inspire_id.upper())  # create inspireid

        # dcat_type (dataset/series)        
        is_series = False
        if wms_layer_info is not None:
            is_series = wms_layer_info.timepositions is not None
        if is_series:
            is_series = len(wms_layer_info.timepositions) != 0
        if is_series:
            dataset.set_resource_type("https://inspire.ec.europa.eu/metadata-codelist/ResourceType/series")
        else:
            dataset.set_resource_type("https://inspire.ec.europa.eu/metadata-codelist/ResourceType/dataset")

        # Set SpatialRepresentationType
        dataset.set_representation_type("https://inspire.ec.europa.eu/metadata-codelist/SpatialRepresentationType/grid")

        # Set access rights (Dataset)
        default_rights = "https://inspire.ec.europa.eu/metadata-codelist/LimitationsOnPublicAccess/noLimitations"
        dataset.set_access_rights(default_rights)

        # Set SpatialResolutionInMeters
        try: 
            dataset.set_spatial_resolution_in_meters(layer_info.denominators[0])
        except:
            pass

        # Set language
        language = "https://publications.europa.eu/resource/authority/language/SPA"
        dataset.set_languages(language)

        # Set theme
        themes = [inspireid_theme]
        self.set_themes(dataset, themes)

        # Set spatial coverage
        if wms_layer_info is not None:
            bb = wms_layer_info.boundingBoxWGS84
            if bb is not None:
                self.set_bounding_box(dataset, bb)
        else:
            if layer_info.boundingBox is not None:
                self.set_bounding_box_from_bounding_box(dataset, layer_info.boundingBox)
        
        # Set temporal coverage (only series)
        if is_series:
            time_extent = wms_layer_info.timepositions[0].split(",")
            for t in range(len(time_extent)):
                time_extent[t] = time_extent[t].split("/")
            dataset.set_temporal_start(time_extent[0][0])
            try:
                dataset.set_temporal_end(time_extent[len(time_extent) - 1][1])
            except IndexError:
                dataset.set_temporal_end(time_extent[len(time_extent) - 1][0])

        # Set provenance (INSPIRE Lineage)
        dataset.set_provenance(default_provenance)

        # Set source (INSPIRE quality) & lineage_source (INSPIRE Lineage sources)
        dataset.set_source(None)
        dataset.set_lineage_source([])

        # Set process steps (INSPIRE quality)
        dataset.set_lineage_process_steps([])

        # Set conformance (INSPIRE regulation + EPSG)
        try:
            self.set_conformance(dataset, epsg_text=layer_info.crsOptions[0])
        except:
            self.set_conformance(dataset)

        # Point of contact (Metadata) and Responsible Party (Resource)
        ## contact_name (pointOfContact Metadata)
        dataset.set_contact_name(wms.provider.contact.name)
        dataset.set_contact_email(wms.provider.contact.email.lower())
        dataset.set_contact_url(default_dcat_info["publisher_url"])
        dataset.set_contact_uri(default_dcat_info["contact_uri"])

        ## maintainer (pointOfContact Resource)
        dataset.set_maintainer_name(wms.provider.contact.name)
        dataset.set_maintainer_email(wms.provider.contact.email.lower())
        dataset.set_maintainer_uri(default_dcat_info["maintainer_uri"]) 

        # Responsible Party (Resource publisher)
        publisher_uri = (ckan_site_url + "/organization/" + organization_name).lower()
        self.set_publisher_info(dataset, publisher_uri, default_dcat_info)

        # Set license
        dataset.set_license(default_license)

        # Add distributions (WMS, WCS & WMTS)
        dataset.set_distributions([])
        if wms_layer_info is not None:
            dataset.add_distribution(Distribution(
                url=self.get_wms_url() + "?request=GetCapabilities" + "#" + name, # http://host/geoserver/ows?service=wms&request=GetCapabilities#workspace:layer
                name="Web Map Service",
                format="WMS",
                description="Servicio que proporciona visualización de mapas (WMS) conforme a las especificaciones del Open Geospatial Consortium (https://www.ogc.org/standards/wms).",
                license=default_license,
                license_id=license_id,
                rights=default_rights,
                language=language,
                conformance=["http://www.opengeospatial.org/standards/wms"]
            ))  # WMS
        dataset.add_distribution(Distribution(
            url=self.get_wcs_url() + "?request=GetCapabilities" + "#" + name, # http://host/geoserver/ows?service=wcs&request=GetCapabilities#workspace:layer
            name="Web Coverage Service",
            format="WCS",
            description="Servicio que proporciona descarga de fenómenos continuos (WCS) conforme a las especificaciones del Open Geospatial Consortium (https://www.ogc.org/standards/wcs).",
            license=default_license,
            license_id=license_id,
            rights=default_rights,
            language=language,
            conformance= ["http://www.opengeospatial.org/standards/wcs"]
        )) # WCS
        if wmts_layer_info is not None:
            dataset.add_distribution(Distribution(
                url=self.get_wmts_url() + "?request=GetCapabilities", # http://host/geoserver/gwc/service/wmts?REQUEST=getcapabilities
                name="Web Map Tile Service",
                format="WMTS",
                description="Servicio que proporciona visualización de mapas teselados (WMTS) conforme a las especificaciones del Open Geospatial Consortium (https://www.ogc.org/standards/wmts).",
                license=default_license,
                license_id=license_id,
                rights=default_rights,
                language=language,
                conformance=["http://www.opengeospatial.org/standards/wmts"]
            ))  # WMTS

        # Add GeoDCAT-AP Metadata distribution
        dataset.add_distribution(Distribution(
            url=ckan_site_url + "/dataset/" + uuid_identifier + ".rdf", # http://localhost:5000/dataset/{uuid_identifier}.rdf
            name="Metadatos GeoDCAT-AP 2.0",
            format="RDF",
            description="Metadatos conforme a la extensión GeoDCAT-AP (https://semiceu.github.io/GeoDCAT-AP/releases/) del perfil de metadatos de portales de datos abiertos en Europa (DCAT-AP) para conjuntos de datos y servicios espaciales. GeoDCAT-AP proporciona un vocabulario RDF y el correspondiente enlace de sintaxis RDF para la unión de los elementos de metadatos del perfil básico de la norma ISO 19115:2003 y los definidos en el marco de la Directiva INSPIRE.",
            license=default_license,
            license_id=license_id,
            rights=default_rights,
            media_type = "https://www.iana.org/assignments/media-types/application/rdf+xml",
            language=language,
            conformance=["https://semiceu.github.io/GeoDCAT-AP/releases/2.0.0/"]
        ))  

        # TODO: Add INSPIRE ISO19139 Metadata distribution
        '''
        Sería interesante generar metadatos INSPIRE, tal vez con un endpoint de pycsw y enlazarlos en el propio metadato de Geoserver, tal y como se hace ya con los metadatos recolectados de endpoint CSW.

        dataset.add_distribution(Distribution(
            url=self.get_csw_url() + "?service=CSW&version=2.0.2&request=GetRecordById&id=" + name + "&elementsetname=full&outputSchema=http://www.isotc211.org/2005/gmd",
            name="Metadatos INSPIRE ISO-19139",
            format="XML",
            media_type = "https://www.iana.org/assignments/media-types/application/xml",
            description="Metadatos para los conjuntos de datos y servicios INSPIRE en ISO/TS 19139 (https://inspire.ec.europa.eu/id/document/tg/metadata-iso19139).",
            license=default_license,
            license_id=license_id,
            rights=default_rights,
            language=language,
            modified=modified_date,
            issued=issued_date,
            conformance= (",".join({
                "https://inspire.ec.europa.eu/documents/inspire-metadata-regulation",
                "https://inspire.ec.europa.eu/documents/commission-regulation-eu-no-13122014-10-december-2014-amending-regulation-eu-no-10892010-0"
                }))
            )) 
            )) 
        '''

        # include default keywords
        keywords = []
        themes = []
        keywords_uri = []
        if default_keywords is not None:
            for k in default_keywords:
                if '/theme/' in k["uri"]:
                    # INSPIRE Theme
                    keywords.append({'name': k["name"].lower()})
                    themes.append(k.attrs['xlink:href'])
                else: 
                    # Keyword
                    keywords.append({'name': k["name"].lower()})
                keywords_uri.append(k["uri"])

        # include keywords extracted from WMS layer. Only keywords in WMS layer are retrieved by owslib
        if wms_layer_info is not None and wms_layer_info.keywords is not None:
            for k in wms_layer_info.keywords:
                if k not in wms_layer_info.title and "wms" not in k and "wfs" not in k and "wcs" not in k:
                    keywords.append({
                        'name': k.lower()})
        # Insert inspireid_theme (default) as theme/keyword
        keywords.append({'name': inspireid_theme.lower()})
        keywords_uri.append("https://inspire.ec.europa.eu/theme/" + inspireid_theme.lower())                
        themes = [inspireid_theme]

        self.set_themes(dataset, list(set(themes)))
        dataset.set_keywords(keywords)
        self.set_keywords_uri(dataset, list(set(keywords_uri)))

        return dataset

    def get_feature_dataset(self, name, ckan_site_url, groups, organization_name, default_dcat_info, default_keywords, default_license, license_id, inspireid_theme, inspireid_nutscode, inspireid_versionid, workspaces, default_provenance, dir3_soup, wms=None, wfs=None, wmts=None):
        """
        Gets Dataset from WFS service. In case the layer is also published as WMS layer, the distribution is also included

        :param name: Dataset name to retrieve
        :param ckan_site_url: CKAN site url
        :param groups: Name of CKAN groups
        :param organization_name: Owner organization name
        :param default_dcat_info: Default publisher info
        :param default_keywords: Default keyword
        :param default_license: Default distribution license
        :param workspace: Geoserver workspace
        :param default_provenance: Default provenance statement
        :param wms: Should not be filled, inferred from url. It will only be filled by getAllDatasets to reuse the same connection
        :param wfs: Should not be filled, inferred from url. It will only be filled by getAllDatasets to reuse the same connection
        :param wmts: Should not be filled, inferred from url. It will only be filled by getAllDatasets to reuse the same connection

        :return: Dataset object
        """
        # CONNECT
        if wms is None:
            wms = self.connect_wms()

        if wfs is None:
            wfs = self.connect_wfs()

        if wmts is None:
            wmts = self.connect_wmts()

        # Metadata info
        # Create UUID 
        uuid_identifier = str(uuid.uuid1())

        ## TODO:Usar UUID en vez de name | OGC name: WMS layer name ({workspace}_{layername})
        ckan_name = self.get_ckan_name(name, organization_name)

        layer_info = wfs.contents[name]
        wms_layer_info = None
        wmts_layer_info = None
        if name in wms.contents:
            wms_layer_info = wms.contents[name]
        if name in wmts.contents:
            wmts_layer_info = wmts.contents[name]
        if name in wfs.contents:
            json_info = wfs.contents[name]

        dataset = Dataset(uuid_identifier, ckan_name, organization_name, license_id)
        
        # Set basic info of MD 
        self.set_title(dataset, layer_info.title, name)
        if layer_info.abstract is not None:
            dataset.set_description(layer_info.abstract)
        else:
            dataset.set_description(default_provenance)

        # Set UUID (identifier)
        dataset.set_identifier(uuid_identifier)

        # Set CKAN groups
        ckan_groups = []
        if groups is not None:
            for g in groups:
                ckan_groups.append({'name': g.lower()})
        dataset.set_groups(ckan_groups)

        # Set inspireId (identifier)
        inspire_id = ".".join(filter(None,[inspireid_nutscode, inspireid_theme, name.replace(':', '.'), inspireid_versionid])).upper()
        dataset.set_alternate_identifier(inspire_id.upper())  # create inspireid

        # dcat_type (dataset/series)        
        is_series = False
        if wms_layer_info is not None:
            is_series = wms_layer_info.timepositions is not None
        if is_series:
            is_series = len(wms_layer_info.timepositions) != 0
        if is_series:
            dataset.set_resource_type("https://inspire.ec.europa.eu/metadata-codelist/ResourceType/series")
        else:
            dataset.set_resource_type("https://inspire.ec.europa.eu/metadata-codelist/ResourceType/dataset")

        # Set SpatialRepresentationType
        dataset.set_representation_type("https://inspire.ec.europa.eu/metadata-codelist/SpatialRepresentationType/vector")

        # Set access rights (Dataset)
        default_rights = "https://inspire.ec.europa.eu/metadata-codelist/LimitationsOnPublicAccess/noLimitations"
        dataset.set_access_rights(default_rights)

        # Set SpatialResolutionInMeters
        try: 
            dataset.set_spatial_resolution_in_meters(layer_info.denominators[0])
        except:
            pass

        # Set language
        language = "https://publications.europa.eu/resource/authority/language/SPA"
        dataset.set_languages(language)

        # Set theme
        themes = [inspireid_theme]
        self.set_themes(dataset, themes)

        # Set spatial coverage
        if wms_layer_info is not None:
            bb = wms_layer_info.boundingBoxWGS84
            if bb is not None:
                self.set_bounding_box(dataset, bb)
        else:
            if layer_info.boundingBox is not None:
                self.set_bounding_box_from_bounding_box(dataset, layer_info.boundingBox)
        
        # Set temporal coverage (only series)
        if is_series:
            time_extent = wms_layer_info.timepositions[0].split(",")
            for t in range(len(time_extent)):
                time_extent[t] = time_extent[t].split("/")
            dataset.set_temporal_start(time_extent[0][0])
            try:
                dataset.set_temporal_end(time_extent[len(time_extent) - 1][1])
            except IndexError:
                dataset.set_temporal_end(time_extent[len(time_extent) - 1][0])

        # Set provenance (INSPIRE Lineage)
        dataset.set_provenance(default_provenance)

        # Set source (INSPIRE quality) & lineage_source (INSPIRE Lineage sources)
        dataset.set_source(None)
        dataset.set_lineage_source([])

        # Set process steps (INSPIRE quality)
        dataset.set_lineage_process_steps([])

        # Set conformance (INSPIRE regulation + EPSG)
        try:
            self.set_conformance(dataset, epsg_text=layer_info.crsOptions[0])
        except:
            self.set_conformance(dataset)

        # Point of contact (Metadata) and Responsible Party (Resource)
        ## contact_name (pointOfContact Metadata)
        dataset.set_contact_name(wms.provider.contact.name)
        dataset.set_contact_email(wms.provider.contact.email.lower())
        dataset.set_contact_url(default_dcat_info["publisher_url"])
        dataset.set_contact_uri(default_dcat_info["contact_uri"])

        ## maintainer (pointOfContact Resource)
        dataset.set_maintainer_name(wms.provider.contact.name)
        dataset.set_maintainer_email(wms.provider.contact.email.lower())
        dataset.set_maintainer_uri(default_dcat_info["maintainer_uri"]) 

        # Responsible Party (Resource publisher)
        publisher_uri = (ckan_site_url + "/organization/" + organization_name).lower()
        self.set_publisher_info(dataset, publisher_uri, default_dcat_info)

        # Set license
        dataset.set_license(default_license)

        # Add distributions (WMS, WFS, WMTS & GeoJSON)
        dataset.set_distributions([])
        if wms_layer_info is not None:
            dataset.add_distribution(Distribution(
                url=self.get_wms_url() + "?request=GetCapabilities" + "#" + name, # http://host/geoserver/ows?service=wms&request=GetCapabilities#workspace:layer
                name="Web Map Service",
                format="WMS",
                description="Servicio que proporciona visualización de mapas (WMS) conforme a las especificaciones del Open Geospatial Consortium (https://www.ogc.org/standards/wms).",
                license=default_license,
                license_id=license_id,
                rights=default_rights,
                language=language,
                conformance=["http://www.opengeospatial.org/standards/wms"]
            ))  # WMS
        dataset.add_distribution(Distribution(
            url=self.get_wfs_url() + "?request=GetCapabilities" + "#" + name, # http://host/geoserver/ows?service=wfs&request=GetCapabilities#workspace:layer
            name="Web Feature Service",
            format="WFS",
            description="Servicio que proporciona descarga de fenómenos discretos (WFS) conforme a las especificaciones del Open Geospatial Consortium (https://www.ogc.org/standards/wfs).",
            license=default_license,
            license_id=license_id,
            rights=default_rights,
            language=language,
            conformance=["http://www.opengeospatial.org/standards/wfs"]
        )) # WFS
        if wmts_layer_info is not None:
            dataset.add_distribution(Distribution(
                url=self.get_wmts_url() + "?request=GetCapabilities", # http://host/geoserver/gwc/service/wmts?REQUEST=getcapabilities
                name="Web Map Tile Service",
                format="WMTS",
                description="Servicio que proporciona visualización de mapas teselados (WMTS) conforme a las especificaciones del Open Geospatial Consortium (https://www.ogc.org/standards/wmts).",
                license=default_license,
                license_id=license_id,
                rights=default_rights,
                language=language,
                conformance=["http://www.opengeospatial.org/standards/wmts"]
            ))  # WMTS
        if json_info is not None:
            workspace= layer_info.id.split(':')[0]
            layername= layer_info.id.split(':')[1]
            dataset.add_distribution(Distribution(
                url=self.get_wfs_url().replace('geoserver/ows', 'geoserver/' + workspace.lower() + '/ows') + '&version=1.0.0&request=GetFeature&typeName=' + layername.lower() + '&outputFormat=application/json&maxFeatures=100', # http://host/geoserver/mnr/ows?service=WFS&version=1.0.0&request=GetFeature&typeName=mnr:nuts&outputFormat=application/json
                name="GeoJSON Service",
                media_type = "https://www.iana.org/assignments/media-types/application/geo+json",
                format="GeoJSON",
                description="Formato para el intercambio de datos espaciales basados en la notación JSON (GeoJSON) conforme a la especificación  de la Internet Engineering Task Force (https://www.rfc-editor.org/rfc/rfc7946).",
                license=default_license,
                license_id=license_id,
                rights=default_rights,
                language=language,
                conformance=["http://www.opengeospatial.org/standards/wfs"]
            ))  # GeoJSON


        # Add GeoDCAT-AP Metadata distribution
        dataset.add_distribution(Distribution(
            url=ckan_site_url + "/dataset/" + ckan_name + ".rdf", # http://localhost:5000/dataset/{ckan_name}.rdf
            name="Metadatos GeoDCAT-AP 2.0",
            format="RDF",
            description="Metadatos conforme a la extensión GeoDCAT-AP (https://semiceu.github.io/GeoDCAT-AP/releases/) del perfil de metadatos de portales de datos abiertos en Europa (DCAT-AP) para conjuntos de datos y servicios espaciales. GeoDCAT-AP proporciona un vocabulario RDF y el correspondiente enlace de sintaxis RDF para la unión de los elementos de metadatos del perfil básico de la norma ISO 19115:2003 y los definidos en el marco de la Directiva INSPIRE.",
            license=default_license,
            license_id=license_id,
            rights=default_rights,
            media_type = "https://www.iana.org/assignments/media-types/application/rdf+xml",
            language=language,
            conformance=["https://semiceu.github.io/GeoDCAT-AP/releases/2.0.0/"]
        ))  

        # TODO: Add INSPIRE ISO19139 Metadata distribution
        '''
        Sería interesante generar metadatos INSPIRE, tal vez con un endpoint de pycsw y enlazarlos en el propio metadato de Geoserver, tal y como se hace ya con los metadatos recolectados de endpoint CSW.

        dataset.add_distribution(Distribution(
            url=self.get_csw_url() + "?service=CSW&version=2.0.2&request=GetRecordById&id=" + name + "&elementsetname=full&outputSchema=http://www.isotc211.org/2005/gmd",
            name="Metadatos INSPIRE ISO-19139",
            format="XML",
            media_type = "https://www.iana.org/assignments/media-types/application/xml",
            description="Metadatos para los conjuntos de datos y servicios INSPIRE en ISO/TS 19139 (https://inspire.ec.europa.eu/id/document/tg/metadata-iso19139).",
            license=default_license,
            license_id=license_id,
            rights=default_rights,
            language=language,
            modified=modified_date,
            issued=issued_date,
            conformance= (",".join({
                "https://inspire.ec.europa.eu/documents/inspire-metadata-regulation",
                "https://inspire.ec.europa.eu/documents/commission-regulation-eu-no-13122014-10-december-2014-amending-regulation-eu-no-10892010-0"
                }))
            )) 
            )) 
        '''

        # include default keywords
        keywords = []
        themes = []
        keywords_uri = []
        if default_keywords is not None:
            for k in default_keywords:
                if '/theme/' in k["uri"]:
                    # INSPIRE Theme
                    keywords.append({'name': k["name"].lower()})
                    themes.append(k.attrs['xlink:href'])
                else: 
                    # Keyword
                    keywords.append({'name': k["name"].lower()})
                keywords_uri.append(k["uri"])

        # include keywords extracted from WMS layer. Only keywords in WMS layer are retrieved by owslib
        if wms_layer_info is not None and wms_layer_info.keywords is not None:
            for k in wms_layer_info.keywords:
                if k not in wms_layer_info.title and "wms" not in k and "wfs" not in k and "wcs" not in k:
                    keywords.append({
                        'name': k.lower()})
        # Insert inspireid_theme (default) as theme/keyword
        keywords.append({'name': inspireid_theme.lower()})
        keywords_uri.append("https://inspire.ec.europa.eu/theme/" + inspireid_theme.lower())                
        themes = [inspireid_theme]

        self.set_themes(dataset, list(set(themes)))
        dataset.set_keywords(keywords)
        self.set_keywords_uri(dataset, list(set(keywords_uri)))

        return dataset

    def get_dataset(self, name, ckan_site_url, groups, organization_name, default_dcat_info, default_keywords, default_license, license_id, inspireid_theme, inspireid_nutscode, inspireid_versionid, workspaces, default_provenance, dir3_soup, wms=None, wfs=None, wcs=None, wmts=None):
        """
        Gets Dataset from WMS service. In case the layer is published either as WFS, WCS or WMTS layer, an additional distribution is included

        :param name: Dataset name to retrieve
        :param ckan_site_url: CKAN site url
        :param groups: Name of CKAN groups
        :param organization_name: Owner organization name
        :param default_dcat_info: Default publisher info
        :param default_keywords: Default keyword
        :param default_license: Default distribution license
        :param workspace: Geoserver workspace
        :param default_provenance: Default provenance statement
        :param wms: Should not be filled, inferred from url. It will only be filled by getAllDatasets to reuse the same connection
        :param wfs: Should not be filled, inferred from url. It will only be filled by getAllDatasets to reuse the same connection
        :param wcs: Should not be filled, inferred from url. It will only be filled by getAllDatasets to reuse the same
        connection
        :param wmts: Should not be filled, inferred from url. It will only be filled by getAllDatasets to reuse the same connection

        :return: Dataset object
        """
        # CONNECT
        if wms is None:
            wms = self.connect_wms()

        if wfs is None:
            wfs = self.connect_wfs()

        if wcs is None:
            wcs = self.connect_wcs()

        if wmts is None:
            wmts = self.connect_wmts()

        # Metadata info
        # Set UUID
        uuid_identifier = str(uuid.uuid1())

        ## TODO:Usar UUID en vez de name | OGC name: WMS layer name ({workspace}_{layername}) 
        ckan_name = self.get_ckan_name(name, organization_name)

        layer_info = wfs.contents[name]
        if name in wmts.contents:
            wmts_layer_info = wmts.contents[name]
        if name in wfs.contents:
            json_info = wfs.contents[name]
        dataset = Dataset(uuid_identifier, ckan_name, organization_name, license_id)

        # Set basic info of MD 
        self.set_title(dataset, layer_info.title, name)
        if layer_info.abstract is not None:
            dataset.set_description(layer_info.abstract)
        else:
            dataset.set_description(default_provenance)

        # Set UUID (identifier)
        dataset.set_identifier(uuid_identifier)

        # Set CKAN groups
        ckan_groups = []
        if groups is not None:
            for g in groups:
                ckan_groups.append({'name': g.lower()})
        dataset.set_groups(ckan_groups)

        # Set inspireId (identifier)
        inspire_id = ".".join(filter(None,[inspireid_nutscode, inspireid_theme, name.replace(':', '.'), inspireid_versionid])).upper()
        dataset.set_alternate_identifier(inspire_id.upper())  # create inspireid

        # dcat_type (dataset/series)        
        is_series = False
        if layer_info is not None:
            is_series = layer_info.timepositions is not None
        if is_series:
            is_series = len(layer_info.timepositions) != 0
        if is_series:
            dataset.set_resource_type("https://inspire.ec.europa.eu/metadata-codelist/ResourceType/series")
        else:
            dataset.set_resource_type("https://inspire.ec.europa.eu/metadata-codelist/ResourceType/dataset")

        # Set SpatialRepresentationType
        dataset.set_representation_type("https://inspire.ec.europa.eu/metadata-codelist/SpatialRepresentationType/vector")

        # Set access rights (Dataset)
        default_rights = "https://inspire.ec.europa.eu/metadata-codelist/LimitationsOnPublicAccess/noLimitations"
        dataset.set_access_rights(default_rights)

        # Set language
        language = "https://publications.europa.eu/resource/authority/language/SPA"
        dataset.set_languages(language)

        # Set SpatialResolutionInMeters
        try: 
            dataset.set_spatial_resolution_in_meters(layer_info.denominators[0])
        except:
            pass

        # Set theme
        themes = [inspireid_theme]
        self.set_themes(dataset, themes)

        # Set spatial coverage
        if layer_info is not None:
            bb = layer_info.boundingBoxWGS84
            if bb is not None:
                self.set_bounding_box(dataset, bb)
        else:
            if layer_info.boundingBox is not None:
                self.set_bounding_box_from_bounding_box(dataset, layer_info.boundingBox)
        
        # Set temporal coverage (only series)
        if is_series:
            time_extent = layer_info.timepositions[0].split(",")
            for t in range(len(time_extent)):
                time_extent[t] = time_extent[t].split("/")
            dataset.set_temporal_start(time_extent[0][0])
            try:
                dataset.set_temporal_end(time_extent[len(time_extent) - 1][1])
            except IndexError:
                dataset.set_temporal_end(time_extent[len(time_extent) - 1][0])

        # Set provenance (INSPIRE Lineage)
        dataset.set_provenance(default_provenance)

        # Set source (INSPIRE quality) & lineage_source (INSPIRE Lineage sources)
        dataset.set_source(None)
        dataset.set_lineage_source([])

        # Set process steps (INSPIRE quality)
        dataset.set_lineage_process_steps([])

        # Set conformance (INSPIRE regulation + EPSG)
        try:
            self.set_conformance(dataset, epsg_text=layer_info.crsOptions[0])
        except:
            self.set_conformance(dataset)

        # Point of contact (Metadata) and Responsible Party (Resource)
        ## contact_name (pointOfContact Metadata)
        dataset.set_contact_name(wms.provider.contact.name)
        dataset.set_contact_email(wms.provider.contact.email.lower())
        dataset.set_contact_url(default_dcat_info["publisher_url"])
        dataset.set_contact_uri(default_dcat_info["contact_uri"])

        ## maintainer (pointOfContact Resource)
        dataset.set_maintainer_name(wms.provider.contact.name)
        dataset.set_maintainer_email(wms.provider.contact.email.lower())
        dataset.set_maintainer_uri(default_dcat_info["maintainer_uri"]) 

        # Responsible Party (Resource publisher)
        publisher_uri = (ckan_site_url + "/organization/" + organization_name).lower()
        self.set_publisher_info(dataset, publisher_uri, default_dcat_info)

        # Set license
        dataset.set_license(default_license)

        # Add distributions (WMS, WFS, WMTS & GeoJSON)
        dataset.set_distributions([])
        if layer_info is not None:
            dataset.add_distribution(Distribution(
                url=self.get_wms_url() + "?request=GetCapabilities" + "#" + name, # http://host/geoserver/ows?service=wms&request=GetCapabilities#workspace:layer
                name="Web Map Service",
                format="WMS",
                description="Servicio que proporciona visualización de mapas (WMS) conforme a las especificaciones del Open Geospatial Consortium (https://www.ogc.org/standards/wms).",
                license=default_license,
                license_id=license_id,
                rights=default_rights,
                language=language,
                conformance=["http://www.opengeospatial.org/standards/wms"]
            ))  # WMS
        dataset.add_distribution(Distribution(
            url=self.get_wfs_url() + "?request=GetCapabilities" + "#" + name, # http://host/geoserver/ows?service=wfs&request=GetCapabilities#workspace:layer
            name="Web Feature Service",
            format="WFS",
            description="Servicio que proporciona descarga de fenómenos discretos (WFS) conforme a las especificaciones del Open Geospatial Consortium (https://www.ogc.org/standards/wfs).",
            license=default_license,
            license_id=license_id,
            rights=default_rights,
            language=language,
            conformance=["http://www.opengeospatial.org/standards/wfs"]
        )) # WFS
        if wmts_layer_info is not None:
            dataset.add_distribution(Distribution(
                url=self.get_wmts_url() + "?request=GetCapabilities", # http://host/geoserver/gwc/service/wmts?REQUEST=getcapabilities
                name="Web Map Tile Service",
                format="WMTS",
                description="Servicio que proporciona visualización de mapas teselados (WMTS) conforme a las especificaciones del Open Geospatial Consortium (https://www.ogc.org/standards/wmts).",
                license=default_license,
                license_id=license_id,
                rights=default_rights,
                language=language,
                conformance=["http://www.opengeospatial.org/standards/wmts"]
            ))  # WMTS
        if json_info is not None:
            workspace= layer_info.id.split(':')[0]
            layername= layer_info.id.split(':')[1]
            dataset.add_distribution(Distribution(
                url=self.get_wfs_url().replace('geoserver/ows', 'geoserver/' + workspace.lower() + '/ows') + '&version=1.0.0&request=GetFeature&typeName=' + layername.lower() + '&outputFormat=application/json&maxFeatures=100', # http://host/geoserver/mnr/ows?service=WFS&version=1.0.0&request=GetFeature&typeName=mnr:nuts&outputFormat=application/json
                name="GeoJSON Service",
                media_type = "https://www.iana.org/assignments/media-types/application/geo+json",
                format="GeoJSON",
                description="Formato para el intercambio de datos espaciales basados en la notación JSON (GeoJSON) conforme a la especificación  de la Internet Engineering Task Force (https://www.rfc-editor.org/rfc/rfc7946).",
                license=default_license,
                license_id=license_id,
                rights=default_rights,
                language=language,
                conformance=["http://www.opengeospatial.org/standards/wfs"]
            ))  # GeoJSON


        # Add GeoDCAT-AP Metadata distribution
        dataset.add_distribution(Distribution(
            url=ckan_site_url + "/dataset/" + ckan_name + ".rdf", # http://localhost:5000/dataset/{ckan_name}.rdf
            name="Metadatos GeoDCAT-AP 2.0",
            format="RDF",
            description="Metadatos conforme a la extensión GeoDCAT-AP (https://semiceu.github.io/GeoDCAT-AP/releases/) del perfil de metadatos de portales de datos abiertos en Europa (DCAT-AP) para conjuntos de datos y servicios espaciales. GeoDCAT-AP proporciona un vocabulario RDF y el correspondiente enlace de sintaxis RDF para la unión de los elementos de metadatos del perfil básico de la norma ISO 19115:2003 y los definidos en el marco de la Directiva INSPIRE.",
            license=default_license,
            license_id=license_id,
            rights=default_rights,
            media_type = "https://www.iana.org/assignments/media-types/application/rdf+xml",
            language=language,
            conformance=["https://semiceu.github.io/GeoDCAT-AP/releases/2.0.0/"]
        ))  

        # TODO: Add INSPIRE ISO19139 Metadata distribution
        '''
        Sería interesante generar metadatos INSPIRE, tal vez con un endpoint de pycsw y enlazarlos en el propio metadato de Geoserver, tal y como se hace ya con los metadatos recolectados de endpoint CSW.

        dataset.add_distribution(Distribution(
            url=self.get_csw_url() + "?service=CSW&version=2.0.2&request=GetRecordById&id=" + name + "&elementsetname=full&outputSchema=http://www.isotc211.org/2005/gmd",
            name="Metadatos INSPIRE ISO-19139",
            format="XML",
            media_type = "https://www.iana.org/assignments/media-types/application/xml",
            description="Metadatos para los conjuntos de datos y servicios INSPIRE en ISO/TS 19139 (https://inspire.ec.europa.eu/id/document/tg/metadata-iso19139).",
            license=default_license,
            license_id=license_id,
            rights=default_rights,
            language=language,
            modified=modified_date,
            issued=issued_date,
            conformance= (",".join({
                "https://inspire.ec.europa.eu/documents/inspire-metadata-regulation",
                "https://inspire.ec.europa.eu/documents/commission-regulation-eu-no-13122014-10-december-2014-amending-regulation-eu-no-10892010-0"
                }))
            )) 
            )) 
        '''

        # include default keywords
        keywords = []
        themes = []
        keywords_uri = []
        if default_keywords is not None:
            for k in default_keywords:
                if '/theme/' in k["uri"]:
                    # INSPIRE Theme
                    keywords.append({'name': k["name"].lower()})
                    themes.append(k.attrs['xlink:href'])
                else: 
                    # Keyword
                    keywords.append({'name': k["name"].lower()})
                keywords_uri.append(k["uri"])

        # include keywords extracted from WMS layer. Only keywords in WMS layer are retrieved by owslib
        if layer_info is not None and layer_info.keywords is not None:
            for k in layer_info.keywords:
                if k not in layer_info.title and "wms" not in k and "wfs" not in k and "wcs" not in k:
                    keywords.append({
                        'name': k.lower()})
        # Insert inspireid_theme (default) as theme/keyword
        keywords.append({'name': inspireid_theme.lower()})
        keywords_uri.append("https://inspire.ec.europa.eu/theme/" + inspireid_theme.lower())                
        themes = [inspireid_theme]

        self.set_themes(dataset, list(set(themes)))
        dataset.set_keywords(keywords)
        self.set_keywords_uri(dataset, list(set(keywords_uri)))

        return dataset


    def get_all_datasets(self, harvest_type, ckan_site_url=None, harvest_url=None, groups=None, organization_name=None, default_dcat_info=None, authorization_key=None, workspaces=None, default_provenance=None, default_keywords=None, default_license="https://creativecommons.org/licenses/by/4.0/", license_id="cc-by", inspireid_theme="HB", inspireid_nutscode='ES', inspireid_versionid=None, default_bbox='{"type": "Polygon", "coordinates": [[[-19.0, 27.0], [4.57, 27.0], [4.57, 44.04], [-19.0, 44.04], [-19.0, 27.0]]]}', constraints=None):
        """
        Gets All the Datasets from Server
        :return: Datasets objects array
        """
        # Obtain dir3_info bs4 soup
        try:
            dir3_url = 'https://datos.gob.es/es/recurso/sector-publico/org/Organismo'
            dir3_soup = BeautifulSoup(requests.get(dir3_url).text, 
                        'html.parser')
        except:
            dir3_soup = None

        if harvest_type =="csw":
            # CONNECT
            csw = self.connect_csw()
            datasets = []
            csw_constraints = []
            csw_mails = [mail.lower().replace(' ','') for mail in constraints["mails"]]
            csw_records = []
            for c in constraints["keywords"]:
                csw_constraints.append(PropertyIsLike("csw:anyText", c))

            # CSW records
            csw_records = self.get_csw_records(csw_constraints, csw_mails, csw_records)
            for md_record in csw_records:
                self.record = md_record
                datasets.append(self.get_csw_record_dataset(ckan_site_url=ckan_site_url, csw_url=self.url, uuid_identifier=self.record.identifier,
                groups=groups, organization_name=organization_name, default_dcat_info=default_dcat_info, default_keywords=default_keywords,default_license=default_license, license_id=license_id, inspireid_theme=inspireid_theme, default_bbox=default_bbox, dir3_soup=dir3_soup))

            return datasets

        elif harvest_type =="ogc": 
            # CONNECT
            wms = self.connect_wms()
            wfs = self.connect_wfs()
            wcs = self.connect_wcs()
            wmts = self.connect_wmts()
            datasets = []

            # OGC records
            for coverage in wcs.contents:
                datasets.append(self.get_coverage_dataset(coverage, ckan_site_url, groups, organization_name, default_dcat_info, default_keywords, default_license, license_id, inspireid_theme, inspireid_nutscode, inspireid_versionid, workspaces,default_provenance, dir3_soup, wms, wcs, wmts))
            for feature in wfs.contents:
                datasets.append(self.get_feature_dataset(feature, ckan_site_url, groups, organization_name, default_dcat_info, default_keywords, default_license, license_id, inspireid_theme, inspireid_nutscode, inspireid_versionid, workspaces,default_provenance, dir3_soup, wms, wfs, wmts))
            return datasets

        # return [self.getDataset(i, organization_name, wms, wfs, wcs) for i in wms.contents]