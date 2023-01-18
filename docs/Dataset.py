#!/usr/bin/env python3
## File: Dataset.py
## Coding: UTF-8
## Author: Manuel Ángel Jáñez García (mjanez@tragsa.es)
## Institution: Tragsatec
## Project: EIKOS
## Goal: The goal of this script is to provide the Class to represent metadata of a Dataset and convert into a data dictionary that is transferred through the CKAN API. Modified Dataset.py to harvest CSW endpoints from ogc_ckan.
## Parent: ogc_ckan/Dataset.py
""" Changelog:
    v2.0 - 30 Ago 2022: Upgraded with process parallelization, management of different harvester types (OGC, CSW) and with expansion possibilities.
    v1.0 - 16 Ago 2022: Create the first version (Compatible GeoDCAT-AP schema (ckanext-scheming))
"""
# Update the version when apply changes 
version = "2.0"

##~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~##
##               Dataset.py             ##
##~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~##

# Metadata of a Dataset and Distributions. CKAN Fields https://project-open-data.cio.gov/v1.1/metadata-resources/ // https://github.com/project-open-data/project-open-data.github.io/blob/f136070aa9fea595277f6ebd1cd66f57ff504dfd/v1.1/metadata-resources.md

## Import libraries   
import urllib.parse
import json


class Distribution:
    # Dataset Distribution Fields: https://github.com/project-open-data/project-open-data.github.io/blob/master/v1.1/metadata-resources.md#dataset-distribution-fields
    def __init__(self, url, name, format, issued=None, modified=None, media_type=None, license=None, license_id=None, rights=None, description=None, language=None, conformance=None):
        self.url = url
        self.name = name
        self.format = format
        self.media_type = media_type
        self.license = license
        self.license_id = license_id
        self.rights = rights
        self.description = description
        self.language = language
        self.issued = issued
        self.modified = modified
        self.conformance = conformance

    def set_url(self, url):
        self.url = url

    def set_name(self, name):
        self.name = name

    def set_format(self, format):
        self.format = format

    def set_media_type(self, media_type):
        self.media_type = media_type

    def set_license(self, license):
        self.license = license

    def set_license_id(self, license_id):
        self.license_id = license_id

    def set_rights(self, rights):
        self.rights = rights

    def set_description(self, description):
        self.description = description

    def set_language(self, language):
        self.language = language

    def set_issued(self, issued):
        self.issued = issued

    def set_modified(self, modified):
        self.modified = modified

    def set_conformance(self, conformance):
        self.conformance = conformance

    def to_dict(self):
        return {'url': self.url,
                'name': self.name,
                'format': self.format,
                'mimetype': self.media_type,
                'license': self.license,
                'license_id': self.license_id,
                'rights': self.rights,
                'description': self.description,
                'language': self.language,
                'issued': self.issued,
                'modified': self.modified,  
                'conforms_to': self.conformance,           
                }

class Dataset:
    # Dataset fields: https://github.com/project-open-data/project-open-data.github.io/blob/master/v1.1/metadata-resources.md#dataset-fields
    def __init__(self, ckan_id, name, owner_org, license_id):
        # initialization of default values
        self.name = name
        self.ckan_id = ckan_id
        self.publisher_uri = None
        self.publisher_name = None
        self.publisher_url = None
        self.publisher_email = None
        self.publisher_identifier = None
        self.publisher_type = None
        self.owner_org = owner_org
        self.groups = []
        # use http://<ckan_url>/api/action/organization_list to see the organization ids in your CKAN site
        self.license_id = license_id
        self.identifier = None
        self.title = None
        self.notes = None
        self.resourceType = None
        self.alternate_identifier = None
        self.access_rights= None
        self.representation_type = None
        self.version_notes = None
        self.spatial_resolution_in_meters = None
        self.languages = None
        self.theme = None
        self.keywords = []
        self.keywords_uri = []
        self.spatial = None
        self.temporalStart = None
        self.temporalEnd = None
        self.issued = None
        self.modified = None
        self.provenance = None
        self.lineage_source = []
        self.lineage_process_steps = []
        self.source = None
        self.conformance = []
        self.contact_uri = None
        self.contact_url = None
        self.contact_name = None
        self.contact_email = None
        self.maintainer_name = None
        self.maintainer_uri = None
        self.maintainer_email = None
        self.author_name = None
        self.author_uri = None
        self.author_email = None
        self.distributions = []
        self.license = None

    def set_name(self, name):
        self.name = name

    def set_ckan_id(self, ckan_id):
        self.ckan_id = ckan_id

    def set_groups(self, groups):
        self.groups = groups

    def set_publisher_uri(self, publisher_uri):
        self.publisher_uri = publisher_uri

    def set_publisher_name(self, publisher_name):
        self.publisher_name = publisher_name
    
    def set_publisher_url(self, publisher_url):
        self.publisher_url = publisher_url

    def set_publisher_email(self, publisher_email):
        self.publisher_email = publisher_email

    def set_publisher_identifier(self, publisher_identifier):
        self.publisher_identifier = publisher_identifier

    def set_publisher_type(self, publisher_type):
        self.publisher_type = publisher_type

    def set_identifier(self, identifier):
        self.identifier = identifier

    def set_title(self, title):
        self.title = title

    def set_description(self, notes):
        self.notes = notes

    def set_resource_type(self, resource_type):
        self.resourceType = resource_type

    def set_alternate_identifier(self, alternate_identifier):
        self.alternate_identifier = alternate_identifier

    def set_access_rights(self, access_rights):
        self.access_rights = access_rights

    def set_representation_type(self, representation_type):
        self.representation_type = representation_type

    def set_version_notes(self, version_notes):
        self.version_notes = version_notes

    def set_spatial_resolution_in_meters(self, spatial_resolution_in_meters):
        self.spatial_resolution_in_meters = spatial_resolution_in_meters

    def set_languages(self, languages):
        self.languages = languages

    def set_theme(self, theme):
        self.theme = theme

    def set_keywords(self, keywords):
        self.keywords = keywords

    def set_keywords_uri(self, keywords_uri):
        self.keywords_uri = keywords_uri

    def set_spatial(self, spatial):
        self.spatial = spatial

    def set_temporal_start(self, temporal_start):
        self.temporalStart = temporal_start

    def set_temporal_end(self, temporal_end):
        self.temporalEnd = temporal_end

    def set_issued(self, issued):
        self.issued = issued

    def set_modified(self, modified):
        self.modified = modified

    def set_provenance(self, provenance):
        self.provenance = provenance

    def set_lineage_source(self, lineage_source):
        self.lineage_source = lineage_source

    def set_lineage_process_steps(self, lineage_process_steps):
        self.lineage_process_steps = lineage_process_steps

    def set_source(self, source):
        self.source = source

    def set_conformance(self, conformance):
        self.conformance = conformance

    def set_contact_uri(self, contact_uri):
        self.contact_uri = contact_uri      

    def set_contact_url(self, contact_url):
        self.contact_url = contact_url   

    def set_contact_name(self, contact_name):
        self.contact_name = contact_name

    def set_contact_email(self, contact_email):
        self.contact_email = contact_email

    def set_maintainer_name(self, maintainer_name):
        self.maintainer_name = maintainer_name

    def set_maintainer_uri(self, maintainer_uri):
        self.maintainer_uri = maintainer_uri  

    def set_maintainer_email(self, maintainer_email):
        self.maintainer_email = maintainer_email

    def set_author_name(self, author_name):
        self.author_name = author_name

    def set_author_uri(self, author_uri):
        self.author_uri = author_uri    

    def set_author_email(self, author_email):
        self.author_email = author_email

    def set_distributions(self, distributions):
        self.distributions = distributions

    def add_distribution(self, distribution):
        self.distributions.append(distribution)

    def set_license(self, license):
        self.license = license

    def set_license_id(self, license_id):
        self.license_id = license_id

    def dataset_dict(self):
        '''    
        CKAN API 'package_create': https://docs.ckan.org/en/2.9/api/index.html#ckan.logic.action.create.package_create
            package_create(
                name = NULL,
                title = NULL,
                private = FALSE,
                author = NULL,
                author_email = NULL,
                maintainer = NULL,
                maintainer_email = NULL,
                license_id = NULL,
                notes = NULL,
                package_url = NULL,
                version = NULL,
                state = "active",
                type = NULL,
                resources = NULL,
                tags = NULL,
                extras = NULL,
                relationships_as_object = NULL,
                relationships_as_subject = NULL,
                groups = NULL,
                owner_org = NULL,
                url = get_default_url(),
                key = get_default_key(),
                as = "list",
                ...
                )
        '''

        # Put the details of the dataset we're going to create into a dict.
        dataset_dict = {
            'id': self.ckan_id,
            'name': self.name,
            'owner_org': self.owner_org,
            'groups': self.groups,
            'title': self.title,
            'notes': self.notes,
            'license_id': self.license_id,
            'tags': self.keywords,
            'tag_uri': self.keywords_uri,
            'dcat_type': self.resourceType,
            'representation_type': self.representation_type,
            'access_rights': self.access_rights,
            'alternate_identifier': self.alternate_identifier,
            'version_notes': self.version_notes,
            'spatial_resolution_in_meters': self.spatial_resolution_in_meters,
            'language': self.languages,
            'theme': self.theme,
            'identifier': self.identifier,
            'provenance': self.provenance,
            'lineage_source': self.lineage_source,
            'lineage_process_steps': self.lineage_process_steps,
            'source': self.source,
            'conforms_to': self.conformance,
            'spatial': self.spatial,
            'publisher_uri': self.publisher_uri,
            'publisher_name': self.publisher_name,
            'publisher_url': self.publisher_url,
            'publisher_email': self.publisher_email,
            'publisher_identifier': self.publisher_identifier,
            'publisher_type': self.publisher_type,
            'contact_uri': self.contact_uri,
            'contact_url': self.contact_url,
            'contact_name': self.contact_name,
            'contact_email': self.contact_email,
            'maintainer': self.maintainer_name,
            'maintainer_uri': self.maintainer_uri,
            'maintainer_email': self.maintainer_email,
            'author': self.author_name,
            'author_uri': self.author_uri,
            'author_email': self.author_email,
            'extras': [
            {'key': 'issued', 'value': self.issued},
            {'key': 'modified', 'value': self.modified},
        ],
            'resources': [i.to_dict() for i in self.distributions]
        }
        if self.temporalStart:
            dataset_dict["extras"].append({'key': 'temporal_start', 'value': self.temporalStart})
        if self.temporalEnd:
            dataset_dict["extras"].append({'key': 'temporal_end', 'value': self.temporalEnd})

        return dataset_dict

    def generate_data(self):
        dataset_dict = self.dataset_dict()
        # Use the json module to dump the dictionary to a string for posting.
        quoted_data = urllib.parse.quote(json.dumps(dataset_dict))
        byte_data = quoted_data.encode('utf-8')
        return byte_data