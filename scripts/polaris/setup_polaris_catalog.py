import os
from polaris.catalog.api.iceberg_catalog_api import IcebergCatalogAPI
from polaris.catalog.api.iceberg_o_auth2_api import IcebergOAuth2API
from polaris.catalog.api_client import ApiClient as CatalogApiClient
from polaris.catalog.api_client import Configuration as CatalogApiClientConfiguration

# some of this is from https://github.com/apache/polaris/blob/e32ef89bb97642f2ac9a4db82252a4fcf7aa0039/getting-started/spark/notebooks/SparkPolaris.ipynb
polaris_credential = 'root:s3cr3t' # pragma: allowlist secret

client_id = os.getenv('POLARIS_ROOT_ID', '')
client_secret = os.getenv('POLARIS_ROOT_SECRET', '')

if client_id == '' or client_secret == '':
    Print("could not find polaris root id or polaris root secret")
client = CatalogApiClient(CatalogApiClientConfiguration(username=client_id,
                                                        password=client_secret,
                                                        host='http://polaris:8181/api/catalog'))

oauth_api = IcebergOAuth2API(client)
token = oauth_api.get_token(scope='PRINCIPAL_ROLE:ALL',
                            client_id=client_id,
                            client_secret=client_secret,
                            grant_type='client_credentials',
                            _headers={'realm': 'default-realm'})

# create a catalog
from polaris.management import *

client = ApiClient(Configuration(access_token=token.access_token,
                                 host='http://polaris:8181/api/management/v1'))
root_client = PolarisDefaultApi(client)

storage_conf = FileStorageConfigInfo(storage_type="FILE", allowed_locations=["file:///tmp"])
catalog_name = 'polaris_demo'
catalog = Catalog(name=catalog_name, type='INTERNAL', properties={"default-base-location": "file:///tmp/polaris/"},
                  storage_config_info=storage_conf)
catalog.storage_config_info = storage_conf
root_client.create_catalog(create_catalog_request=CreateCatalogRequest(catalog=catalog))
resp = root_client.get_catalog(catalog_name=catalog.name)


# UTILITY FUNCTIONS
# Creates a principal with the given name
def create_principal(api, principal_name):
    principal = Principal(name=principal_name, type="SERVICE")
    try:
        principal_result = api.create_principal(CreatePrincipalRequest(principal=principal))
        return principal_result
    except ApiException as e:
        if e.status == 409:
            return api.rotate_credentials(principal_name=principal_name)
        else:
            raise e

# Create a catalog role with the given name
def create_catalog_role(api, catalog, role_name):
    catalog_role = CatalogRole(name=role_name)
    try:
        api.create_catalog_role(catalog_name=catalog.name, create_catalog_role_request=CreateCatalogRoleRequest(catalog_role=catalog_role))
        return api.get_catalog_role(catalog_name=catalog.name, catalog_role_name=role_name)
    except ApiException as e:
        return api.get_catalog_role(catalog_name=catalog.name, catalog_role_name=role_name)
    else:
        raise e

# Create a principal role with the given name
def create_principal_role(api, role_name):
    principal_role = PrincipalRole(name=role_name)
    try:
        api.create_principal_role(CreatePrincipalRoleRequest(principal_role=principal_role))
        return api.get_principal_role(principal_role_name=role_name)
    except ApiException as e:
        return api.get_principal_role(principal_role_name=role_name)


# Create a bunch of new roles

# Create the engineer_principal
engineer_principal = create_principal(root_client, "collado")

# Create the principal role
engineer_role = create_principal_role(root_client, "engineer")

# Create the catalog role
manager_catalog_role = create_catalog_role(root_client, catalog, "manage_catalog")

# Grant the catalog role to the principal role
# All principals in the principal role have the catalog role's privileges
root_client.assign_catalog_role_to_principal_role(principal_role_name=engineer_role.name,
                                                  catalog_name=catalog.name,
                                                  grant_catalog_role_request=GrantCatalogRoleRequest(catalog_role=manager_catalog_role))

# Assign privileges to the catalog role
# Here, we grant CATALOG_MANAGE_CONTENT
root_client.add_grant_to_catalog_role(catalog.name, manager_catalog_role.name,
                                      AddGrantRequest(grant=CatalogGrant(catalog_name=catalog.name,
                                                                         type='catalog',
                                                                         privilege=CatalogPrivilege.CATALOG_MANAGE_CONTENT)))

# Assign the principal role to the principal
root_client.assign_principal_role(engineer_principal.principal.name, grant_principal_role_request=GrantPrincipalRoleRequest(principal_role=engineer_role))


