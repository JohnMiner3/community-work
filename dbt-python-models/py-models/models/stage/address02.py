import json
from azure.core.credentials import AzureKeyCredential
from azure.maps.search import MapsSearchClient
from pyspark.sql.functions import *

#
#  validate address function
#

def get_additional_address_info(api_key, raw_address):

    # create object
    azmaps = MapsSearchClient(credential=AzureKeyCredential(api_key))

    # initialize variables
    longitude = 0.0
    latitude = 0.0
    address = {'formattedAddress': '', 'postalCode': ''}

    # Perform geocoding
    try:
        retval1 = azmaps.get_geocoding(query=raw_address)

        if retval1.get('features'):
            coordinates = retval1['features'][0]['geometry']['coordinates']
            longitude, latitude = coordinates

    except Exception as e:
        pass

    # Reverse geocoding  
    try:
        retval2 = azmaps.get_reverse_geocoding(coordinates=coordinates)

        if retval2.get('features'):
            address = retval2['features'][0]['properties']['address']

    except Exception as e:
        pass

    # create dict 
    data = { "address": address['formattedAddress'], "postalCode": address['postalCode'], "longitude": longitude, "latitude": latitude }

    # return json string
    return json.dumps(data)


#
# Wrap the function and specify the return type
#

get_additional_address_info_udf = udf(get_additional_address_info, StringType())


#
#  python model function

def model(dbt, session):

    # get meta object
    meta = dbt.config.get("meta")
    
    # get api key  
    api_key = meta.get("maps_api_key")

    # get df from previous model
    in_df = dbt.ref("address01")

    # create full address
    in_df = in_df.withColumn('full_address', concat(col('address'), lit(', '), col('city'), lit(', '), col('state'), lit(' '), lpad(col('zip').cast("string"), 5, "0")))

    # call web svc
    in_df = in_df.withColumn("az_maps_info", get_additional_address_info_udf(lit(api_key), col('full_address')))

    # parse out latitude
    in_df = in_df.withColumn('latitude', get_json_object(in_df['az_maps_info'], '$.latitude').cast("double"))

    # parse out longitude
    in_df = in_df.withColumn('longitude', get_json_object(in_df['az_maps_info'], '$.longitude').cast("double"))

    # parse out postal code
    in_df = in_df.withColumn('postal_code', get_json_object(in_df['az_maps_info'], '$.postalCode').cast("string"))

    # final dataframe
    return in_df


