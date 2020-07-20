# -*- coding: utf-8 -*-
"""
Created on Sun Jun 12 2020

@author: gari.ciodaro.guerra

check the state of Redshift cluster.
If active returns the endpoint and ARN.
"""


import boto3
import configparser

config = configparser.ConfigParser()
config.read_file(open('./configuration/redshift.cfg'))

# load configuration parameters
KEY                    = config.get('AWS','KEY')
SECRET                 = config.get('AWS','SECRET')
DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")

#initilizied redshift client
redshift = boto3.client('redshift',
                        region_name='us-east-2',
                        aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET)

cluster_props=redshift.describe_clusters(
    ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]

if __name__=="__main__":
    if cluster_props["ClusterStatus"]=="available":
        print("DWH_ENDPOINT ",cluster_props['Endpoint']['Address'])
        print("DWH_ROLE_ARN ",cluster_props['IamRoles'][0]['IamRoleArn'])
    else:
        print(cluster_props["ClusterStatus"])
