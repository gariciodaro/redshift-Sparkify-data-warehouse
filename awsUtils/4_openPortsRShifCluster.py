# -*- coding: utf-8 -*-
"""
Created on Sun Jun 12 2020

@author: gari.ciodaro.guerra

open ports of RedShift for TCP connections.
"""


import boto3
import configparser

config = configparser.ConfigParser()
config.read_file(open('./configuration/redshift.cfg'))

# load configuration parameters
KEY                    = config.get('AWS','KEY')
SECRET                 = config.get('AWS','SECRET')
DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
DWH_PORT               = config.get("DWH","DWH_PORT")

ec2 = boto3.resource('ec2',
    region_name='us-east-2',
    aws_access_key_id=KEY,
    aws_secret_access_key=SECRET)

#initilizied redshift client
redshift = boto3.client('redshift',
                        region_name='us-east-2',
                        aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET)

cluster_props=redshift.describe_clusters(
    ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]

if __name__=="__main__":

    try:
        vpc = ec2.Vpc(id=cluster_props['VpcId'])
        defaultSg = list(vpc.security_groups.all())[0]
        print(defaultSg)
        defaultSg.authorize_ingress(
            GroupName=defaultSg.group_name,
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(DWH_PORT),
            ToPort=int(DWH_PORT)
        )
    except Exception as e:
        print(e)