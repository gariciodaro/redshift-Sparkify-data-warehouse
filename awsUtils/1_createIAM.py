# -*- coding: utf-8 -*-
"""
Created on Sun Jun 12 2020

@author: gari.ciodaro.guerra

Create an IAM role on AWS to grand access 
to Redshift instances on your behalf.
Attach  policy to read-only to S3 services.
"""

import boto3
import configparser
config = configparser.ConfigParser()
config.read_file(open('./configuration/redshift.cfg'))


# load configuration parameters
KEY                    = config.get('AWS','KEY')
SECRET                 = config.get('AWS','SECRET')
DWH_IAM_ROLE_NAME      = config.get("DWH", "DWH_IAM_ROLE_NAME")

#initilizied iam client
iam = boto3.client('iam',
                    region_name='us-east-2',
                    aws_access_key_id=KEY,
                    aws_secret_access_key=SECRET)

if __name__=="__main__":
    try:
        print("1.1 Creating a new IAM Role") 
        dwhRole = iam.create_role(
            Path='/',
            RoleName=DWH_IAM_ROLE_NAME,
            Description = "Allows Redshift clusters to call\
                                                   AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps(
                {'Statement': [{'Action': 'sts:AssumeRole',
                   'Effect': 'Allow',
                   'Principal': {'Service': 'redshift.amazonaws.com'}}],
                 'Version': '2012-10-17'})
        )    
    except Exception as e:
        print(e)

    print("1.2 Attaching Policy")
    iam.attach_role_policy(RoleName=DWH_IAM_ROLE_NAME,
                     PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                    )['ResponseMetadata']['HTTPStatusCode']

    print("1.3 Get the IAM role ARN")
    roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']

    print(roleArn)