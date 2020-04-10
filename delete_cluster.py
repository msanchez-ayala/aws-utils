"""
This module deletes the IAM user and Redshift cluster specified in the config
file using boto3.

It is required to have a config file called 'dwh.cfg' (or rename in the code if
you want.)

Author: M. Sanchez-Ayala 04/09/2020
"""
import boto3
import configparser
from create_cluster import create_aws_clients

def main():
    config = configparser.ConfigParser()
    config.read_file(open('dwh.cfg'))

    iam, redshift = create_aws_clients(
        'us-west-2',
        config['AWS']['KEY'],
        config['AWS']['SECRET']
    )

    # Delete the cluster
    redshift.delete_cluster(
        ClusterIdentifier = config['DWH']['DWH_CLUSTER_IDENTIFIER'],
        SkipFinalClusterSnapshot = True
    )

    # Delete the created resources
    iam.detach_role_policy(
        RoleName = config['DWH']['DWH_IAM_ROLE_NAME'],
        PolicyArn = "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
    )
    iam.delete_role(RoleName = config['DWH']['DWH_IAM_ROLE_NAME'])

    print('Cluster and IAM role deleted')


if __name__ == '__main__':
    main()
