"""
This module creates a new IAM user and Redshift cluster using boto3.

It is required to have a config file called 'dwh.cfg' (or rename in the code if
you want.)

Author: M. Sanchez-Ayala 04/09/2020
"""
import boto3
from botocore.exceptions import ClientError
import json
import configparser


def create_aws_clients(clients, region, key, secret):
    """
    Returns
    -------
    client_objs: a list of boto3 client objects for each of the `clients`
    strings that were passed through as arguments. They are returned in the same
    order in which they were given.

    Parameters
    -----------
    clients: either a string or a list of strings in
        ['iam', 'ec2', 's3', 'redshift']
    region: a string indicating the geographic region as denoted by AWS.
        You can find this on your AWS console in the upper right.

    key: AWS IAM user access key ID

    secret: AWS IAM user secret access ID
    """
    if type(clients) == str:
        clients = [clients]

    client_objs = [
        boto3.client(
            client,
            region_name = region,
            aws_access_key_id = key,
            aws_secret_access_key = secret
        )
        for client in clients
    ]

    return client_objs

def create_iam_role(iam_client, iam_role_name):
    """
    Creates a new IAM role for the current session.

    Returns:
    --------
    role_arn: the arn for the newly created IAM role.

    Parameters:
    -----------
    iam_client: boto3 IAM client
    iam_role_name: the IAM role name specified in the config file.
    """
    #1.1 Create the role,
    try:
        print("1.1 Creating a new IAM Role")
        dwhRole = iam_client.create_role(
            Path = '/',
            RoleName = iam_role_name,
            Description = "Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument = json.dumps({
                'Statement': [{
                    'Action': 'sts:AssumeRole',
                    'Effect': 'Allow',
                    'Principal': {
                        'Service': 'redshift.amazonaws.com'
                    }
                }],
                'Version': '2012-10-17'
            })
        )
    except Exception as e:
        print(e)

    print("1.2 Attaching Policy")

    iam_client.attach_role_policy(
        RoleName=iam_role_name,
        PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
    )['ResponseMetadata']['HTTPStatusCode']

    print("1.3 Get the IAM role ARN")
    role_arn = iam_client.get_role(RoleName=iam_role_name)['Role']['Arn']
    print('Successful IAM role creation')

    return role_arn

def create_redshift_cluster(
    redshift_client, cluster_type, node_type, num_nodes, db,
    cluster_id, db_user, db_password, role_arn
):
    """
    Creates a redshift cluster given the specifications of the config file.
    """
    try:
        response = redshift_client.create_cluster(
            #HW
            ClusterType = cluster_type,
            NodeType = node_type,
            NumberOfNodes = int(num_nodes),

            #Identifiers & Credentials
            DBName = db,
            ClusterIdentifier = cluster_id,
            MasterUsername = db_user,
            MasterUserPassword = db_password,

            #Roles (for s3 access)
            IamRoles = [role_arn]
        )

        print('Successful Redshift Cluster Creation')

    except Exception as e:
        print(e)


def display_cluster_props(redshift_client, cluster_id):
    """
    Prints out cluster properties to confirm cluster creation.

    Parameters:
    -----------
    cluster_id: the cluster identifier.
    """
    # Print out properties
    myClusterProps = redshift_client.describe_clusters(ClusterIdentifier=cluster_id)['Clusters'][0]

    print(myClusterProps)


def main():
    """
    Wraps everything together
    """
    config = configparser.ConfigParser()
    config.read_file(open('dwh.cfg'))

    iam, redshift = create_aws_clients(
        ['iam', 'redshift'],
        'us-west-2',
        config['AWS']['KEY'],
        config['AWS']['SECRET']
    )
    role_arn = create_iam_role(iam, config['DWH']['DWH_IAM_ROLE_NAME'])

    create_redshift_cluster(
        redshift,
        config['DWH']['DWH_CLUSTER_TYPE'],
        config['DWH']['DWH_NODE_TYPE'],
        config['DWH']['DWH_NUM_NODES'],
        config['DWH']['DWH_DB'],
        config['DWH']['DWH_CLUSTER_IDENTIFIER'],
        config['DWH']['DWH_DB_USER'],
        config['DWH']['DWH_DB_PASSWORD'],
        role_arn
    )

    display_cluster_props(redshift, config['DWH']['DWH_CLUSTER_IDENTIFIER'])


if __name__ == '__main__':
    main()
