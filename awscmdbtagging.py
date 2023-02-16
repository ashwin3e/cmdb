import json
import botocore
from botocore.exceptions import ClientError
import boto3
import urllib.request
import datetime
from dateutil.tz import tzlocal
import logging
import re
import time
class AssumeRoleFailure(Exception): pass
logging.getLogger().setLevel(logging.INFO)
logger = logging.getLogger()
cmdb_cache: dict = {}
def lambda_handler(event, context):  
    logger.info(event)
    account_id = event["account_id"]
    account_name = event["account_name"]
    account_write = event["account_write"]
    services = {}
    report = {}
    account_arn = "arn:aws:iam::" + account_id + ":role/cmdb_tagger_access"
    logger.info("Account: " + account_id)
    # Assume cross-account role
    # The "session" object will be used for all boto3 calls.
    try:
        session = assumed_role_session(account_arn)
    except:
        raise AssumeRoleFailure(account_arn)
    
    tagger_client = session.client('resourcegroupstaggingapi')
    
    # Get all distinct tag keys
    response = tagger_client.get_tag_keys()
    
    all_keys = response["TagKeys"]
    ref_keys = []
    
    # Filter keys for CMDB ref matches
    for key in all_keys:
        if re.search('cmdb[-|_]ref', key.lower().strip(), re.IGNORECASE):
            ref_keys.append(key)
    
    logger.info("cmdb-ref variations: " + json.dumps(ref_keys))
    
    for ref_key in ref_keys:
        if ref_key == "cmdb-ref":
            cmdb_key_format = False
        else:
            cmdb_key_format = True
        
        # Get a list of all cmdb-ref values.
        response = tagger_client.get_tag_values(
            Key=ref_key
        )
        
        ref_list = response["TagValues"]
        
        # Iterate through each cmdb-ref value (i.e. each CMDB system/environment)
        for ref in ref_list:
            logger.info("CMDB Ref: " + str(ref))
            
            cmdb_ref = ref
            
            # Check cmdb-ref value format.
            if not re.search('^\d{7}', cmdb_ref, re.IGNORECASE):
                cmdb_value_format = True
                cmdb_ref = re.sub("[^0-9]", "", cmdb_ref)[-7:]
                cmdb_ref = cmdb_ref.zfill(7)
            else:
                cmdb_value_format = False
            
            services[cmdb_ref] = {}
            
            # Force write is used for testing only.
            if "force_write" in event:
                if event["force_write"]:
                    cmdb_value_format = True
            
            # Get details from the CMDB database
            cmdb_details = get_cmdb_details(cmdb_ref)
            
            if "error" in cmdb_details:
                logger.warning("Skipping cmdb-ref: " + cmdb_ref)
                report[account_id + "-" + cmdb_ref] = {}
                report[account_id + "-" + cmdb_ref]["account_id"] = account_id
                report[account_id + "-" + cmdb_ref]["cmdb-ref"] = cmdb_ref
                report[account_id + "-" + cmdb_ref]["status"] = "failed"
                report[account_id + "-" + cmdb_ref]["comment"] = "invlaid cmdb-ref"
            else:
                # Get list of resource with the current cmdb-ref
                cmdb_details["arns"] = []
                
                response = tagger_client.get_resources(
                    ResourcesPerPage=100,
                    TagFilters=[
                        {
                            'Key': ref_key,
                            'Values': [ref]
                        }
                    ])
                resource_list = response['ResourceTagMappingList']
                    
                while 'PaginationToken' in response and response['PaginationToken']:
                    token = response['PaginationToken']
                    response = tagger_client.get_resources(
                        ResourcesPerPage=50,
                        PaginationToken=token,
                        TagFilters=[
                            {
                                'Key': ref_key,
                                'Values': [ref]
                            }
                        ])
                    resource_list += response['ResourceTagMappingList']
                
                logger.info("Resource count: " + str(len(resource_list)))
                
                # Iterate through resources returned
                for resource in resource_list:
                    resource_arn = resource["ResourceARN"]
                    
                    # If the cmdb-ref format is incorrect tags will be written by default.
                    if cmdb_value_format:
                        write_tags = True
                    else:
                        write_tags = False
                    
                    # Iterate through the resource tags to check cmdb-name and cmdb-environment
                    for tag in resource["Tags"]:
                        if tag["Key"] == "cmdb-name":
                            if tag["Value"] != cmdb_details["cmdb-name"]:
                                write_tags = True
                        elif tag["Key"] == "cmdb-environment":
                            if tag["Value"] != cmdb_details["cmdb-environment"]:
                                write_tags = True
                    
                    # If the tags need to be updated, add resource ARN to list
                    if write_tags:
                        cmdb_details["arns"].append(resource["ResourceARN"])
            
                    # Finish service based on ARN and add to list for stats
                    service = re.search('arn:aws:(\w*):.*', resource["ResourceARN"], re.IGNORECASE).group(1)
                    try:
                        services[cmdb_ref][service] += 1
                    except:
                        services[cmdb_ref][service] = 1
                
                # If account is writable and tags need to be updated, write tags
                if write_tags and account_write:
                    tag_resource(session, cmdb_details, cmdb_key_format, ref_key)
        
    # Respond with details of account and resources found.
    response = {
        "account_id": account_id,
        "account_name": account_name,
        "cmdb-ref-values": ref_list,
        "services": services
    }
    
    logger.info(response)
    
    return response

# Reformat JSON object, needed for tagging EC2 instances and others.
def reformat_tags(tags_list):
    return_tags = []
    for key in tags_list:
            tag = {}
            tag["Key"] = key
            tag["Value"] = tags_list[key]
            return_tags.append(tag)
    
    return return_tags

# Write tags to resources
def tag_resource(session, cmdb_details, cmdb_key_format, ref_key):

    tagger_client = session.client('resourcegroupstaggingapi')
    
    tags_list = {}
    tags_list["cmdb-ref"] = cmdb_details["cmdb-ref"]
    tags_list["cmdb-environment"] = cmdb_details["cmdb-environment"]
    tags_list["cmdb-name"] = cmdb_details["cmdb-name"]
    
    # logger.info("cmdb details: " + json.dumps(cmdb_details["arns"]))
    
    # List of services that cannot be tagged using the resource groups tagging API.
    arn_expections = []
    exceptions = ["s3", "ec2"]
    
    # Remove resource ARN that cannot be tagging using the resource groups tagging API.
    for arn in cmdb_details["arns"]:
        service = re.search('arn:aws:(\w*):.*', arn, re.IGNORECASE).group(1)
        
        if service in exceptions:
            arn_expections.append(arn)
            cmdb_details["arns"].remove(arn)
    
    # Tag resources supported by the resource groups tagging API. All resources are tagged with a single API call
    if len(cmdb_details["arns"]) > 0:
        # Tag resources
        if len(cmdb_details["arns"]) < 21:
            try:
                response = tagger_client.tag_resources(ResourceARNList=cmdb_details["arns"], Tags=tags_list)
            except(ValueError, TypeError):
                logger.warning(TypeError, ValueError)
            
            # Untag incorrect cmdb-ref key formatted tags
            if cmdb_key_format:
                try:
                    response = tagger_client.untag_resources(ResourceARNList=cmdb_details["arns"], TagKeys=[ref_key])
                except(ValueError, TypeError):
                    logger.warning(TypeError, ValueError)
        else:
            arn_count = len(cmdb_details["arns"])
            
            for arn_count_start in range(0, len(cmdb_details["arns"]), 20):
                arn_count_end = arn_count - arn_count_start
                if arn_count_end > 20:
                    arn_count_end = arn_count_start + 20
                else:
                    arn_count_end = arn_count
                
                try:
                    response = tagger_client.tag_resources(ResourceARNList=cmdb_details["arns"][arn_count_start:arn_count_end], Tags=tags_list)
                except(ValueError, TypeError):
                    logger.warning(TypeError, ValueError)
                
                # Untag incorrect cmdb-ref key formatted tags
                if cmdb_key_format:
                    try:
                        response = tagger_client.untag_resources(ResourceARNList=cmdb_details["arns"][arn_count_start:arn_count_end], TagKeys=[ref_key])
                    except(ValueError, TypeError):
                        logger.warning(TypeError, ValueError)
        
    
    # Iterate through any resources not supported by the resource groups tagging API.
    for resource_arn in arn_expections:
        service = re.search('arn:aws:(\w*):.*', resource_arn, re.IGNORECASE).group(1)
        
        if service == "s3":
            bucket_name = re.search('arn:aws:s3:::(.*)', resource_arn, re.IGNORECASE).group(1)
            
            s3_client = session.client('s3')
            
            existing_tags = s3_client.get_bucket_tagging(Bucket=bucket_name)["TagSet"]
            
            s3_tags_temp = {}
            
            for tag_pair in existing_tags:
                s3_tags_temp[tag_pair["Key"]] = tag_pair["Value"]
            
            for key, value in tags_list.items():
                s3_tags_temp[key] = value
            
            if cmdb_key_format:
                s3_tags_temp.pop(ref_key)
            
            s3_tags = reformat_tags(s3_tags_temp)
            
            try:
                response = s3_client.put_bucket_tagging(Bucket=bucket_name, Tagging={"TagSet":s3_tags})
            except(ValueError, TypeError):
                logger.warning(TypeError, ValueError)
            
        elif service == "ec2":
            ec2_resource = session.resource('ec2')
            ec2_client = session.client('ec2')
            
            ec2_tags = reformat_tags(tags_list)
    
            resource_type = re.search('arn:aws:ec2:.*:(.*)\/.*', resource_arn, re.IGNORECASE).group(1)
            instance = re.search('arn:aws:ec2:.*:.*\/(.*)', resource_arn, re.IGNORECASE).group(1)
            
            instance_exists = False
            
            for check_instance in ec2_resource.instances.all():
                if check_instance.id == instance:
                    instance_exists = True
            
            if instance_exists:
                try:
                    response = ec2_client.create_tags(Resources=[instance], Tags=ec2_tags)
                except (RuntimeError, TypeError, NameError):
                    logger.info(RuntimeError, TypeError, ValueError)
                    pass
                
                if cmdb_key_format:
                    try:
                        ec2_instance = ec2_resource.Instance(instance)
                        response = ec2_instance.delete_tags(Resources=[instance], Tags=[{'Key': ref_key}]);
                    except(ValueError, TypeError):
                        logger.info(TypeError, ValueError)

        else:
            logger.warning("No tagging configured: " + resource_arn)
    
    return {"complete"}

def assumed_role_session(role_arn: str, base_session: botocore.session.Session = None):

    base_session = base_session or boto3.session.Session()._session
    fetcher = botocore.credentials.AssumeRoleCredentialFetcher(
        client_creator = base_session.create_client,
        source_credentials = base_session.get_credentials(),
        role_arn = role_arn,
        extra_args = {}
    )
    creds = botocore.credentials.DeferredRefreshableCredentials(
        method = 'assume-role',
        refresh_using = fetcher.fetch_credentials,
        time_fetcher = lambda: datetime.datetime.now(tzlocal())
    )
    botocore_session = botocore.session.Session()
    botocore_session._credentials = creds
    
    return boto3.Session(botocore_session = botocore_session)

# Retrieve username and password for service account to access CMDB database
def get_secret():

    secret_name = "CMDB/ServiceNowAPI"
    region_name = "eu-west-1"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        if e.response['Error']['Code'] == 'DecryptionFailureException':
            raise e
        elif e.response['Error']['Code'] == 'InternalServiceErrorException':
            raise e
        elif e.response['Error']['Code'] == 'InvalidParameterException':
            raise e
        elif e.response['Error']['Code'] == 'InvalidRequestException':
            raise e
        elif e.response['Error']['Code'] == 'ResourceNotFoundException':
            raise e
    else:
        if 'SecretString' in get_secret_value_response:
            secret = get_secret_value_response['SecretString']
        else:
            decoded_binary_secret = base64.b64decode(get_secret_value_response['SecretBinary'])
    
    return json.loads(secret)

# Call CMDB database and return results. Cache results in DynamoDB for 5 minutes.
def get_cmdb_details(cmdb_ref):
    
    logger.info("Get Details for: " + cmdb_ref)
    
    dynamodb = boto3.client('dynamodb')
    
    cache = dynamodb.get_item(
        TableName='cmdb-tagging-cache',
        Key={
            'cmdb-ref': {'S': cmdb_ref}
            }
        )
    
    if 'Item' in cache:
        cmdb_details = {}
        cmdb_details["cmdb-ref"] = cmdb_ref
        cmdb_details["cmdb-name"] = cache["Item"]["cmdb-name"]["S"]
        cmdb_details["cmdb-environment"] = cache["Item"]["cmdb-environment"]["S"]
        return cmdb_details
    else:
        service_account = get_secret()
    
        base_url = "https://icon.service-now.com/api/now/table/cmdb_ci_service_discovered?"
        query_format = "sysparm_display_value=value"
        query_fields = "&sysparm_fields=number,name,u_system_instance_type"
        query_filter = "&sysparm_query=number="
    
        query_url = base_url + query_format + query_fields + query_filter + "SNSVC" + cmdb_ref
    
        passman = urllib.request.HTTPPasswordMgrWithDefaultRealm()
        passman.add_password(None, query_url, service_account["Username"], service_account["Password"])
        authhandler = urllib.request.HTTPBasicAuthHandler(passman)
        opener = urllib.request.build_opener(authhandler)
        urllib.request.install_opener(opener)
        
        try:
            res = urllib.request.urlopen(query_url)
        except urllib.error.HTTPError as e:
            logger.info(e)
            
        res_body = res.read()
        results = json.loads(res_body.decode('utf-8'))
        
        logger.info("ServiceNow returned " + str(len(results["result"])) + " result(s)")
        
        if len(results["result"]) == 1:
            cmdb_details = {}
            cmdb_details["cmdb-ref"] = cmdb_ref
            cmdb_details["cmdb-name"] = results["result"][0]["name"]
            cmdb_details["cmdb-environment"] = results["result"][0]["u_system_instance_type"]
            
            ttl = str(3600 + int(time.time()))
            
            cache = dynamodb.put_item(
                TableName='cmdb-tagging-cache',
                Item={
                    'cmdb-ref': {'S': cmdb_details["cmdb-ref"]},
                    'cmdb-name': {'S': cmdb_details["cmdb-name"]},
                    'cmdb-environment': {'S': cmdb_details["cmdb-environment"]},
                    'ttl': {'N': ttl}
                    }
                )
            
            return cmdb_details
        else:
            cmdb_details = {}
            cmdb_details["error"] = "ServiceNow returned " + str(len(results["result"])) + " result(s)"
            
            return cmdb_details