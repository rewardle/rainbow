import boto3
from datetime import datetime

class DeploymentBucketParameterNotFound(Exception):
    pass

def epoch_in_milliseconds_from_timestamp(timestamp):
    return int((timestamp - datetime(1970, 1, 1)).total_seconds() * 1000)

class S3Helper(object):
    def __init__(self):
        self.s3_client = boto3.client('s3')

    @staticmethod
    def get_deployment_bucket_name_from_template_parameters(parameters, deployment_bucket_name_parameter):
        """
        Get deployment bucket name
        
        :type parameters: dict
        :type deploymentbucketnameparameter: string
        :rtype: string
        :return: deployment bucket name

        """
        deployment_bucket_name_parameter_value = ""
        if deployment_bucket_name_parameter in parameters:
            deployment_bucket_name_parameter_value= parameters[deployment_bucket_name_parameter]
        
        if len(deployment_bucket_name_parameter_value.strip()) == 0:
            raise DeploymentBucketParameterNotFound("Deployment Bucket Parameter '{0}' not found.".format(deployment_bucket_name_parameter))

        return deployment_bucket_name_parameter_value

    @staticmethod    
    def get_template_key(stackname):
        """
        upload stack 
        
        :type stackname: string
        :rtype: string
        :return: template object key 

        """
        deployment_tool = "cloudformation"
        utc_timestamp = datetime.utcnow()
        current_utc_timestamp = utc_timestamp.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
        epoch = epoch_in_milliseconds_from_timestamp(utc_timestamp)
        filename ="compiled-cloudformation-template.json"
        return "{0}/{1}/{2}-{3}/{4}".format(deployment_tool,stackname,str(epoch),current_utc_timestamp,filename)  

    def upload_template_to_s3_deployment_bucket(self, deployment_bucket_name, template_key, template):
        """
        upload stack cf temkplate to s3 
        :type deployment_bucket_name: string
        :type template_key: string
        :type template: string
        :rtype: string
        :return: pre-signed template key url for uploaded template

        """
        self.s3_client.put_object(Bucket=deployment_bucket_name, Key=template_key,Body=template)
        presigned_template_key_url = self.s3_client.generate_presigned_url(ClientMethod="get_object", Params={"Bucket": deployment_bucket_name, "Key": template_key})
        return presigned_template_key_url   