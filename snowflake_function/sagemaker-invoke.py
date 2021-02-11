import json,datetime,decimal,boto3,logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def default_json_transform(obj):
    if isinstance(obj, decimal.Decimal):
        return str(obj)
    if isinstance(obj, (datetime.date, datetime.datetime)):
        return obj.isoformat()

    raise TypeError

def lambda_handler(event, context):
    status_code = 200

    # The return value will contain an array of arrays (one inner array per input row).
    array_of_rows_to_return = [ ]

    # initialise row to this value for reporting errors that occur before the row processing
    row = '(none)'

    try:
        if 'headers' not in event:
            raise ValueError("Headers were not passed in to external function")
        if 'sf-custom-sagemaker-endpoint-name' not in event['headers']:
            raise ValueError("Your external function must include the parameter \"HEADERS = ('sagemaker-endpoint-name'='<your sagemaker endpoint name>','sagemaker-region'='<your sagemaker region>')\"")
        if 'sf-custom-sagemaker-region' not in event['headers']:
            raise ValueError("Your external function must include the parameter \"HEADERS = ('sagemaker-endpoint-name'='<your sagemaker endpoint name>','sagemaker-region'='<your sagemaker region>')\"")
        
        sagemaker_endpoint_name=event['headers']['sf-custom-sagemaker-endpoint-name']
        sagemaker_region=event['headers']['sf-custom-sagemaker-region']

        client = boto3.client("sagemaker-runtime", region_name=sagemaker_region)
        logger.info(event["body"])
        input_rows = json.loads(event["body"])["data"]
        input_values = [",".join(map(str,row[1])) for row in input_rows]

        logger.info(input_values)
        input_body = "\n".join(input_values)
        logger.info(input_body)
        response = client.invoke_endpoint(
            EndpointName = sagemaker_endpoint_name,
            Body = input_body,
            ContentType = "text/csv",
            Accept = "application/json"
        )

        output_body = json.loads(response["Body"].read().decode())
        print(output_body)
        # {'predictions': [{'score': 0.6601340174674988, 'predicted_label': 1}]}
        if 'scores' in output_body:
            data = [[index, row] for index, row in enumerate(output_body["scores"])]
        elif 'predictions' in output_body:
            data = [[index, row] for index, row in enumerate(output_body["predictions"])]
        else:
            raise ValueError("Response from Sagemaker did not contain 'scores' or 'predictions'")
        json_compatible_string_to_return = json.dumps({"data" : data}, default=default_json_transform)

    except Exception as err:
        # 400 implies some type of error.
        status_code = 400
        # Tell caller what this function could not handle.
        json_compatible_string_to_return = json.dumps({"data" : row,"error" : str(err)})

    # Return the return value and HTTP status code.
    return {
        'statusCode': status_code,
        'body': json_compatible_string_to_return
    }