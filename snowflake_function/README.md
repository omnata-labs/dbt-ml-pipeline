# Sagemaker External Function endpoint

## Contributors

- Whoever wrote this: https://community.snowflake.com/s/article/How-To-Use-Amazon-SageMaker-model-with-data-in-Snowflake-directly-in-SQL
- James Weakley ([Omnata](https://omnata.com))

## Installation

### Step 1 - Prerequisites

1) Install [Python 3.8](https://www.python.org/downloads/).

2) Install the [serverless framework](https://www.serverless.com/framework/docs/providers/aws/guide/installation/).

3) Follow the [serverless setup guide](https://www.serverless.com/framework/docs/providers/aws/guide/credentials/) to authenticate to your AWS account, ready to deploy.

4) Install the [serverless-python-requirements](https://www.npmjs.com/package/serverless-python-requirements) plugin, which takes care of bundling the python libraries into our lambda when deploying.

5) If you prefer, you can set up [AWS profiles and/or export environment variables](https://www.serverless.com/framework/docs/providers/aws/guide/credentials#use-an-existing-aws-profile) for serverless to pick them up. If your organisation uses SAML for accessing AWS, [saml2aws](https://github.com/Versent/saml2aws) is a great way to generate temporary CLI credentials.

### Step 2 - Deploy the API Gateway and Lambda (sans Snowflake privileges)

[Clone this repo](https://docs.github.com/en/free-pro-team@latest/github/creating-cloning-and-archiving-repositories/cloning-a-repository), cd into it and run: `serverless deploy --config serverless-initial.yml --region <your aws region>`

(substitute your actual AWS region accordingly).

Once deployed, run: `serverless info -v --config serverless-initial.yml --region <your aws region>`

In the Stack Outputs section at the bottom, there are two pieces of information you need for the next step:
1. The ARN of the role for Snowflake to assume (see "SnowflakeExternalFunctionRole", starts with `arn:aws:iam::`)
2. The API endpoint for Snowflake to hit (see "ServiceEndpoint", starts with `https://`)


### Step 3 - Create the Snowflake API integration

Run the following SQL, with [the appropriate privileges](https://docs.snowflake.com/en/sql-reference/sql/create-api-integration.html#usage-notes)
```
create or replace api integration SAGEMAKER_LAMBDA
    api_provider = aws_api_gateway
    api_aws_role_arn = '<The SnowflakeExternalFunctionRole value from step 2>'
    enabled = true
    api_allowed_prefixes = ('<The ServiceEndpoint value from step 2, with the /dev suffix removed>');

    describe integration SAGEMAKER_LAMBDA;
```

The output from the above `describe` statement will return two pieces of information you need to further update the AWS infrastructure:
1) The `API_AWS_IAM_USER_ARN` used to permit Snowflake to assume the AWS role that can call the AWS API gateway
2) The `API_AWS_EXTERNAL_ID ` used to restrict the scope of the above user to our specific api integration

### Step 4 - Update the AWS infrastructure to trust the Snowflake IAM user

Back on your local command line, run:
`serverless deploy --region <your aws region> --snowflake_user_arn <API_AWS_IAM_USER_ARN from step 3> --snowflake_external_id <API_AWS_EXTERNAL_ID from step 3>`

Wait for the stack to update successfully.

### Step 5 - Create the external functions in Snowflake

Run the following SQL:
```
create or replace external function SAGEMAKER_INVOKE(parameters variant)
    returns variant
    VOLATILE
    api_integration = SAGEMAKER_LAMBDA  
    HEADERS = ( 'sagemaker-endpoint-name' = '<your sagemaker endpoint name>') 
    MAX_BATCH_ROWS = 10000
    as '<The ServiceEndpoint value from step 2>'
    ;
    
```

## Examples

```
select SAGEMAKER_INVOKE(ARRAY_CONSTRUCT(-0.32,0.29,-1.041,0,-0.91,0.021,1,1,1,0,0,1,0))
```
