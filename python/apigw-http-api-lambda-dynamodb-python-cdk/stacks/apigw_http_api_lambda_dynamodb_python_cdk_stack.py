# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import os
from aws_cdk import (
    Stack,
    aws_dynamodb as dynamodb_,
    aws_lambda as lambda_,
    aws_apigateway as apigw_,
    aws_ec2 as ec2,
    aws_iam as iam,
    aws_logs as logs,
    aws_cloudtrail as cloudtrail,
    aws_s3 as s3,
    aws_wafv2 as wafv2,
    Duration,
)
from constructs import Construct

TABLE_NAME = "demo_table"


class ApigwHttpApiLambdaDynamodbPythonCdkStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # CloudTrail S3 bucket for audit logs
        cloudtrail_bucket = s3.Bucket(
            self,
            "CloudTrailBucket",
            encryption=s3.BucketEncryption.S3_MANAGED,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
        )

        # Configure CloudTrail for API activity logging
        trail = cloudtrail.Trail(
            self,
            "ApiTrail",
            bucket=cloudtrail_bucket,
            is_logging=True,
            include_global_service_events=True,
            is_multi_region_trail=True,
            enable_file_validation=True,
        )

        # VPC
        vpc = ec2.Vpc(
            self,
            "Ingress",
            cidr="10.1.0.0/16",
            subnet_configuration=[
                ec2.SubnetConfiguration(
                    name="Private-Subnet", subnet_type=ec2.SubnetType.PRIVATE_ISOLATED,
                    cidr_mask=24
                )
            ],
        )

        # VPC Flow Logs configuration
        flow_log_group = logs.LogGroup(
            self,
            "VpcFlowLogGroup",
            retention=logs.RetentionDays.ONE_MONTH,
        )

        vpc.add_flow_log(
            "FlowLog",
            destination=ec2.FlowLogDestination.to_cloud_watch_logs(flow_log_group),
            traffic_type=ec2.FlowLogTrafficType.ALL,
        )
        
        # Create VPC endpoint
        dynamo_db_endpoint = ec2.GatewayVpcEndpoint(
            self,
            "DynamoDBVpce",
            service=ec2.GatewayVpcEndpointAwsService.DYNAMODB,
            vpc=vpc,
        )

        # This allows to customize the endpoint policy
        dynamo_db_endpoint.add_to_policy(
            iam.PolicyStatement(  # Restrict to listing and describing tables
                principals=[iam.AnyPrincipal()],
                actions=[                "dynamodb:DescribeStream",
                "dynamodb:DescribeTable",
                "dynamodb:Get*",
                "dynamodb:Query",
                "dynamodb:Scan",
                "dynamodb:CreateTable",
                "dynamodb:Delete*",
                "dynamodb:Update*",
                "dynamodb:PutItem"],
                resources=["*"],
            )
        )

        # Create DynamoDb Table
        demo_table = dynamodb_.Table(
            self,
            TABLE_NAME,
            partition_key=dynamodb_.Attribute(
                name="id", type=dynamodb_.AttributeType.STRING
            ),
        )

        # Create the Lambda function with reserved concurrency (REL05-BP02)
        api_hanlder = lambda_.Function(
            self,
            "ApiHandler",
            function_name="apigw_handler",
            runtime=lambda_.Runtime.PYTHON_3_9,
            code=lambda_.Code.from_asset("lambda/apigw-handler"),
            handler="index.handler",
            vpc=vpc,
            vpc_subnets=ec2.SubnetSelection(
                subnet_type=ec2.SubnetType.PRIVATE_ISOLATED
            ),
            memory_size=1024,
            timeout=Duration.minutes(5),
            tracing=lambda_.Tracing.ACTIVE,  # Enable X-Ray tracing
            reserved_concurrent_executions=100,  # REL05-BP02: Control Lambda concurrency
        )

        # grant permission to lambda to write to demo table
        demo_table.grant_write_data(api_hanlder)
        api_hanlder.add_environment("TABLE_NAME", demo_table.table_name)

        # API Gateway access logs configuration
        api_log_group = logs.LogGroup(
            self,
            "ApiGatewayAccessLogs",
            retention=logs.RetentionDays.ONE_MONTH,
        )

        # Create API Gateway with throttling configuration (REL05-BP02)
        api = apigw_.LambdaRestApi(
            self,
            "Endpoint",
            handler=api_hanlder,
            deploy_options=apigw_.StageOptions(
                access_log_destination=apigw_.LogGroupLogDestination(api_log_group),
                access_log_format=apigw_.AccessLogFormat.json_with_standard_fields(),
                tracing_enabled=True,  # Enable X-Ray tracing
                # REL05-BP02: Configure throttling limits
                throttle_rate_limit=1000,  # Requests per second
                throttle_burst_limit=2000,  # Burst capacity
            ),
        )

        # REL05-BP02: Create API Key for controlled access
        api_key = apigw_.ApiKey(
            self,
            "ApiKey",
            description="API key for controlled access to the demo API"
        )

        # REL05-BP02: Create Usage Plan with per-client throttling
        usage_plan = apigw_.UsagePlan(
            self,
            "UsagePlan",
            name="StandardUsagePlan",
            description="Standard usage plan with throttling and quota limits",
            throttle=apigw_.ThrottleSettings(
                rate_limit=500,  # Per-client rate limit (requests per second)
                burst_limit=1000  # Per-client burst limit
            ),
            quota=apigw_.QuotaSettings(
                limit=10000,  # Daily quota
                period=apigw_.Period.DAY
            )
        )

        # Associate API key with usage plan
        usage_plan.add_api_key(api_key)
        usage_plan.add_api_stage(
            stage=api.deployment_stage,
            api=api
        )

        # REL05-BP02: Create WAF Web ACL with rate-based rules
        web_acl = wafv2.CfnWebACL(
            self,
            "ApiWAF",
            scope="REGIONAL",
            default_action=wafv2.CfnWebACL.DefaultActionProperty(allow={}),
            rules=[
                wafv2.CfnWebACL.RuleProperty(
                    name="RateLimitRule",
                    priority=1,
                    statement=wafv2.CfnWebACL.StatementProperty(
                        rate_based_statement=wafv2.CfnWebACL.RateBasedStatementProperty(
                            limit=2000,  # Requests per 5-minute window per IP
                            aggregate_key_type="IP"
                        )
                    ),
                    action=wafv2.CfnWebACL.RuleActionProperty(
                        block={}
                    ),
                    visibility_config=wafv2.CfnWebACL.VisibilityConfigProperty(
                        sampled_requests_enabled=True,
                        cloud_watch_metrics_enabled=True,
                        metric_name="RateLimitRule"
                    )
                )
            ],
            visibility_config=wafv2.CfnWebACL.VisibilityConfigProperty(
                sampled_requests_enabled=True,
                cloud_watch_metrics_enabled=True,
                metric_name="ApiWAF"
            )
        )

        # Associate WAF with API Gateway stage
        wafv2.CfnWebACLAssociation(
            self,
            "WAFAssociation",
            resource_arn=f"arn:aws:apigateway:{self.region}::/restapis/{api.rest_api_id}/stages/{api.deployment_stage.stage_name}",
            web_acl_arn=web_acl.attr_arn
        )