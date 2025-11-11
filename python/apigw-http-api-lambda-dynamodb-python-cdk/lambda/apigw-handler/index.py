# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import boto3
import os
import json
import logging
import uuid
import time
from aws_xray_sdk.core import xray_recorder
from aws_xray_sdk.core import patch_all

# Patch AWS SDK calls for automatic tracing
patch_all()

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb_client = boto3.client("dynamodb")


@xray_recorder.capture('lambda_handler')
def handler(event, context):
    table = os.environ.get("TABLE_NAME")
    
    # Structured security logging
    security_log = {
        "event_type": "api_request",
        "timestamp": int(time.time()),
        "request_id": context.aws_request_id,
        "source_ip": event.get("requestContext", {}).get("identity", {}).get("sourceIp"),
        "user_agent": event.get("requestContext", {}).get("identity", {}).get("userAgent"),
        "method": event.get("httpMethod"),
        "path": event.get("path"),
        "resource": event.get("resource"),
        "stage": event.get("requestContext", {}).get("stage")
    }
    logger.info(json.dumps(security_log))
    
    logging.info(f"## Loaded table name from environemt variable DDB_TABLE: {table}")
    
    if event["body"]:
        with xray_recorder.in_subsegment('process_request_body'):
            item = json.loads(event["body"])
            logging.info(f"## Received payload: {item}")
            year = str(item["year"])
            title = str(item["title"])
            id = str(item["id"])
        
        with xray_recorder.in_subsegment('dynamodb_put_item'):
            dynamodb_client.put_item(
                TableName=table,
                Item={"year": {"N": year}, "title": {"S": title}, "id": {"S": id}},
            )
        
        # Log successful data operation
        operation_log = {
            "event_type": "data_operation",
            "operation": "put_item",
            "table": table,
            "item_id": id,
            "request_id": context.aws_request_id,
            "status": "success"
        }
        logger.info(json.dumps(operation_log))
        
        message = "Successfully inserted data!"
        return {
            "statusCode": 200,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"message": message}),
        }
    else:
        logging.info("## Received request without a payload")
        default_id = str(uuid.uuid4())
        
        with xray_recorder.in_subsegment('dynamodb_put_default_item'):
            dynamodb_client.put_item(
                TableName=table,
                Item={
                    "year": {"N": "2012"},
                    "title": {"S": "The Amazing Spider-Man 2"},
                    "id": {"S": default_id},
                },
            )
        
        # Log default data operation
        operation_log = {
            "event_type": "data_operation",
            "operation": "put_item",
            "table": table,
            "item_id": default_id,
            "request_id": context.aws_request_id,
            "status": "success",
            "note": "default_payload_used"
        }
        logger.info(json.dumps(operation_log))
        
        message = "Successfully inserted data!"
        return {
            "statusCode": 200,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"message": message}),
        }