import os
import json
from datetime import datetime, timedelta, timezone
import uuid

import boto3

stepfunctions = boto3.client("stepfunctions")


def _get_yesterday_utc() -> str:
    """
    Returns yesterday's date in UTC as YYYY-MM-DD.
    """
    yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).date()
    return yesterday.isoformat()


def lambda_handler(event, context):
    """
    EventBridge -> this Lambda -> Step Functions.

    Currently:
      - Computes 'yesterday' in UTC
      - Starts the daily pipeline Step Function with {"day": "YYYY-MM-DD"}

    Later can extend this to:
      - Accept overrides from `event` (backfills, custom ranges)
      - Do pre-flight checks, schema gates, etc.
    """
    state_machine_name = os.environ["STATE_MACHINE_NAME"]
    aws_account_id = os.environ["AWS_ACCOUNT_ID"]
    aws_region = os.environ.get("AWS_REGION", "us-east-1")

    state_machine_arn = f"arn:aws:states:{aws_region}:{aws_account_id}:stateMachine:{state_machine_name}"

    # For now, always run "yesterday" in UTC
    day = _get_yesterday_utc()

    execution_input = {"day": day}

    # Make execution name unique but predictable-ish
    execution_name = f"{state_machine_name}-{day}-{uuid.uuid4().hex[:8]}"

    resp = stepfunctions.start_execution(
        stateMachineArn=state_machine_arn,
        name=execution_name,
        input=json.dumps(execution_input),
    )

    return {
        "statusCode": 200,
        "body": json.dumps(
            {
                "message": "Started Step Functions execution",
                "stateMachineArn": state_machine_arn,
                "executionArn": resp["executionArn"],
                "day": day,
            }
        ),
    }
