import boto3

dynamodb = boto3.client('dynamodb')

try:
    dynamodb.update_table(
        TableName='battle-royale',
        AttributeDefinitions=[
            {
                "AttributeName": "map",
                "AttributeType": "S"
            },
            {
                "AttributeName": "PK",
                "AttributeType": "S"
            }
        ],
        GlobalSecondaryIndexUpdates=[
            {
                "Create": {
                    "IndexName": "MapPKIndex",
                    "KeySchema": [
                        {
                            "AttributeName": "map",
                            "KeyType": "HASH"
                        },
                        {
                            "AttributeName": "PK",
                            "KeyType": "RANGE"
                        }
                    ],
                    "Projection": {
                        "ProjectionType": "ALL"
                    }
                }
            }
        ],
        BillingMode="PAY_PER_REQUEST"
    )
    print("Table updated successfully. Added MapPKIndex GSI with map as partition key and PK as sort key.")
    print("Note: This GSI will consume on-demand capacity based on the actual usage.")
except Exception as e:
    print("Could not update table. Error:")
    print(e)
