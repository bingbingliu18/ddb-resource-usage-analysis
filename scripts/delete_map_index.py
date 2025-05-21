import boto3

dynamodb = boto3.client('dynamodb')

try:
    # Delete the OpenGamesIndex GSI from the battle-royale table
    dynamodb.update_table(
        TableName='battle-royale',
        GlobalSecondaryIndexUpdates=[
            {
                "Delete": {
                    "IndexName": "OpenGamesIndex"
                }
            }
        ]
    )
    print("Table updated successfully. Deleted OpenGamesIndex GSI.")
    print("Note: The deletion process may take some time to complete.")
    print("You can check the status of the deletion using the AWS Management Console or AWS CLI.")
except Exception as e:
    print("Could not delete GSI. Error:")
    print(e)
