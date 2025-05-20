import boto3
import random
import time
import uuid
import json
import logging
import sys
from datetime import datetime
from botocore.exceptions import ClientError

# Configure logging
# Console logger for application logs
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(logging.Formatter('%(asctime)s - %(message)s'))

# File logger for JSON logs - only for DynamoDB resource usage
file_handler = logging.FileHandler("dynamodb_resource_usage.log")
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(message)s'))  # Include timestamp in log file

# Create separate loggers
app_logger = logging.getLogger('app')
app_logger.setLevel(logging.INFO)
app_logger.addHandler(console_handler)
app_logger.propagate = False

# DynamoDB resource usage logger
ddb_logger = logging.getLogger('ddb_resources')
ddb_logger.setLevel(logging.INFO)
ddb_logger.addHandler(file_handler)
ddb_logger.propagate = False

# For backward compatibility
logger = app_logger

# Initialize DynamoDB client
dynamodb = boto3.client('dynamodb')

# Constants
TABLE_NAME = "battle-royale"
MODULE_NAME = "query-user-games"

class ResourceTracker:
    """
    Class to track and accumulate DynamoDB resource consumption across multiple operations.
    """
    def __init__(self):
        self.reset()
    
    def reset(self):
        """Reset all tracking metrics."""
        self.total_rcu = 0
        self.total_wcu = 0
        self.table_consumption = {'rcu': 0, 'wcu': 0}  # Track base table consumption
        self.gsi_consumption = {}  # Track GSI consumption separately
        self.operations = []
        self.start_time = time.time()
        self.status = "success"
        self.error = None
    
    def add_consumption(self, operation, consumed_capacity):
        """
        Add resource consumption from an operation to the total.
        
        Parameters:
        - operation: The operation name
        - consumed_capacity: The consumed capacity information from DynamoDB
        """
        # Handle case where consumed_capacity might be None
        if consumed_capacity is None:
            self.operations.append(operation)
            return
        
        if isinstance(consumed_capacity, dict):
            # For single operations
            # Get base table consumption directly from DynamoDB response
            table_rcu = consumed_capacity.get('Table', {}).get('ReadCapacityUnits', 0)
            table_wcu = consumed_capacity.get('Table', {}).get('WriteCapacityUnits', 0)
            
            # If Table doesn't have separate RCU/WCU, check for CapacityUnits
            if table_rcu == 0 and table_wcu == 0 and 'Table' in consumed_capacity:
                table_capacity = consumed_capacity['Table'].get('CapacityUnits', 0)
                if operation.startswith('get_') or operation.startswith('query_'):
                    table_rcu = table_capacity
                else:
                    table_wcu = table_capacity
            
            # Update table consumption
            self.table_consumption['rcu'] += table_rcu
            self.table_consumption['wcu'] += table_wcu
            
            # Track GSI consumption if available
            gsi_rcu_total = 0
            gsi_wcu_total = 0
            if 'GlobalSecondaryIndexes' in consumed_capacity:
                for gsi_name, gsi_data in consumed_capacity['GlobalSecondaryIndexes'].items():
                    gsi_rcu = gsi_data.get('ReadCapacityUnits', 0)
                    gsi_wcu = gsi_data.get('WriteCapacityUnits', 0)
                    
                    # If GSI doesn't have separate RCU/WCU, check for CapacityUnits
                    if gsi_rcu == 0 and gsi_wcu == 0:
                        gsi_capacity = gsi_data.get('CapacityUnits', 0)
                        if operation.startswith('get_') or operation.startswith('query_'):
                            gsi_rcu = gsi_capacity
                        else:
                            gsi_wcu = gsi_capacity
                    
                    if gsi_name not in self.gsi_consumption:
                        self.gsi_consumption[gsi_name] = {'rcu': 0, 'wcu': 0}
                    
                    self.gsi_consumption[gsi_name]['rcu'] += gsi_rcu
                    self.gsi_consumption[gsi_name]['wcu'] += gsi_wcu
                    
                    gsi_rcu_total += gsi_rcu
                    gsi_wcu_total += gsi_wcu
            
            # Add to total consumption - directly from DynamoDB's total or sum of components
            if 'CapacityUnits' in consumed_capacity:
                # If DynamoDB provides a total, use it
                capacity_units = consumed_capacity.get('CapacityUnits', 0)
                if operation.startswith('get_') or operation.startswith('query_'):
                    self.total_rcu += capacity_units
                else:
                    self.total_wcu += capacity_units
            else:
                # Otherwise sum the components
                self.total_rcu += table_rcu + gsi_rcu_total
                self.total_wcu += table_wcu + gsi_wcu_total
        
        elif isinstance(consumed_capacity, list):
            # For transactional operations
            for item in consumed_capacity:
                if isinstance(item, dict):  # Ensure item is a dictionary
                    # Get base table consumption directly from DynamoDB response
                    table_rcu = item.get('Table', {}).get('ReadCapacityUnits', 0)
                    table_wcu = item.get('Table', {}).get('WriteCapacityUnits', 0)
                    
                    # If Table doesn't have separate RCU/WCU, check for CapacityUnits
                    if table_rcu == 0 and table_wcu == 0 and 'Table' in item:
                        table_capacity = item['Table'].get('CapacityUnits', 0)
                        if operation.startswith('get_') or operation.startswith('query_'):
                            table_rcu = table_capacity
                        else:
                            table_wcu = table_capacity
                    
                    # Update table consumption
                    self.table_consumption['rcu'] += table_rcu
                    self.table_consumption['wcu'] += table_wcu
                    
                    # Track GSI consumption for transactional operations
                    gsi_item_rcu_total = 0
                    gsi_item_wcu_total = 0
                    if 'GlobalSecondaryIndexes' in item:
                        for gsi_name, gsi_data in item['GlobalSecondaryIndexes'].items():
                            gsi_rcu = gsi_data.get('ReadCapacityUnits', 0)
                            gsi_wcu = gsi_data.get('WriteCapacityUnits', 0)
                            
                            # If GSI doesn't have separate RCU/WCU, check for CapacityUnits
                            if gsi_rcu == 0 and gsi_wcu == 0:
                                gsi_capacity = gsi_data.get('CapacityUnits', 0)
                                if operation.startswith('get_') or operation.startswith('query_'):
                                    gsi_rcu = gsi_capacity
                                else:
                                    gsi_wcu = gsi_capacity
                            
                            if gsi_name not in self.gsi_consumption:
                                self.gsi_consumption[gsi_name] = {'rcu': 0, 'wcu': 0}
                            
                            self.gsi_consumption[gsi_name]['rcu'] += gsi_rcu
                            self.gsi_consumption[gsi_name]['wcu'] += gsi_wcu
                            
                            gsi_item_rcu_total += gsi_rcu
                            gsi_item_wcu_total += gsi_wcu
                    
                    # Add to total consumption
                    if 'CapacityUnits' in item:
                        # If DynamoDB provides a total, use it
                        capacity_units = item.get('CapacityUnits', 0)
                        if operation.startswith('get_') or operation.startswith('query_'):
                            self.total_rcu += capacity_units
                        else:
                            self.total_wcu += capacity_units
                    else:
                        # For this item, add the components to the total
                        item_rcu = table_rcu + gsi_item_rcu_total
                        item_wcu = table_wcu + gsi_item_wcu_total
                        
                        self.total_rcu += item_rcu
                        self.total_wcu += item_wcu
        
        self.operations.append(operation)
    
    def set_error(self, error_msg):
        """Set error status and message."""
        self.status = "failed"
        self.error = error_msg
    
    def get_latency_ms(self):
        """Get total latency in milliseconds."""
        return (time.time() - self.start_time) * 1000

# Create global resource tracker
resource_tracker = ResourceTracker()

def get_random_user_from_table():
    """
    Query users from the DynamoDB table and return a random user ID.
    This function does not track DynamoDB resource consumption.
    """
    try:
        # Query users from the table
        response = dynamodb.scan(
            TableName=TABLE_NAME,
            FilterExpression="begins_with(PK, :pk_prefix) AND begins_with(SK, :sk_prefix)",
            ExpressionAttributeValues={
                ":pk_prefix": {"S": "USER#"},
                ":sk_prefix": {"S": "#METADATA#"}
            },
            ProjectionExpression="PK",
            Limit=10  # Limit to 10 users for efficiency
        )
        
        users = response.get('Items', [])
        
        if not users:
            logger.warning("No users found in the table, generating a random user ID instead")
            return f"user_{uuid.uuid4().hex[:8]}"
        
        # Select a random user
        selected_user = random.choice(users)
        user_id = selected_user['PK']['S'].replace('USER#', '')
        
        logger.info(f"Selected random user: {user_id}")
        return user_id
        
    except Exception as e:
        logger.error(f"Error fetching users from table: {str(e)}")
        # Fallback to generating a random user ID
        return f"user_{uuid.uuid4().hex[:8]}"

def query_user_games(user_id):
    """
    Query games joined by a specific user using the InvertedIndex GSI.
    
    Parameters:
    - user_id: The user ID to query games for
    
    Returns:
    - List of games the user has joined
    """
    try:
        logger.info(f"Querying games for user '{user_id}' using InvertedIndex GSI...")
        
        response = dynamodb.query(
            TableName=TABLE_NAME,
            IndexName="InvertedIndex",
            KeyConditionExpression="SK = :sk_val",
            ExpressionAttributeValues={
                ":sk_val": {"S": f"USER#{user_id}"}
            },
            ReturnConsumedCapacity="INDEXES"  # Track both table and GSI consumption
        )
        
        # Add consumed capacity to tracker
        if 'ConsumedCapacity' in response:
            consumed_capacity = response['ConsumedCapacity']
            resource_tracker.add_consumption(f"query_user_games_{user_id}", consumed_capacity)
        else:
            # Just record that the operation happened
            resource_tracker.operations.append(f"query_user_games_{user_id}")
        
        games = response.get('Items', [])
        logger.info(f"Found {len(games)} games joined by user '{user_id}'")
        
        # Extract game details from the response
        game_details = []
        for game in games:
            game_id = None
            if 'game_id' in game and 'S' in game['game_id']:
                game_id = game['game_id']['S']
            elif 'PK' in game and 'S' in game['PK']:
                pk = game['PK']['S']
                if pk.startswith('GAME#'):
                    game_id = pk[5:]  # Remove 'GAME#' prefix
            
            if game_id:
                game_details.append({
                    'game_id': game_id,
                    'username': user_id
                })
        
        return game_details
    
    except ClientError as e:
        error_code = e.response.get('Error', {}).get('Code', 'Unknown')
        error_message = e.response.get('Error', {}).get('Message', str(e))
        
        if error_code == 'ResourceNotFoundException':
            logger.error(f"GSI 'InvertedIndex' not found: {error_message}")
            resource_tracker.set_error(f"GSI not found: {error_message}")
        else:
            logger.error(f"DynamoDB error ({error_code}): {error_message}")
            resource_tracker.set_error(f"DynamoDB error: {error_message}")
        
        return []
    except Exception as e:
        resource_tracker.set_error(str(e))
        logger.error(f"Error querying games using InvertedIndex GSI: {str(e)}")
        return []

def get_game_details(game_ids):
    """
    Get detailed information about games using their IDs.
    
    Parameters:
    - game_ids: List of game IDs to get details for
    
    Returns:
    - List of game details
    """
    if not game_ids:
        return []
    
    try:
        logger.info(f"Getting details for {len(game_ids)} games...")
        
        # Prepare batch get items request
        keys = []
        for game_id in game_ids:
            keys.append({
                'PK': {'S': f"GAME#{game_id}"},
                'SK': {'S': f"#METADATA#{game_id}"}
            })
        
        # Split into batches of 100 if needed (DynamoDB BatchGetItem limit)
        all_game_details = []
        for i in range(0, len(keys), 100):
            batch_keys = keys[i:i+100]
            
            response = dynamodb.batch_get_item(
                RequestItems={
                    TABLE_NAME: {
                        'Keys': batch_keys,
                        'ConsistentRead': False
                    }
                },
                ReturnConsumedCapacity="INDEXES"
            )
            
            # Add consumed capacity to tracker
            if 'ConsumedCapacity' in response:
                consumed_capacity = response['ConsumedCapacity']
                resource_tracker.add_consumption(f"get_game_details_batch_{i//100}", consumed_capacity)
            else:
                # Just record that the operation happened
                resource_tracker.operations.append(f"get_game_details_batch_{i//100}")
            
            # Extract game details
            if 'Responses' in response and TABLE_NAME in response['Responses']:
                items = response['Responses'][TABLE_NAME]
                
                for item in items:
                    game_detail = {}
                    
                    # Extract game ID
                    if 'game_id' in item and 'S' in item['game_id']:
                        game_detail['game_id'] = item['game_id']['S']
                    elif 'PK' in item and 'S' in item['PK']:
                        pk = item['PK']['S']
                        if pk.startswith('GAME#'):
                            game_detail['game_id'] = pk[5:]  # Remove 'GAME#' prefix
                    
                    # Extract other game details
                    if 'map' in item and 'S' in item['map']:
                        game_detail['map'] = item['map']['S']
                    
                    if 'people' in item and 'N' in item['people']:
                        game_detail['people'] = int(item['people']['N'])
                    
                    if 'max_people' in item and 'N' in item['max_people']:
                        game_detail['max_people'] = int(item['max_people']['N'])
                    
                    if 'open_timestamp' in item and 'N' in item['open_timestamp']:
                        game_detail['open_timestamp'] = int(item['open_timestamp']['N'])
                    
                    all_game_details.append(game_detail)
        
        logger.info(f"Retrieved details for {len(all_game_details)} games")
        return all_game_details
    
    except Exception as e:
        resource_tracker.set_error(str(e))
        logger.error(f"Error getting game details: {str(e)}")
        return []

def log_resource_usage(user_id):
    """
    Log accumulated DynamoDB resource consumption for the entire module
    
    Parameters:
    - user_id: The user ID
    """
    timestamp = datetime.now().isoformat()
    
    # Create log entry with accumulated resource usage
    log_entry = {
        "timestamp": timestamp,
        "module": MODULE_NAME,
        "operations": resource_tracker.operations,
        "user_id": user_id,
        "rcu": resource_tracker.total_rcu,
        "wcu": resource_tracker.total_wcu,
        "table": TABLE_NAME,
        "status": resource_tracker.status,
        "latency_ms": resource_tracker.get_latency_ms(),
        "region": boto3.session.Session().region_name,
        "request_id": str(uuid.uuid4())
    }
    
    # Add base table consumption information
    log_entry["table_usage"] = resource_tracker.table_consumption
    
    # Add GSI consumption information if available
    if resource_tracker.gsi_consumption:
        log_entry["gsi_usage"] = resource_tracker.gsi_consumption
    
    # Add error information if available
    if resource_tracker.error:
        log_entry["error"] = resource_tracker.error
    
    # Log only to the DynamoDB resource logger - not to console
    ddb_logger.info(json.dumps(log_entry))

def main():
    """
    Main function: Query games joined by a user using InvertedIndex GSI
    and get detailed information about those games while tracking resource usage.
    """
    # Reset resource tracker at the beginning
    resource_tracker.reset()
    
    try:
        # Get user ID from command line argument or use a random user
        if len(sys.argv) > 1:
            user_id = sys.argv[1]
            logger.info(f"Using provided user ID: {user_id}")
        else:
            # Get a random user ID from the table
            user_id = get_random_user_from_table()
            logger.info(f"Using random user ID: {user_id}")
        
        # Query games joined by the user using InvertedIndex GSI
        user_games = query_user_games(user_id)
        
        if not user_games:
            logger.warning(f"No games found for user '{user_id}'")
            log_resource_usage(user_id)
            return
        
        # Extract game IDs
        game_ids = [game['game_id'] for game in user_games if 'game_id' in game]
        
        # Get detailed information about the games
        game_details = get_game_details(game_ids)
        
        # Display results
        logger.info(f"User '{user_id}' has joined {len(user_games)} games:")
        for game in game_details:
            game_id = game.get('game_id', 'Unknown')
            game_map = game.get('map', 'Unknown')
            people = game.get('people', 0)
            max_people = game.get('max_people', 0)
            
            logger.info(f"Game ID: {game_id}, Map: {game_map}, Players: {people}/{max_people}")
        
        # Log the accumulated resource usage for the entire process
        log_resource_usage(user_id)
        
    except Exception as e:
        logger.error(f"Unexpected error in main function: {str(e)}")
        resource_tracker.set_error(f"Unexpected error: {str(e)}")
        log_resource_usage("error")

if __name__ == "__main__":
    main()
