import boto3
import random
import time
import uuid
import json
import logging
from datetime import datetime
from entities import Game, UserGameMapping

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
MODULE_NAME = "join-game"
MAX_PLAYERS = 500
# Available maps list
MAPS = [
    "Green Grasslands", "Dirty Desert", "Urban Underground", "Juicy Jungle", "Open Ocean",
    "Mystic Mountains", "Frozen Frontier", "Volcanic Valley", "Haunted Hills", "Sunny Shores",
    "Cosmic Crater", "Ancient Ruins", "Neon City", "Foggy Forest", "Crystal Caves",
    "Burning Badlands", "Stormy Skies", "Toxic Tundra", "Peaceful Peaks", "Deadly Dunes",
    "Lava Lakes", "Windy Wasteland", "Tropical Treetops", "Icy Islands", "Murky Marshes",
    "Savage Savannah", "Radiant Reef", "Dusty Docks", "Phantom Fortress", "Bamboo Basin",
    "Crimson Canyon", "Emerald Estuary", "Golden Gorge", "Hidden Harbor", "Ivory Isle",
    "Jade Jungle", "Karst Kingdom", "Lunar Landscape", "Midnight Meadow", "Nebula Nexus",
    "Obsidian Outpost", "Prismatic Plains", "Quantum Quarry", "Ruby Ridge", "Sapphire Springs",
    "Twilight Temple", "Umbra Uplands", "Verdant Valley", "Whispering Woods", "Xenon Xanadu"
]

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
                if operation.startswith('get_') or operation.startswith('query_') or operation.startswith('scan_'):
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
                        if operation.startswith('get_') or operation.startswith('query_') or operation.startswith('scan_'):
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
                if operation.startswith('get_') or operation.startswith('query_') or operation.startswith('scan_'):
                    self.total_rcu += capacity_units
                else:
                    self.total_wcu += capacity_units
            else:
                # Otherwise sum the components
                self.total_rcu += table_rcu + gsi_rcu_total
                self.total_wcu += table_wcu + gsi_wcu_total
        
        elif isinstance(consumed_capacity, list):
            # For transactional operations
            total_table_rcu = 0
            total_table_wcu = 0
            total_gsi_rcu = 0
            total_gsi_wcu = 0
            
            for item in consumed_capacity:
                if isinstance(item, dict):  # Ensure item is a dictionary
                    # Get base table consumption directly from DynamoDB response
                    table_rcu = item.get('Table', {}).get('ReadCapacityUnits', 0)
                    table_wcu = item.get('Table', {}).get('WriteCapacityUnits', 0)
                    
                    # If Table doesn't have separate RCU/WCU, check for CapacityUnits
                    if table_rcu == 0 and table_wcu == 0 and 'Table' in item:
                        table_capacity = item['Table'].get('CapacityUnits', 0)
                        if operation.startswith('get_') or operation.startswith('query_') or operation.startswith('scan_'):
                            table_rcu = table_capacity
                        else:
                            table_wcu = table_capacity
                    
                    # Update table consumption
                    self.table_consumption['rcu'] += table_rcu
                    self.table_consumption['wcu'] += table_wcu
                    
                    total_table_rcu += table_rcu
                    total_table_wcu += table_wcu
                    
                    # Track GSI consumption for transactional operations
                    if 'GlobalSecondaryIndexes' in item:
                        for gsi_name, gsi_data in item['GlobalSecondaryIndexes'].items():
                            gsi_rcu = gsi_data.get('ReadCapacityUnits', 0)
                            gsi_wcu = gsi_data.get('WriteCapacityUnits', 0)
                            
                            # If GSI doesn't have separate RCU/WCU, check for CapacityUnits
                            if gsi_rcu == 0 and gsi_wcu == 0:
                                gsi_capacity = gsi_data.get('CapacityUnits', 0)
                                if operation.startswith('get_') or operation.startswith('query_') or operation.startswith('scan_'):
                                    gsi_rcu = gsi_capacity
                                else:
                                    gsi_wcu = gsi_capacity
                            
                            if gsi_name not in self.gsi_consumption:
                                self.gsi_consumption[gsi_name] = {'rcu': 0, 'wcu': 0}
                            
                            self.gsi_consumption[gsi_name]['rcu'] += gsi_rcu
                            self.gsi_consumption[gsi_name]['wcu'] += gsi_wcu
                            
                            total_gsi_rcu += gsi_rcu
                            total_gsi_wcu += gsi_wcu
                    
                    # Add to total consumption - directly from DynamoDB's total or sum of components
                    if 'CapacityUnits' in item:
                        # If DynamoDB provides a total, use it
                        capacity_units = item.get('CapacityUnits', 0)
                        if operation.startswith('get_') or operation.startswith('query_') or operation.startswith('scan_'):
                            self.total_rcu += capacity_units
                        else:
                            self.total_wcu += capacity_units
                    else:
                        # For this item, add the components to the total
                        item_rcu = table_rcu
                        item_wcu = table_wcu
                        
                        # Add GSI consumption for this item
                        if 'GlobalSecondaryIndexes' in item:
                            for gsi_name, gsi_data in item['GlobalSecondaryIndexes'].items():
                                item_rcu += gsi_data.get('ReadCapacityUnits', 0) or gsi_data.get('CapacityUnits', 0) if operation.startswith('get_') or operation.startswith('query_') or operation.startswith('scan_') else 0
                                item_wcu += gsi_data.get('WriteCapacityUnits', 0) or gsi_data.get('CapacityUnits', 0) if not (operation.startswith('get_') or operation.startswith('query_') or operation.startswith('scan_')) else 0
                        
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

def get_open_games_by_map(map_name):
    """
    Get open games by map name using the OpenGamesIndex GSI.
    Returns games for the specified map that have an open_timestamp attribute.
    
    Parameters:
    - map_name: The map name to query
    """
    try:
        # Use query with OpenGamesIndex GSI to find open games for the specified map
        logger.info(f"Querying for open games on map '{map_name}' using OpenGamesIndex GSI...")
        
        query_params = {
            "TableName": TABLE_NAME,
            "IndexName": "OpenGamesIndex",
            "KeyConditionExpression": "#map = :map_val",
            "FilterExpression": "attribute_exists(open_timestamp)",
            "ExpressionAttributeNames": {
                "#map": "map"  # Use expression attribute names to handle reserved keywords
            },
            "ExpressionAttributeValues": {
                ":map_val": {"S": map_name}
            },
            "ReturnConsumedCapacity": "INDEXES"  # Track both table and GSI consumption
        }
        
        # Execute query
        response = dynamodb.query(**query_params)
        
        # Add consumed capacity to tracker
        if 'ConsumedCapacity' in response:
            consumed_capacity = response['ConsumedCapacity']
            # Log raw consumed capacity for debugging
            app_logger.info(f"Raw ConsumedCapacity for query_open_games_map_{map_name}: {json.dumps(consumed_capacity, indent=2)}")
            resource_tracker.add_consumption(f"query_open_games_map_{map_name}", consumed_capacity)
        else:
            # Just record that the operation happened
            resource_tracker.operations.append(f"query_open_games_map_{map_name}")
        
        # Process results
        games = response.get('Items', [])
        logger.info(f"Found {len(games)} open games on map '{map_name}'")
        
        return games
    except Exception as e:
        resource_tracker.set_error(str(e))
        logger.error(f"Error querying games: {str(e)}")
        return []

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

def join_game_for_user(game_data, username):
    """
    Add a user to a game and track DynamoDB resource consumption.
    Uses game data directly from query operation.
    
    Parameters:
    - game_data: The game data from query operation
    - username: The username to add to the game
    """
    try:
        # Extract game ID from the game data
        game_id = None
        if 'game_id' in game_data and 'S' in game_data['game_id']:
            game_id = game_data['game_id']['S']
        elif 'PK' in game_data and 'S' in game_data['PK']:
            pk = game_data['PK']['S']
            if pk.startswith('GAME#'):
                game_id = pk[5:]  # Remove 'GAME#' prefix
        
        if not game_id:
            logger.error("Could not determine game ID from game data")
            resource_tracker.set_error("Could not determine game ID")
            return False
        
        logger.info(f"Attempting to join user {username} to game {game_id}")
        
        # Use transact_write_items to add user to game
        try:
            response = dynamodb.transact_write_items(
                TransactItems=[
                    {
                        "Put": {
                            "TableName": TABLE_NAME,
                            "Item": {
                                "PK": {"S": f"GAME#{game_id}"},
                                "SK": {"S": f"USER#{username}"},
                                "game_id": {"S": game_id},
                                "username": {"S": username}
                            },
                            "ConditionExpression": "attribute_not_exists(SK)"
                        },
                    },
                    {
                        "Update": {
                            "TableName": TABLE_NAME,
                            "Key": {
                                "PK": {"S": f"GAME#{game_id}"},
                                "SK": {"S": f"#METADATA#{game_id}"},
                            },
                            "UpdateExpression": "SET people = people + :p",
                            "ConditionExpression": "people <= :limit",
                            "ExpressionAttributeValues": {
                                ":p": {"N": "1"},
                                ":limit": {"N": str(MAX_PLAYERS - 1)}
                            }
                        }
                    }
                ],
                ReturnConsumedCapacity="INDEXES"  # Track both table and GSI consumption
            )
            
            # Add consumed capacity to tracker if available
            if 'ConsumedCapacity' in response:
                consumed_capacity = response['ConsumedCapacity']
                # Log raw consumed capacity for debugging - print full details
                app_logger.info(f"Raw ConsumedCapacity for join_game_transaction (FULL DETAILS): {json.dumps(consumed_capacity, indent=2)}")
                resource_tracker.add_consumption("join_game_transaction", consumed_capacity)
            else:
                # Just record that the operation happened
                resource_tracker.operations.append("join_game_transaction")
            
            logger.info(f"Successfully added user {username} to game {game_id}")
            return True
            
        except Exception as transaction_error:
            logger.error(f"Transaction error: {str(transaction_error)}")
            resource_tracker.set_error(f"Transaction error: {str(transaction_error)}")
            return False
            
    except Exception as e:
        resource_tracker.set_error(str(e))
        logger.error(f"Could not add user to game: {str(e)}")
        return False

def log_resource_usage(user_id):
    """
    Log accumulated DynamoDB resource consumption for the entire join-game module
    
    Parameters:
    - user_id: The user ID
    """
    timestamp = datetime.now().isoformat()
    
    # Calculate total resource consumption from both steps: finding games and joining game
    total_rcu = resource_tracker.total_rcu
    total_wcu = resource_tracker.total_wcu
    
    # Create log entry with accumulated resource usage
    log_entry = {
        "timestamp": timestamp,
        "module": MODULE_NAME,
        "operations": resource_tracker.operations,
        "user_id": user_id,
        "rcu": total_rcu,
        "wcu": total_wcu,
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
    
    # Log only to the DynamoDB resource logger
    ddb_logger.info(json.dumps(log_entry))

def main():
    """
    Main function: Randomly select a map, query for open games on that map using OpenGamesIndex GSI,
    and join a user to a game while recording resource usage.
    """
    # Reset resource tracker at the beginning
    resource_tracker.reset()
    
    try:
        # Randomly select a map
        selected_map = random.choice(MAPS)
        logger.info(f"Randomly selected map: {selected_map}")
        
        # Get open games on the selected map using OpenGamesIndex GSI
        open_games = get_open_games_by_map(selected_map)
        
        # If no open games on the current map, try other maps
        if not open_games:
            logger.warning(f"No open games found on map '{selected_map}'")
            
            # Try other maps
            logger.info("Trying to find open games on other maps...")
            for map_name in MAPS:
                if map_name != selected_map:
                    open_games = get_open_games_by_map(map_name)
                    if open_games:
                        logger.info(f"Found open games on map '{map_name}'")
                        break
        
        # If no open games on any map, log and exit
        if not open_games:
            logger.warning("No open games found on any map")
            resource_tracker.set_error("No open games available")
            log_resource_usage("N/A")
            return
        
        # Randomly select a game from the available open games
        selected_game = random.choice(open_games)
        logger.info(f"Randomly selected 1 game from {len(open_games)} available games")
        
        # Get a random user ID from the table (not tracking resource consumption)
        user_id = get_random_user_from_table()
        
        # Join the game - pass the entire game data from query operation
        success = join_game_for_user(selected_game, user_id)
        
        # If successful, reset any error state
        if success:
            resource_tracker.status = "success"
            resource_tracker.error = None
            logger.info(f"Successfully joined user {user_id} to game")
        else:
            logger.error(f"Failed to join user {user_id} to game")
        
        # Log the accumulated resource usage for the entire process
        log_resource_usage(user_id)
        
    except Exception as e:
        logger.error(f"Unexpected error in main function: {str(e)}")
        resource_tracker.set_error(f"Unexpected error: {str(e)}")
        log_resource_usage("error")

if __name__ == "__main__":
    main()
