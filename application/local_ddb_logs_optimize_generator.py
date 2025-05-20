#!/usr/bin/env python3
import json
import time
import random
import datetime
import uuid
import threading
import logging
import sys
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

# Constants for log generation
USERS = ["linda1130", "potterantonio938", "spencerjohnson431", "ryan2152", "todd04710", "jamie04997"]
MAPS = ["Haunted Hills", "Bamboo Basin", "Quantum Quarry", "Murky Marshes", "Whispering Woods", 
        "Juicy Jungle", "Hidden Harbor", "Crystal Caves", "Jade Jungle", "Ruby Ridge"]
TABLE_NAME = "battle-royale"
REGION = "us-east-1"

# Local file paths for storing logs
LOG_DIR = "logs"
NDJSON_LOG_FILE = os.path.join(LOG_DIR, "ddb_resource_logs.ndjson")

# Simulation parameters
CALLS_PER_SECOND = 5  # 5 calls per second for each module
BATCH_SIZE = 10  # Number of logs to batch before writing to file

# Statistics counters
stats = {
    "join_game_logs_generated": 0,
    "query_user_logs_generated": 0,
    "successful_writes": 0,
    "failed_writes": 0,
    "start_time": time.time()
}

def ensure_log_directory():
    """Ensure the log directory exists"""
    if not os.path.exists(LOG_DIR):
        os.makedirs(LOG_DIR)
        logging.info(f"Created log directory: {LOG_DIR}")

def generate_join_game_log():
    """Generate a log entry for the join-game module"""
    user_id = random.choice(USERS)
    map_name = random.choice(MAPS)
    
    # RCU is typically 0.5 or 1.0 for join-game (based on example logs)
    rcu = random.choice([0.5, 1.0])
    # WCU is typically 8.0 for join-game (based on example logs)
    wcu = 8.0
    
    # Latency between 69-90ms (based on example logs)
    latency_ms = round(random.uniform(69.0, 90.0), 8)
    
    # Generate timestamp
    timestamp = datetime.datetime.now().isoformat()
    
    # Generate request ID
    request_id = str(uuid.uuid4())
    
    # Create log entry with structure matching example logs
    log_entry = {
        "timestamp": timestamp,
        "module": "join-game",
        "operations": [f"query_open_games_map_{map_name}", "join_game_transaction"],
        "user_id": user_id,
        "rcu": rcu,
        "wcu": wcu,
        "table": TABLE_NAME,
        "status": "success",
        "latency_ms": latency_ms,
        "region": REGION,
        "request_id": request_id,
        "table_usage": {"rcu": 0.0, "wcu": 4.0},
        "gsi_usage": {
            "OpenGamesIndex": {"rcu": rcu, "wcu": 1.0},
            "InvertedIndex": {"rcu": 0, "wcu": 2.0},
            "MapPKIndex": {"rcu": 0, "wcu": 1.0}
        }
    }
    
    stats["join_game_logs_generated"] += 1
    return log_entry

def generate_query_user_games_log():
    """Generate a log entry for the query-user-games module"""
    user_id = random.choice(USERS)
    
    # RCU is typically between 2.0 and 5.0 for query-user-games (based on example logs)
    rcu = round(random.uniform(2.0, 5.0), 1)
    # WCU is typically 0 for query-user-games (read-only operation)
    wcu = 0
    
    # Latency between 60-70ms (based on example logs)
    latency_ms = round(random.uniform(60.0, 70.0), 8)
    
    # Generate timestamp
    timestamp = datetime.datetime.now().isoformat()
    
    # Generate request ID
    request_id = str(uuid.uuid4())
    
    # Create log entry with structure matching example logs
    log_entry = {
        "timestamp": timestamp,
        "module": "query-user-games",
        "operations": [f"query_user_games_{user_id}", "get_game_details_batch_0"],
        "user_id": user_id,
        "rcu": rcu,
        "wcu": wcu,
        "table": TABLE_NAME,
        "status": "success",
        "latency_ms": latency_ms,
        "region": REGION,
        "request_id": request_id,
        "table_usage": {"rcu": rcu - 0.5, "wcu": 0},
        "gsi_usage": {
            "InvertedIndex": {"rcu": 0.5, "wcu": 0}
        }
    }
    
    stats["query_user_logs_generated"] += 1
    return log_entry

def write_logs_to_file(logs):
    """Write logs to NDJSON file"""
    if not logs:
        return True
    
    try:
        # Write to NDJSON file (append mode)
        with open(NDJSON_LOG_FILE, 'a') as f:
            for log in logs:
                f.write(json.dumps(log) + '\n')
        
        logging.info(f"Successfully wrote {len(logs)} logs to NDJSON file")
        stats["successful_writes"] += len(logs)
        return True
        
    except Exception as e:
        logging.error(f"Error writing logs to file: {str(e)}")
        stats["failed_writes"] += len(logs)
        return False

def generate_and_write_logs(module_type):
    """Generate logs for a specific module and write them to file"""
    logs_buffer = []
    
    while True:
        try:
            # Generate logs based on module type
            if module_type == "join-game":
                log = generate_join_game_log()
            else:  # query-user-games
                log = generate_query_user_games_log()
            
            # Add log to buffer
            logs_buffer.append(log)
            
            # If buffer reaches batch size, write to files
            if len(logs_buffer) >= BATCH_SIZE:
                write_logs_to_file(logs_buffer)
                logs_buffer = []
            
            # Sleep to maintain the desired rate
            time.sleep(1.0 / CALLS_PER_SECOND)
            
        except Exception as e:
            logging.error(f"Error in {module_type} log generation: {str(e)}")
            time.sleep(1)  # Sleep and retry

def print_statistics():
    """Print statistics about the log generation and file writing process"""
    while True:
        time.sleep(30)  # Update stats every 30 seconds
        
        runtime = time.time() - stats["start_time"]
        hours, remainder = divmod(runtime, 3600)
        minutes, seconds = divmod(remainder, 60)
        
        logging.info("=== Statistics ===")
        logging.info(f"Runtime: {int(hours)}h {int(minutes)}m {int(seconds)}s")
        logging.info(f"Join-game logs generated: {stats['join_game_logs_generated']}")
        logging.info(f"Query-user logs generated: {stats['query_user_logs_generated']}")
        logging.info(f"Total logs generated: {stats['join_game_logs_generated'] + stats['query_user_logs_generated']}")
        logging.info(f"Successful writes: {stats['successful_writes']}")
        logging.info(f"Failed writes: {stats['failed_writes']}")
        
        # Calculate file size
        if os.path.exists(NDJSON_LOG_FILE):
            ndjson_size = os.path.getsize(NDJSON_LOG_FILE) / (1024 * 1024)  # Size in MB
            logging.info(f"NDJSON log file size: {ndjson_size:.2f} MB")
        
        if runtime > 0:
            rate = (stats['join_game_logs_generated'] + stats['query_user_logs_generated']) / runtime
            logging.info(f"Generation rate: {rate:.2f} logs/second")

def main():
    """Main function to run the continuous log generator"""
    logging.info("Starting DynamoDB resource usage log generator (NDJSON Version)")
    
    # Ensure log directory exists
    ensure_log_directory()
    
    # Initialize NDJSON log file if it doesn't exist
    if not os.path.exists(NDJSON_LOG_FILE):
        open(NDJSON_LOG_FILE, 'w').close()
        logging.info(f"Created empty NDJSON log file: {NDJSON_LOG_FILE}")
    
    # Start threads for each module type
    join_game_thread = threading.Thread(
        target=generate_and_write_logs,
        args=("join-game",),
        daemon=True
    )
    
    query_user_games_thread = threading.Thread(
        target=generate_and_write_logs,
        args=("query-user-games",),
        daemon=True
    )
    
    # Start statistics thread
    stats_thread = threading.Thread(
        target=print_statistics,
        daemon=True
    )
    
    join_game_thread.start()
    query_user_games_thread.start()
    stats_thread.start()
    
    logging.info("Log generation threads started")
    logging.info(f"Logs will be written to {NDJSON_LOG_FILE}")
    logging.info("Press Ctrl+C to stop the application")
    
    try:
        # Keep the main thread alive
        while True:
            time.sleep(10)
    except KeyboardInterrupt:
        # Calculate final statistics
        runtime = time.time() - stats["start_time"]
        hours, remainder = divmod(runtime, 3600)
        minutes, seconds = divmod(remainder, 60)
        
        logging.info("\n=== Final Statistics ===")
        logging.info(f"Total runtime: {int(hours)}h {int(minutes)}m {int(seconds)}s")
        logging.info(f"Join-game logs generated: {stats['join_game_logs_generated']}")
        logging.info(f"Query-user logs generated: {stats['query_user_logs_generated']}")
        logging.info(f"Total logs generated: {stats['join_game_logs_generated'] + stats['query_user_logs_generated']}")
        logging.info(f"Successful writes: {stats['successful_writes']}")
        logging.info(f"Failed writes: {stats['failed_writes']}")
        
        # Calculate file size
        if os.path.exists(NDJSON_LOG_FILE):
            ndjson_size = os.path.getsize(NDJSON_LOG_FILE) / (1024 * 1024)  # Size in MB
            logging.info(f"Final NDJSON log file size: {ndjson_size:.2f} MB")
        
        if runtime > 0:
            rate = (stats['join_game_logs_generated'] + stats['query_user_logs_generated']) / runtime
            logging.info(f"Average generation rate: {rate:.2f} logs/second")
        
        logging.info("Log generation stopped")

if __name__ == "__main__":
    main()
