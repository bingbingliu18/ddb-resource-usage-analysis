import json
import boto3
import random
import uuid
import time
import argparse
from datetime import datetime, timedelta
from faker import Faker
import multiprocessing
from concurrent.futures import ThreadPoolExecutor
import threading

# Initialize Faker to generate realistic data
fake = Faker()

# Initialize DynamoDB resource
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('battle-royale')

# Constants
TOTAL_RECORDS = 1000000  # 固定为100万条记录
BATCH_SIZE = 25  # DynamoDB allows max 25 items per batch write
CHUNK_SIZE = 1000  # Process 1000 records at a time
DEFAULT_NUM_THREADS = 20  # Default number of threads to use

# Global counter with lock for thread safety
successful_records = 0
records_lock = threading.Lock()

def generate_user_record():
    """Generate a random user record similar to the existing data"""
    username = f"{fake.user_name()}{random.randint(1, 999)}"
    
    user = {
        "PK": f"USER#{username}",
        "SK": f"#METADATA#{username}",
        "address": fake.address().replace('\n', ' '),
        "birthdate": fake.date_of_birth(minimum_age=18, maximum_age=90).strftime("%Y-%m-%d"),
        "email": fake.email(),
        "name": fake.name(),
        "username": username
    }
    
    return user

def generate_game_record():
    """Generate a random game record according to the specified rules:
    - 1% are open games with people=0 and open_timestamp
    - 99% are started games with people=500
    - 30% of started games are completed
    """
    game_id = str(uuid.uuid4())
    creator = f"{fake.user_name()}{random.randint(1, 999)}"
    create_time = (datetime.now() - timedelta(days=random.randint(0, 365))).strftime("%Y-%m-%d-%H:%M:%S")
    
    # 50 different map names for games
    maps = [
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
    
    # 基本游戏记录，所有游戏都有这些字段
    game = {
        "PK": f"GAME#{game_id}",
        "SK": f"#METADATA#{game_id}",
        "game_id": game_id,
        "map": random.choice(maps),  # 所有游戏都有map字段
        "create_time": create_time,
        "creator": creator
    }
    
    # Determine if this is an open game (1% chance)
    is_open_game = random.random() < 0.01
    
    if is_open_game:
        # Open games have people=0 and open_timestamp
        game["people"] = 0
        game["open_timestamp"] = create_time
    else:
        # All started games have people=500 and start_time
        game["people"] = 500
        game["start_time"] = (datetime.strptime(create_time, "%Y-%m-%d-%H:%M:%S") + 
                             timedelta(minutes=random.randint(5, 15))).strftime("%Y-%m-%d-%H:%M:%S")
        
        # 30% of started games are completed
        if random.random() < 0.3:
            game["end_time"] = (datetime.strptime(game["start_time"], "%Y-%m-%d-%H:%M:%S") + 
                               timedelta(minutes=random.randint(10, 30))).strftime("%Y-%m-%d-%H:%M:%S")
            
            # Add winners for completed games
            players = [f"{fake.user_name()}{random.randint(1, 999)}" for _ in range(3)]
            game["gold"] = players[0]
            game["silver"] = players[1]
            game["bronze"] = players[2]
    
    return game

def generate_game_player_record(game_id):
    """Generate a game-player relationship record"""
    username = f"{fake.user_name()}{random.randint(1, 999)}"
    
    record = {
        "PK": f"GAME#{game_id}",
        "SK": f"USER#{username}",
        "username": username,
        "game_id": game_id
    }
    
    # Add place for winners (small chance)
    if random.random() < 0.06:  # ~6% chance to be a winner
        places = ["gold", "silver", "bronze"]
        record["place"] = random.choice(places)
    
    return record

def generate_records(num_records):
    """Generate a mix of user, game, and game-player records"""
    records = []
    
    for _ in range(num_records):
        record_type = random.random()
        
        if record_type < 0.6:  # 60% chance for user records
            records.append(generate_user_record())
        elif record_type < 0.7:  # 10% chance for game metadata
            records.append(generate_game_record())
        else:  # 30% chance for game-player relationship
            # Generate a random game ID
            game_id = str(uuid.uuid4())
            records.append(generate_game_player_record(game_id))
    
    return records

def write_batch_to_dynamodb(records):
    """Write a batch of records to DynamoDB using batch_writer"""
    global successful_records
    
    try:
        with table.batch_writer() as batch:
            for record in records:
                batch.put_item(Item=record)
        
        # Update the global counter in a thread-safe way
        with records_lock:
            successful_records += len(records)
            
        return len(records)
    except Exception as e:
        print(f"Error writing batch to DynamoDB: {e}")
        # Retry logic could be added here
        return 0

def process_chunk(chunk_id, target_records):
    """Process a chunk of records"""
    global successful_records
    
    start_time = time.time()
    records_written = 0
    
    # Check if we've already reached the target
    with records_lock:
        if successful_records >= target_records:
            return 0
    
    # Generate records for this chunk
    chunk_size = min(CHUNK_SIZE, target_records - successful_records)
    if chunk_size <= 0:
        return 0
        
    records = generate_records(chunk_size)
    
    # Write records in batches
    for i in range(0, len(records), BATCH_SIZE):
        # Check if we've reached the target before processing this batch
        with records_lock:
            if successful_records >= target_records:
                break
                
        # Calculate how many more records we need
        with records_lock:
            remaining = target_records - successful_records
            
        # Adjust batch size to not exceed the target
        batch = records[i:i+min(BATCH_SIZE, remaining)]
        if not batch:  # Skip if batch is empty
            continue
            
        records_written += write_batch_to_dynamodb(batch)
        
        # Small delay to avoid throttling
        time.sleep(0.1)
    
    elapsed = time.time() - start_time
    print(f"Chunk {chunk_id}: Wrote {records_written} records in {elapsed:.2f} seconds")
    return records_written

def main():
    """Main function to generate and insert exactly 1 million records with configurable thread count"""
    global successful_records
    successful_records = 0  # Reset counter at start
    
    # Parse command line arguments - only threads parameter is configurable
    parser = argparse.ArgumentParser(description='Generate and insert exactly 1 million records into DynamoDB')
    parser.add_argument('--threads', type=int, default=DEFAULT_NUM_THREADS,
                        help=f'Number of concurrent threads to use (default: {DEFAULT_NUM_THREADS})')
    args = parser.parse_args()
    
    # Use the provided thread count, but fix record count to 1 million
    num_threads = args.threads
    target_records = TOTAL_RECORDS  # Fixed at 1 million
    
    start_time = time.time()
    
    print(f"Starting to generate and insert exactly 1,000,000 records using {num_threads} threads...")
    
    # Continue processing chunks until we reach the target
    chunk_id = 0
    while successful_records < target_records:
        # Calculate how many chunks to process in parallel
        remaining = target_records - successful_records
        num_chunks = min(num_threads, (remaining + CHUNK_SIZE - 1) // CHUNK_SIZE)
        
        if num_chunks <= 0:
            break
            
        # Process chunks in parallel
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = [executor.submit(process_chunk, chunk_id + i, target_records) 
                      for i in range(num_chunks)]
            
            # Wait for all futures to complete
            for future in futures:
                future.result()
                
        chunk_id += num_chunks
        
        # Print progress update
        print(f"Progress: {successful_records}/{target_records} records inserted ({(successful_records/target_records)*100:.2f}%)")
    
    elapsed = time.time() - start_time
    print(f"Completed! Wrote exactly {successful_records} records in {elapsed:.2f} seconds")
    print(f"Average rate: {successful_records / elapsed:.2f} records/second")
    print(f"Using {num_threads} threads")
    
    # Final verification
    if successful_records == target_records:
        print(f"SUCCESS: Inserted exactly 1,000,000 records as requested.")
    else:
        print(f"WARNING: Inserted {successful_records} records, which differs from the target of 1,000,000.")

if __name__ == "__main__":
    main()
