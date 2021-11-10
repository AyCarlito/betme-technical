from pymongo import MongoClient
import requests
import json
import argparse
import schedule
import time

API_KEY = ""
CONNECTION_STRING = ""
in_play_matches = {}

def get_arguments():
    """Command line argument parsing. 

    Returns:
        [Argparse Object]: Object containing command line arguments
    """
    parser = argparse.ArgumentParser(description='Bet.me technical task')
    parser.add_argument("--c", help="Mongodb Connection String")
    parser.add_argument("--k", help="Odds API Key")
    parser.add_argument("--d", help="In-play API request delay (Seconds)", type=int)
    return parser.parse_args()

def get_sports():
    """API call to sports endpoint

    Returns:
        HTTP Response: Returns a list of available sports
    """
    sports_response = requests.get(
    'https://api.the-odds-api.com/v4/sports', 
        params={
            'api_key': API_KEY
        }
    )
    return sports_response

def get_fixtures(sport_key):
    """API call to odds endpoint

    For UK region and H2H market only as per spec

    Args:
        sport_key (str): "A unique slug for the sport."

    Returns:
        HTTP Response: Returns list of live and upcoming games for a given sport, showing bookmaker odds for the specified region and markets
    """
    odds_response = requests.get(
    f'https://api.the-odds-api.com/v4/sports/{sport_key}/odds',
        params={
            'api_key': API_KEY,
            'regions': "uk",
            'markets': "h2h",
        }
    )
    return odds_response

def create_db():
    """Connect to MongoDB specified by connection string

    Returns:
        MongoDB Database
    """
    print("Establishing Database Connection")
    client= MongoClient(CONNECTION_STRING)
    db = client['bet_me']
    return db

def update_sports(db):
    """Add/Update sports collection in database

    Perform API call to sports endpoint. 
    Convert response to JSON.
    Create "sports" collection.
    Insert JSON into sports collection.

    Args:
        db : Connected MongoDB database
    """
    print("Getting sports")
    db.drop_collection('sports')
    collection_sports = db['sports']
    sports = get_sports().json()
    collection_sports.insert_many(sports)
    return

def update_fixtures(db):
    """Add/Update fixtures collection in database

    Perform API Call to sports endpoint to get available sports and convert response to JSON.
    Use sport_key for each sport and perform API call to odds endpoint.
    Insert list of fixtures for each sport into fixtures collection if "Status Code = 200" otherwise 
    output response message.
    
    
    Args:
        db : Connected MongoDB database
    """

    print("Getting fixtures")
    db.drop_collection('fixtures')
    collection_fixtures = db['fixtures']
    sports = get_sports().json()
    total = len(sports)
    for i,sport in enumerate(sports):
        print(f"Populating {sport['key']}: {i+1} of {total} sports")
        response = get_fixtures(sport['key'])
        if response.status_code == 200:
            collection_fixtures.insert_many(response.json())
        else:
            print(response.json()['message'])
    return

def update_inplay(db):
    """Update Inplay matches

    Get inplay matches by performing API call to odds endpoint with "sport_key" set to "upcoming". 
    For each inplay match, find and replace in db.

    Args:
        db : Connected MongoDB database]
    """
    print("Updating Inplay matches")
    global in_play_matches
    response = get_fixtures("upcoming")
    if response.status_code == 200:
        in_play_matches = response.json()
        for match in in_play_matches:
            db.fixtures.replace_one({"id": match['id']}, match)
    else:
        print(response.json()['message'])
    return



def main():
    """Main Function

    Get user arguments.
    Set API_KEY and CONNECTION_STRING to command line arguments.
    Create Database.
    Get list of available sports on startup.
    Get list of fixtures for each sport on startup.
    Update inplay matches every x seconds (where x is the command line argument specifiying the delay)
    Update list of fixtures every hour.
    """
    global API_KEY, CONNECTION_STRING

    args = get_arguments()
 
    API_KEY = args.k
    CONNECTION_STRING = args.c

    db = create_db()
    update_sports(db)
    update_fixtures(db)

    schedule.every(args.d).seconds.do(update_inplay, db)
    schedule.every().hour.do(update_fixtures, db)

    while True:
        schedule.run_pending()
        time.sleep(1)
   

if __name__ == "__main__":
    main()

