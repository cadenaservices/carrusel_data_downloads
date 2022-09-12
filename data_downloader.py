#!/usr/bin/env python
from copy import deepcopy
import datetime
import json
import time

import requests
from conf import get_settings


settings = get_settings()


# ### API FUNCTIONS ### #

def check_api_limits():
    print("WARNING: check_api_limits() can have a delay of a couple of minutes updating data.\nAvoid making many calls in rapid series trusting the info this function provides.")
    # This request does not count towards the daily limit
    headers = {
        'x-rapidapi-host': settings.FOOTBALL_API_URL,
        'x-rapidapi-key': settings.FOOTBALL_API_KEY,
    }
    endpoint = settings.FOOTBALL_API_URL + "status"
    response = requests.get(endpoint, headers=headers)
    response_json = response.json()
    if response.status_code==200 and not response_json.get("errors"):
        limits = response_json["response"]["requests"]
        limits.update({
            "remaining": limits['limit_day'] - limits['current']
        })

        return limits
    else:
        print("Something went wrong!")
        raise Exception(str(response.__dict__))


def download(endpoint, params=None, endpoint_has_no_pagination=True, download_datetime=''):
    if not params:
        params = {}

    # download_datetime is meant to use the same date folder when multiple downloads are called for the same request
    # download_datetime format is: %Y%m%d_%H%M%SZ  --> e.g.: 20220831_101914Z
    # if not provided, "now" time is used each time this function is called
    if not download_datetime:
        now = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%SZ")
    else:
        now = download_datetime

    # Order params by key
    sorted_keys = sorted(params.keys())
    sorted_params = {key:params[key] for key in sorted_keys}

    # Build get_string
    get_string_items = []
    for pk, pv in sorted_params.items():
        get_string_items.append(str(pk) + "=" + str(pv))
    get_string = "&".join(get_string_items)
        
    # Build file_name
    if not params:
        file_name = "no_params"
    else:
        file_name = get_string.replace("=", "_").replace("&", "__")

    headers = {
        'x-rapidapi-host': settings.FOOTBALL_API_URL,
        'x-rapidapi-key': settings.FOOTBALL_API_KEY,
    }
    dest_folder = settings.PROJECT_DIR / "raw_data" / endpoint / f"{now}"
    # Ensure destination folder exists
    dest_folder.mkdir(parents=True, exist_ok=True)
    
    paths_to_return = []
    page_num = 1
    # Do-while
    while True:
        limits = check_api_limits()
        if not limits["remaining"]:
            raise Exception(f"Limit reached. No requests left for today. Daily limit is {limits['limit_day']} requests.")
        if limits["remaining"] <= 20 and not bypass_requests_limit_failsafe:
            # We avoid making requests if remaining requests for the day are 20 or less
            # to leave room and avoid hitting the limit unadvertedly
            raise Exception(f"Failsafe triggered. Only 20 or less request left, so only manual downloads with 'bypass_requests_limit_failsafe=True'")

        time.sleep(6)  # Sleep to avoid hitting API max connections limit. Cap to 10 per minute
        dest_file = dest_folder / f"{file_name}__p{page_num}.json"
        with open(dest_file, "w+") as f:
            if endpoint_has_no_pagination:
                endpoint = settings.FOOTBALL_API_URL + f"{endpoint}?{get_string}"
            else:
                endpoint = settings.FOOTBALL_API_URL + f"{endpoint}?{get_string}&page={page_num}"
            print(f"Making request to: {endpoint}")
            response = requests.get(endpoint, headers=headers)
            response_json = response.json()
            if response.status_code==200 and not response_json.get("errors"):
                formatted_response = json.dumps(response_json, indent=2)
                f.write(formatted_response)
                paths_to_return.append(dest_file)
            else:
                print("Something went wrong!")
                raise Exception(str(response.__dict__))

        # Update current and total pages
        current_page = response_json['paging']['current']
        total_pages = response_json['paging']['total']

        # Exit do-while if already got the last page
        if endpoint_has_no_pagination or current_page == total_pages:
            break
        else:
            page_num += 1
    return paths_to_return

# ### END OF API FUNCTIONS ### #


# ### DATA NORMALIZING FUNCTIONS ### #

def normalize_leagues_and_countries_data(data_paths=None):
    if not data_paths:
        raise Exception("No data paths provided")

    # Normalize leagues and countries
    countries_info = {}
    leagues_info = {}
    for file_path in data_paths:
        with open(file_path, 'r+') as f:
           file_data = json.loads(f.read())
           for league_data in file_data['response']:

               # Data for country
               country_name = league_data["country"]["name"]
               if country_name == "World":
                   # AA is the code we use to refer to "World" that API Football sets as 'null'
                   # It is ISO3166-1 Alpha-2 compliant, as its a code reserved for "user-assigned code"
                   # and would never be used to define nothing official in the standard
                   country_code = "AA"
               else:
                   country_code = league_data["country"]["code"]
               
               countries_info.update({
                   country_code: {
                       "name": country_name
                    }
               })

               # Data for league
               external_id = league_data["league"]["id"]
               leagues_info.update({
                   external_id: {
                        "name": league_data["league"]["name"],
                        "country": country_code
                    }
               })

    dest_folder = settings.PROJECT_DIR / "newest_data"
    dest_folder.mkdir(parents=True, exist_ok=True)
    # Store country data
    with open(dest_folder / "countries_data.json", "w+") as countries_file:
        formatted_countries = json.dumps(countries_info, indent=2)
        countries_file.write(formatted_countries)

    # Store league data
    with open(dest_folder / "leagues_data.json", "w+") as leagues_file:
        formatted_leagues = json.dumps(leagues_info, indent=2)
        leagues_file.write(formatted_leagues)


def normalize_all_matches_for_current_season_and_active_leagues(data_paths=None):
    if not data_paths:
        raise Exception("No data paths provided")

    matches_info = {}
    end_status = ["FT", "AET", "PEN"]
    notend_status = ["TBD", "NS", "1H", "HT", "2H", "ET", "P", "BT", "PST", "LIVE"]
    invalid_status = ["SUSP", "INT", "CANC", "ABD", "AWD", "WO", None]
    api_football_status_translation = {}
    api_football_status_translation.update(dict.fromkeys(end_status, "END"))
    api_football_status_translation.update(dict.fromkeys(notend_status, "NOTEND"))
    api_football_status_translation.update(dict.fromkeys(invalid_status, "INVALID"))
    for file_path in data_paths:
        with open(file_path, 'r+') as f:
            file_data = json.loads(f.read())
            for match_data in file_data['response']:
                external_id = match_data["fixture"]["id"]
                date = datetime.datetime.fromisoformat(match_data["fixture"]["date"]).date().isoformat()
                matches_info.update({
                    external_id: {
                        "date": date,
                        "status": api_football_status_translation[match_data["fixture"]["status"]["short"]], 
                        "home_team_external_id": match_data["teams"]["home"]["id"],
                        "home_team_external_name": match_data["teams"]["home"]["name"],
                        "away_team_external_id": match_data["teams"]["away"]["id"],
                        "away_team_external_name": match_data["teams"]["away"]["name"],
                        "league_id": match_data["league"]["id"],
                    }
                })

    dest_folder = settings.PROJECT_DIR / "newest_data"
    dest_folder.mkdir(parents=True, exist_ok=True)
    # Store country data
    with open(dest_folder / "matches_data.json", "w+") as matches_file:
        formatted_matches = json.dumps(matches_info, indent=2)
        matches_file.write(formatted_matches)


def normalize_squads_for_given_teams(data_paths=None):
    if not data_paths:
        raise Exception("No data paths provided")
    
    players_info = {}
    for file_path in data_paths:
        with open(file_path, 'r+') as f:
            file_data = json.loads(f.read())
            for squad in file_data['response']:
                for player in squad['players']:
                    external_id = player['id']
                    players_info.update({
                        external_id: {
                            "name": player['name'],
                            "position": player['position'].lower()
                        }
                    })

    dest_folder = settings.PROJECT_DIR / "newest_data"
    dest_folder.mkdir(parents=True, exist_ok=True)
    # Store players data
    with open(dest_folder / "players_data.json", "w+") as players_file:
        formatted_players = json.dumps(players_info, indent=2)
        players_file.write(formatted_players)



def normalize_events_for_given_matches(data_paths=None):
    if not data_paths:
        raise Exception("No data paths provided")
    
    events_info = {}
    valid_goals_detail = ["normal goal", "own goal", "penalty"]
    for file_path in data_paths:
        with open(file_path, 'r+') as f:
            file_data = json.loads(f.read())
            match_external_id = file_data['parameters']['fixture']
            events_info.update({match_external_id: []})
            for match_event in file_data['response']:
                # Currently, only scored goals are needed

                if match_event["type"] == "Goal":
                    event_detail = match_event["detail"].lower()
                    if event_detail in valid_goals_detail:
                        events_info[match_external_id].append({
                                "team": match_event["team"]["id"],
                                "player_id": match_event["player"]["id"],
                                "type": "goal",
                                "detail": event_detail
                        })

    dest_folder = settings.PROJECT_DIR / "newest_data"
    dest_folder.mkdir(parents=True, exist_ok=True)
    # Store matches events data
    with open(dest_folder / "events_data.json", "w+") as events_file:
        formatted_events = json.dumps(events_info, indent=2)
        events_file.write(formatted_events)


# ### END OF DATA NORMALIZING FUNCTIONS ### #

# ### COMPLETE DOWNLOAD AND NORMALIZE FUNCTIONS ### #

def download_and_normalize_leagues_and_countries():
    # This function should be called:
    #     - Once per season or year
    #     - Manually if there were some country or league changes that affect us
    data_paths = download("leagues")
    normalize_leagues_and_countries_data(data_paths=data_paths)


def download_and_normalize_all_matches_for_current_season_and_active_leagues():
    # This function should be called:
    #     - Once per year including all desired league ids
    #     - Each time a new league is required to be activated
    now = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%SZ")
    data_paths = []
    season = settings.CURRENT_SEASON
    leagues = settings.ACTIVE_LEAGUES
    for league_id in leagues:
        query_params = {
            "league": league_id,
            "season": season
        }
        data_paths += download("fixtures", params=query_params, download_datetime=now)
    normalize_all_matches_for_current_season_and_active_leagues(data_paths=data_paths)


def download_and_normalize_squads_for_given_teams(team_ids=None):
    # This function should be called:
    #     - Once each round to download the squad of the selected matches
    #     - Each time a new team is required to be downloaded
    now = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%SZ")
    data_paths = []
    if not team_ids:
        raise Exception("Team IDs not provided")

    for team_id in team_ids:
        query_params = {
            "team": team_id
        }
        data_paths += download('players/squads', params=query_params, download_datetime=now)
    normalize_squads_for_given_teams(data_paths=data_paths)


def download_and_normalize_events_from_given_matches(match_ids=None):
    # This function should be called:
    #     - Once after the last match of the round ends
    now = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%SZ")
    data_paths = []
    if not match_ids:
        raise Exception("Match IDs not provided")

    for match_id in match_ids:
        query_params = {
            "fixture": match_id
        }
        data_paths += download('fixtures/events', params=query_params, download_datetime=now)
    normalize_events_for_given_matches(data_paths=data_paths)

# ### END OF COMPLETE DOWNLOAD AND NORMALIZE FUNCTIONS ### #


def get_path_identifier(path):
    # This function receives a path and returns the string resulting from removing 
    # everything before "raw_data" (included) and everything after the "date folder" (included)
    keep_item_flag = False
    id_parts = []
    for part in path.parts:
        if keep_item_flag:
            # Check if date folder
            if part[0] == "2":
                return '/'.join(id_parts)
            else:
                id_parts.append(part)
        else:
            if part == "raw_data":
                keep_item_flag = True
                continue


def deep_dict_merge(a: dict, b: dict) -> dict:
    result = deepcopy(a)
    for bk, bv in b.items():
        av = result.get(bk)
        if isinstance(av, dict) and isinstance(bv, dict):
            result[bk] = deep_dict_merge(av, bv)
        else:
            result[bk] = deepcopy(bv)
    return result


def refresh_normalizations_using_latest_downloaded_data():
    # For every raw data category (fixtures, events, players, leagues, ...)
    # compile paths to the files with the last data available locally in raw_data folder.
    # For example, if we downloaded events for a given fixture three times, we only keep 
    # the path to the last one of that downloads.

    # Path identifiers and function associations
    path_id_associations = {
        "fixtures": normalize_all_matches_for_current_season_and_active_leagues,
        "fixtures/events": normalize_events_for_given_matches,
        "leagues": normalize_leagues_and_countries_data,
        "players/squads": normalize_squads_for_given_teams
    }

    # Get all the latest file paths for the given active_path
    raw_data_path = settings.PROJECT_DIR / "raw_data"
    all_paths = raw_data_path.rglob("**/*")
    # filtered_paths structure
    # {
    #     'players/squad': {
    #         'team_202__p1.json': absolute_path
    #         }
    #     }
    # }
    filtered_paths = {}
    for item in all_paths:
        absolute_path = item.absolute()
        # Path id is the chunk of the path that identifies the type of data, for example:
        # /a/b/c/raw_data/players/squad/20220909Z/team_1__p1.json
        # the identifier of that sample data is "players/squad"
        path_id = get_path_identifier(absolute_path) 
        if absolute_path.name.endswith(".json"):
            current_folder_date = datetime.datetime.strptime(absolute_path.parts[-2], "%Y%m%d_%H%M%SZ")
            current_file_name = absolute_path.parts[-1]
            # Check if we already processed a file with the same name
            previous_file = filtered_paths.get(path_id, {}).get(current_file_name)
            if previous_file:
                # Compare dates of previous and current file paths and keep the newest
                previous_file_date = datetime.datetime.strptime(previous_file.parts[-2], "%Y%m%d_%H%M%SZ")
                if current_folder_date > previous_file_date:
                   filtered_paths = deep_dict_merge(filtered_paths, {
                        path_id: {
                            current_file_name: absolute_path
                        }
                    })
            else:
                filtered_paths = deep_dict_merge(filtered_paths, {
                    path_id: {
                        current_file_name: absolute_path
                    }
                })


    # Loop over each association, get all the latest file paths for each data and pass 
    # them to the associated function to create the normalized files with the newest
    # downloaded data possible
    for path_id, associated_function in path_id_associations.items():
        path_id_data = filtered_paths.get(path_id)
        if path_id_data:
           data_paths = [p for p in path_id_data.values()] 
        
        associated_function(data_paths=data_paths)

