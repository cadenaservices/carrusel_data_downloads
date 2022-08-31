import os
import pathlib

from dotenv import dotenv_values


def get_settings():
    return BaseSettings()


class BaseSettings:
    # Base path of the project to generate absolute paths dynamically
    PROJECT_DIR = pathlib.Path(os.path.dirname(os.path.abspath(__file__)))
    
    # FOOTBALL API URL
    FOOTBALL_API_URL = "https://v3.football.api-sports.io/"

    # CURRENT SEASON
    # This is the year the season begins
    CURRENT_SEASON = 2022  # e.g.: For the 2022/2023 season, use the first year --> 2022

    # ACTIVE LEAGUES
    # List here the ids of the leagues to use (to download fixtures, etc).
    # Each League division is considered a "league", e.g.: La Liga 1ª división, and La 
    # Liga 2ª división, each has their own league id
    # Currently Enabled:
    #     - 140: La Liga: Primera División
    #     - 435: Primera RFEF Group 1
    #     -  39: Premier League
    #     -   2: UEFA Champions League
    #     -   3: UEFA Europa League
    ACTIVE_LEAGUES = [2, 3, 39, 140, 435]

    def __init__(self):
        self.load_secrets()

    def load_secrets(self):
        # Load the secrets for this environment
        # FOOTBAL_API_KEY
        secrets = dotenv_values(".env.secrets")
        for key, value in secrets.items():
            setattr(self, key, value)
