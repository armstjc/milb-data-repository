import json
import time
from datetime import datetime
from urllib.request import urlopen

import pandas as pd
from tqdm import tqdm


def get_milb_team_list(season: int, save=True):
    """

    """
    row_df = pd.DataFrame()
    teams_df = pd.DataFrame()

    now = datetime.now()

    if season > (now.year+1):
        raise ValueError(f'`season` cannot be greater than {now.year+1}.')

    teams_url = f"https://statsapi.mlb.com/api/v1/teams?season={season}"
    response = urlopen(teams_url)
    time.sleep(2)

    if response.code == 200:
        pass
    elif response.code == 403:
        raise ConnectionRefusedError(
            'The MiLB API is actively refusing your connection.\nHTTP Error Code:\t403')
    else:
        raise ConnectionError(
            f'Could not establish a connection to the MiLB API.\nHTTP Error Code:\t{response.code}')

    json_data = json.loads(response.read())

    for team in tqdm(json_data['teams']):
        team_id = team['id']
        try:
            team_full_name = team['name']
        except:
            team_full_name = None

        team_link = team['link']

        try:
            team_venue_id = team['venue']['id']
        except:
            team_venue_id = None

        try:
            team_venue_name = team['venue']['name']
        except:
            team_venue_name = None

        try:
            team_venue_link = team['venue']['link']
        except:
            team_venue_link = None

        try:
            team_code = team['teamCode']
        except:
            team_code = None

        try:
            file_code = team['fileCode']
        except:
            file_code = None

        try:
            team_abbreviation = team['abbreviation']
        except:
            team_abbreviation = None

        try:
            team_short_name = team['shortName']
        except:
            team_short_name = None

        try:
            team_nickname = team['teamName']
        except:
            team_nickname = None

        try:
            team_location = team['locationName']
        except:
            team_location = None

        try:
            first_year_of_play = int(team['firstYearOfPlay'])
        except:
            first_year_of_play = None

        try:
            league_id = team['league']['id']
        except:
            league_id = None

        try:
            league_name = team['league']['name']
        except:
            league_name = None

        try:
            league_link = team['league']['link']
        except:
            league_link = None

        try:
            division_id = team['division']['id']
        except:
            division_id = None

        try:
            division_name = team['division']['name']
        except:
            division_name = None

        try:
            division_link = team['division']['link']
        except:
            division_link = None

        try:
            sport_id = team['sport']['id']
        except:
            sport_id = None

        try:
            sport_name = team['sport']['name']
        except:
            sport_name = None

        try:
            sport_link = team['sport']['link']
        except:
            sport_link = None

        # College teams are returned in this API endpoint.
        # Those teams do not have a parent org.
        try:
            parent_org_name = team['parentOrgName']
        except:
            parent_org_name = None

        try:
            parent_org_id = team['parentOrgId']
        except:
            parent_org_id = None

        try:
            franchise_name = team['franchiseName']
        except:
            franchise_name = None

        try:
            club_name = team['clubName']
        except:
            club_name = None

        is_active_team = team['active']
        row_df = pd.DataFrame(
            {
                'team_id': team_id,
                'team_full_name': team_full_name,
                'team_link': team_link,
                'season': season,
                'team_venue_id': team_venue_id,
                'team_venue_name': team_venue_name,
                'team_venue_link': team_venue_link,
                'team_code': team_code,
                'file_code': file_code,
                'team_abbreviation': team_abbreviation,
                'team_nickname': team_nickname,
                'team_location': team_location,
                'first_year_of_play': first_year_of_play,
                'league_id': league_id,
                'league_name': league_name,
                'league_link': league_link,
                'division_id': division_id,
                'division_name': division_name,
                'division_link': division_link,
                'sport_id': sport_id,
                'sport_name': sport_name,
                'sport_link': sport_link,
                'team_short_name': team_short_name,
                'parent_org_name': parent_org_name,
                'parent_org_id': parent_org_id,
                'franchise_name': franchise_name,
                'club_name': club_name,
                'is_active_team': is_active_team

            }, index=[0]
        )

        del team_id, team_full_name, team_link, team_venue_id, \
            team_venue_name, team_venue_link, team_code, \
            team_abbreviation, team_nickname, team_location, \
            first_year_of_play, league_id, league_name, \
            league_link, division_id, division_name, division_link,\
            sport_id, sport_name, sport_link, team_short_name,\
            parent_org_name, parent_org_id, franchise_name,\
            club_name, is_active_team
        teams_df = pd.concat([teams_df, row_df], ignore_index=True)

    if save == True:
        teams_df.to_csv(f'teams/{season}_teams.csv', index=False)

    return teams_df


if __name__ == "__main__":
    now = datetime.now()
    # for i in range(now.year, now.year+1):
    for i in range(2015, now.year+1):
        print(f'Getting a list of teams in the MLB API for the {i} season.')
        get_milb_team_list(i)
