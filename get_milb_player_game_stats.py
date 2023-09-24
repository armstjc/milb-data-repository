import argparse
import json
import os
import platform
import time
from datetime import datetime
from urllib.request import urlopen

import pandas as pd
from tqdm import tqdm

from get_milb_schedule import get_milb_schedule


def get_milb_player_game_stats(game_id: int, cache_data=False, cache_dir=""):
    """
    Retrives and parses the player game box score stats from a valid
    MiLB game ID.

    Parameters
    ----------
    `game_id`: (int, mandatory):
        The MiLB game ID you want player game box score stats from.

    `cache_data`: (bool, optional) = `False`:
        Optional boolean flag. 
        If set to `True`, data downloaded by this function will be cached to 
        a folder named `./milb/`. This folder will either be located in the user's
        home directory, 
        or in a existing directory specified by the optional argument `cache_dir`.

    `cache_dir`: (str, optional) = `""`:
        Optional string. If not set to `""` or `None`, this will be the directory used
        to cache data if `cache_data` is set to `True`. 
        This directory must exist prior to running this function!

    Returns
    ----------
    A pandas `DataFrame` object containing player game box score stats from 
    the MiLB game ID.
    """
    if cache_data == True and (cache_dir == "" or cache_dir == None):
        home_dir = os.path.expanduser('~')

        try:
            os.mkdir(f"{home_dir}/.milb/")
        except:
            pass

        try:
            os.mkdir(f"{home_dir}/.milb/pbp/")
            os.mkdir(f"{home_dir}/.milb/lineups/")
        except:
            pass

    elif cache_data == True and (cache_dir != "" or cache_dir != None):
        try:
            os.mkdir(f"{cache_dir}/.milb/")
        except:
            pass

        try:
            os.mkdir(f"{cache_dir}/.milb/pbp/")
            os.mkdir(f"{cache_dir}/.milb/lineups/")
        except:
            pass

    game_df = pd.DataFrame()
    row_df = pd.DataFrame()
    game_url = f"https://statsapi.mlb.com/api/v1.1/game/{game_id}/feed/live?"

    if cache_data == True and (cache_dir == "" or cache_dir == None):
        # Cached files, default directory
        try:
            with open(f"{home_dir}/.milb/pbp/{game_id}.json", 'r') as f:
                json_string = f.read()

            json_data = json.loads(json_string)
        except:
            response = urlopen(game_url)
            time.sleep(1)

            if response.code == 200:
                pass
            elif response.code == 403:
                raise ConnectionRefusedError(
                    'The MiLB API is actively refusing your connection.\nHTTP Error Code:\t403')
            else:
                raise ConnectionError(
                    f'Could not establish a connection to the MiLB API.\nHTTP Error Code:\t{response.code}')

            json_data = json.loads(response.read())
            with open(f"{home_dir}/.milb/pbp/{game_id}.json", 'w+') as f:
                f.write(json.dumps(json_data, indent=2))

    elif cache_data == True and (cache_dir != "" or cache_dir != None):
        try:
            with open(f"{cache_dir}/.milb/pbp/{game_id}.json", 'r') as f:
                json_string = f.read()

            json_data = json.loads(json_string)
        except:
            response = urlopen(game_url)
            time.sleep(1)

            if response.code == 200:
                pass
            elif response.code == 403:
                raise ConnectionRefusedError(
                    'The MiLB API is actively refusing your connection.\nHTTP Error Code:\t403')
            else:
                raise ConnectionError(
                    f'Could not establish a connection to the MiLB API.\nHTTP Error Code:\t{response.code}')

            json_data = json.loads(response.read())
            with open(f"{cache_dir}/.milb/pbp/{game_id}.json", 'w+') as f:
                f.write(json.dumps(json_data, indent=2))

    else:
        # No cached files used.
        response = urlopen(game_url)
        time.sleep(1)

        if response.code == 200:
            pass
        elif response.code == 403:
            raise ConnectionRefusedError(
                'The MiLB API is actively refusing your connection.\nHTTP Error Code:\t403')
        else:
            raise ConnectionError(
                f'Could not establish a connection to the MiLB API.\nHTTP Error Code:\t{response.code}')

        json_data = json.loads(response.read())

    # json_data = json.loads(response.read())
    if len(json_data) == 0:
        print(f'\nCould not get player game stats data for game ID {game_id}')
        return pd.DataFrame()

    game_type = json_data['gameData']['game']['type']
    game_date = json_data['gameData']['datetime']['officialDate']
    game_date = datetime.strptime(game_date, "%Y-%m-%d")
    game_date_str = game_date.strftime("%Y-%m-%d")

    league_id = json_data['gameData']['teams']['away']['league']['id']
    league_name = json_data['gameData']['teams']['away']['league']['name']

    league_level_id = json_data['gameData']['teams']['away']['sport']['id']
    league_level_name = json_data['gameData']['teams']['away']['sport']['name']

    try:
        away_team_org_id = json_data['gameData']['teams']['away']['parentOrgId']
    except:
        away_team_org_id = None

    try:
        away_team_org_name = json_data['gameData']['teams']['away']['parentOrgName']
    except:
        away_team_org_name = None

    try:
        away_team_id = json_data['gameData']['teams']['away']['id']
    except:
        away_team_id = None

    try:
        away_team_abv = json_data['gameData']['teams']['away']['abbreviation']
    except:
        away_team_abv = None

    try:
        away_team_name = json_data['gameData']['teams']['away']['name']
    except:
        away_team_name = None

    try:
        home_team_org_id = json_data['gameData']['teams']['home']['parentOrgId']
    except:
        home_team_org_id = None

    try:
        home_team_org_name = json_data['gameData']['teams']['home']['parentOrgName']
    except:
        home_team_org_name = None

    try:
        home_team_id = json_data['gameData']['teams']['home']['id']
    except:
        home_team_id = None

    try:
        home_team_abv = json_data['gameData']['teams']['home']['abbreviation']
    except:
        home_team_abv = None

    try:
        home_team_name = json_data['gameData']['teams']['home']['name']
    except:
        home_team_name = None

    try:
        away_runs = json_data['gameData']['linescore']['teams']['away']['runs']
    except:
        away_runs = 0

    try:
        home_runs = json_data['gameData']['linescore']['teams']['home']['runs']
    except:
        home_runs = 0

    away_player_stats = json_data['liveData']['boxscore']['teams']['away']['players']
    home_player_stats = json_data['liveData']['boxscore']['teams']['home']['players']

    for (key, value) in away_player_stats.items():
        loc = "A"
        if home_runs == away_runs:
            final_score_str = f"T {away_runs}-{home_runs}"
        elif away_runs > home_runs:
            final_score_str = f"W {away_runs}-{home_runs}"
        elif away_runs < home_runs:
            final_score_str = f"L {away_runs}-{home_runs}"

        player_pos = ""
        player_pos_num = 0

        try:
            for i in value['allPositions']:
                player_pos_num += 1

                if player_pos_num == 1:
                    player_pos = i['abbreviation']
                else:
                    player_pos = f"{player_pos}/{i['abbreviation']}"
        except:
            player_pos = None

        del player_pos_num

        player_id = value['person']['id']
        try:
            player_jersey_number = str(value['jerseyNumber'])
        except:
            player_jersey_number = None
        try:
            player_full_name = value['person']['fullName']
        except:
            player_full_name = None
        try:
            player_batitng_order = str(value['battingOrder'])
        except:
            player_batitng_order = None

        row_df = pd.DataFrame(
            {
                'season': game_date.year,
                'game_id': game_id,
                'game_date': game_date,
                'game_type': game_type,
                'league_id': league_id,
                'league_name': league_name,
                'league_level_id': league_level_id,
                'league_level_name': league_level_name,
                'team_org_id': away_team_org_id,
                'team_org_name': away_team_org_name,
                'team_id': away_team_id,
                'team_abv': away_team_abv,
                'team_name': away_team_name,
                'loc': loc,
                'opp_org_id': home_team_org_id,
                'opp_org_name': home_team_org_name,
                'opp_id': home_team_id,
                'opp_abv': home_team_abv,
                'opp_name': home_team_name,
                'team_runs': away_runs,
                'opp_runs': home_runs,
                'score': final_score_str,
                'player_id': player_id,
                'player_jersey_number': player_jersey_number,
                'player_full_name': player_full_name,
                'player_position': player_pos,
                'player_batitng_order': player_batitng_order
            },
            index=[0]
        )

        player_data = value['stats']

        if len(player_data['batting']) > 0:
            row_df['batting_G'] = player_data['batting']['gamesPlayed']
            row_df['batting_PA'] = player_data['batting']['plateAppearances']
            row_df['batting_AB'] = player_data['batting']['atBats']
            row_df['batting_R'] = player_data['batting']['runs']
            row_df['batting_H'] = player_data['batting']['hits']
            row_df['batting_2B'] = player_data['batting']['doubles']
            row_df['batting_3B'] = player_data['batting']['triples']
            row_df['batting_HR'] = player_data['batting']['homeRuns']
            row_df['batting_RBI'] = player_data['batting']['rbi']
            row_df['batting_SB'] = player_data['batting']['stolenBases']
            row_df['batting_CS'] = player_data['batting']['caughtStealing']
            row_df['batting_BB'] = player_data['batting']['baseOnBalls']
            row_df['batting_IBB'] = player_data['batting']['intentionalWalks']
            row_df['batting_SO'] = player_data['batting']['strikeOuts']
            row_df['batting_TB'] = player_data['batting']['totalBases']
            row_df['batting_GiDP'] = player_data['batting']['groundIntoDoublePlay']
            row_df['batting_GiTP'] = player_data['batting']['groundIntoTriplePlay']
            row_df['batting_HBP'] = player_data['batting']['hitByPitch']
            row_df['batting_SH'] = player_data['batting']['sacBunts']
            row_df['batting_SF'] = player_data['batting']['sacFlies']
            row_df['batting_CI'] = player_data['batting']['catchersInterference']
            row_df['batting_FO'] = player_data['batting']['flyOuts']
            row_df['batting_GO'] = player_data['batting']['groundOuts']
            row_df['batting_LOB'] = player_data['batting']['leftOnBase']

        if len(player_data['pitching']) > 0:
            row_df['batting_G'] = player_data['pitching']['gamesPlayed']
            row_df['pitching_G'] = player_data['pitching']['gamesPitched']
            row_df['pitching_GS'] = player_data['pitching']['gamesStarted']
            row_df['pitching_GF'] = player_data['pitching']['gamesFinished']
            row_df['pitching_CG'] = player_data['pitching']['completeGames']
            row_df['pitching_SHO'] = player_data['pitching']['shutouts']

            row_df['pitching_W'] = player_data['pitching']['wins']
            row_df['pitching_L'] = player_data['pitching']['losses']

            row_df['pitching_SVO'] = player_data['pitching']['saveOpportunities']
            row_df['pitching_SV'] = player_data['pitching']['saves']
            row_df['pitching_BS'] = player_data['pitching']['blownSaves']
            row_df['pitching_HLD'] = player_data['pitching']['holds']

            row_df['pitching_IP'] = round(player_data['pitching']['outs']/3, 3)
            row_df['pitching_IP_str'] = str(
                player_data['pitching']['inningsPitched'])

            row_df['pitching_R'] = player_data['pitching']['runs']
            row_df['pitching_ER'] = player_data['pitching']['earnedRuns']
            row_df['pitching_BF'] = player_data['pitching']['battersFaced']
            row_df['pitching_AB'] = player_data['pitching']['atBats']
            row_df['pitching_H'] = player_data['pitching']['hits']
            row_df['pitching_2B'] = player_data['pitching']['doubles']
            row_df['pitching_3B'] = player_data['pitching']['triples']
            row_df['pitching_HR'] = player_data['pitching']['homeRuns']
            row_df['pitching_RBI'] = player_data['pitching']['rbi']
            row_df['pitching_BB'] = player_data['pitching']['baseOnBalls']
            row_df['pitching_IBB'] = player_data['pitching']['intentionalWalks']
            row_df['pitching_SO'] = player_data['pitching']['strikeOuts']
            row_df['pitching_HBP'] = player_data['pitching']['hitByPitch']
            row_df['pitching_BK'] = player_data['pitching']['balks']
            row_df['pitching_WP'] = player_data['pitching']['wildPitches']

            row_df['pitching_GO'] = player_data['pitching']['groundOuts']
            row_df['pitching_AO'] = player_data['pitching']['airOuts']
            row_df['pitching_SB'] = player_data['pitching']['stolenBases']
            row_df['pitching_CS'] = player_data['pitching']['caughtStealing']

            row_df['pitching_SH'] = player_data['pitching']['sacBunts']
            row_df['pitching_SF'] = player_data['pitching']['sacFlies']
            row_df['pitching_CI'] = player_data['pitching']['catchersInterference']
            row_df['pitching_PB'] = player_data['pitching']['passedBall']
            row_df['pitching_PK'] = player_data['pitching']['pickoffs']

            row_df['pitching_IR'] = player_data['pitching']['inheritedRunners']
            row_df['pitching_IRS'] = player_data['pitching']['inheritedRunnersScored']

            row_df['pitching_PI'] = player_data['pitching']['numberOfPitches']
            row_df['pitching_PI_strikes'] = player_data['pitching']['strikes']
            row_df['pitching_PI_balls'] = player_data['pitching']['balls']

        game_df = pd.concat([game_df, row_df], ignore_index=True)

    for (key, value) in home_player_stats.items():
        loc = "H"

        if home_runs == away_runs:
            final_score_str = f"T {home_runs}-{away_runs}"
        elif home_runs > away_runs:
            final_score_str = f"W {home_runs}-{away_runs}"
        elif home_runs < away_runs:
            final_score_str = f"L {home_runs}-{away_runs}"

        player_pos = ""
        player_pos_num = 0

        try:
            for i in value['allPositions']:
                player_pos_num += 1

                if player_pos_num == 1:
                    player_pos = i['abbreviation']
                else:
                    player_pos = f"{player_pos}/{i['abbreviation']}"
        except:
            player_pos = None

        del player_pos_num

        player_id = value['person']['id']
        try:
            player_jersey_number = str(value['jerseyNumber'])
        except:
            player_jersey_number = None

        try:
            player_full_name = value['person']['fullName']
        except:
            player_full_name = None

        try:
            player_batitng_order = str(value['battingOrder'])
        except:
            player_batitng_order = None

        row_df = pd.DataFrame(
            {
                'season': game_date.year,
                'game_id': game_id,
                'game_date': game_date,
                'game_type': game_type,
                'league_id': league_id,
                'league_name': league_name,
                'league_level_id': league_level_id,
                'league_level_name': league_level_name,
                'team_org_id': home_team_org_id,
                'team_org_name': home_team_org_name,
                'team_id': home_team_id,
                'team_abv': home_team_abv,
                'team_name': home_team_name,
                'loc': loc,
                'opp_org_id': away_team_org_id,
                'opp_org_name': away_team_org_name,
                'opp_id': away_team_id,
                'opp_abv': away_team_abv,
                'opp_name': away_team_name,
                'team_runs': home_runs,
                'opp_runs': away_runs,
                'score': final_score_str,
                'player_id': player_id,
                'player_jersey_number': player_jersey_number,
                'player_full_name': player_full_name,
                'player_position': player_pos,
                'player_batitng_order': player_batitng_order
            },
            index=[0]
        )

        player_data = value['stats']

        if len(player_data['batting']) > 0:
            row_df['batting_G'] = player_data['batting']['gamesPlayed']
            row_df['batting_PA'] = player_data['batting']['plateAppearances']
            row_df['batting_AB'] = player_data['batting']['atBats']
            row_df['batting_R'] = player_data['batting']['runs']
            row_df['batting_H'] = player_data['batting']['hits']
            row_df['batting_2B'] = player_data['batting']['doubles']
            row_df['batting_3B'] = player_data['batting']['triples']
            row_df['batting_HR'] = player_data['batting']['homeRuns']
            row_df['batting_RBI'] = player_data['batting']['rbi']
            row_df['batting_SB'] = player_data['batting']['stolenBases']
            row_df['batting_CS'] = player_data['batting']['caughtStealing']
            row_df['batting_BB'] = player_data['batting']['baseOnBalls']
            row_df['batting_IBB'] = player_data['batting']['intentionalWalks']
            row_df['batting_SO'] = player_data['batting']['strikeOuts']
            row_df['batting_TB'] = player_data['batting']['totalBases']
            row_df['batting_GiDP'] = player_data['batting']['groundIntoDoublePlay']
            row_df['batting_GiTP'] = player_data['batting']['groundIntoTriplePlay']
            row_df['batting_HBP'] = player_data['batting']['hitByPitch']
            row_df['batting_SH'] = player_data['batting']['sacBunts']
            row_df['batting_SF'] = player_data['batting']['sacFlies']
            row_df['batting_CI'] = player_data['batting']['catchersInterference']
            row_df['batting_FO'] = player_data['batting']['flyOuts']
            row_df['batting_GO'] = player_data['batting']['groundOuts']
            row_df['batting_LOB'] = player_data['batting']['leftOnBase']

        if len(player_data['pitching']) > 0:
            row_df['batting_G'] = player_data['pitching']['gamesPlayed']
            row_df['pitching_G'] = player_data['pitching']['gamesPitched']
            row_df['pitching_GS'] = player_data['pitching']['gamesStarted']
            row_df['pitching_GF'] = player_data['pitching']['gamesFinished']
            row_df['pitching_CG'] = player_data['pitching']['completeGames']
            row_df['pitching_SHO'] = player_data['pitching']['shutouts']

            row_df['pitching_W'] = player_data['pitching']['wins']
            row_df['pitching_L'] = player_data['pitching']['losses']

            row_df['pitching_SVO'] = player_data['pitching']['saveOpportunities']
            row_df['pitching_SV'] = player_data['pitching']['saves']
            row_df['pitching_BS'] = player_data['pitching']['blownSaves']
            row_df['pitching_HLD'] = player_data['pitching']['holds']

            row_df['pitching_IP'] = round(player_data['pitching']['outs']/3, 3)
            row_df['pitching_IP_str'] = str(
                player_data['pitching']['inningsPitched'])

            row_df['pitching_R'] = player_data['pitching']['runs']
            row_df['pitching_ER'] = player_data['pitching']['earnedRuns']
            row_df['pitching_BF'] = player_data['pitching']['battersFaced']
            row_df['pitching_AB'] = player_data['pitching']['atBats']
            row_df['pitching_H'] = player_data['pitching']['hits']
            row_df['pitching_2B'] = player_data['pitching']['doubles']
            row_df['pitching_3B'] = player_data['pitching']['triples']
            row_df['pitching_HR'] = player_data['pitching']['homeRuns']
            row_df['pitching_RBI'] = player_data['pitching']['rbi']
            row_df['pitching_BB'] = player_data['pitching']['baseOnBalls']
            row_df['pitching_IBB'] = player_data['pitching']['intentionalWalks']
            row_df['pitching_SO'] = player_data['pitching']['strikeOuts']
            row_df['pitching_HBP'] = player_data['pitching']['hitByPitch']
            row_df['pitching_BK'] = player_data['pitching']['balks']
            row_df['pitching_WP'] = player_data['pitching']['wildPitches']

            row_df['pitching_GO'] = player_data['pitching']['groundOuts']
            row_df['pitching_AO'] = player_data['pitching']['airOuts']
            row_df['pitching_SB'] = player_data['pitching']['stolenBases']
            row_df['pitching_CS'] = player_data['pitching']['caughtStealing']

            row_df['pitching_SH'] = player_data['pitching']['sacBunts']
            row_df['pitching_SF'] = player_data['pitching']['sacFlies']
            row_df['pitching_CI'] = player_data['pitching']['catchersInterference']
            row_df['pitching_PB'] = player_data['pitching']['passedBall']
            row_df['pitching_PK'] = player_data['pitching']['pickoffs']

            row_df['pitching_IR'] = player_data['pitching']['inheritedRunners']
            row_df['pitching_IRS'] = player_data['pitching']['inheritedRunnersScored']

            row_df['pitching_PI'] = player_data['pitching']['numberOfPitches']
            row_df['pitching_PI_strikes'] = player_data['pitching']['strikes']
            row_df['pitching_PI_balls'] = player_data['pitching']['balls']

        game_df = pd.concat([game_df, row_df], ignore_index=True)

    return game_df


def get_month_milb_player_game_stats(season: int, month: int, level="AAA", cache_data=False, cache_dir="", save=True):
    """

    """

    game_df = pd.DataFrame()
    stats_df = pd.DataFrame()
    sched_df = pd.DataFrame()

    if (level.lower() == 'aaa') or (level.lower() == 'triple-a') or (level.lower() == 'triple a'):
        sched_df = get_milb_schedule(season, 'AAA')
    elif (level.lower() == 'aa') or (level.lower() == 'double-a') or (level.lower() == 'double a'):
        sched_df = get_milb_schedule(season, 'AA')
    elif (level.lower() == 'a+') or (level.lower() == 'high-a') or (level.lower() == 'high a'):
        sched_df = get_milb_schedule(season, 'A+')
    elif (level.lower() == 'a') or (level.lower() == 'single-a') or (level.lower() == 'single-a'):
        sched_df = get_milb_schedule(season, 'A')
    elif (level.lower() == 'a-') or (level.lower() == 'short-a') or (level.lower() == 'short a'):
        sched_df = get_milb_schedule(season, 'A-')
    elif (level.lower() == 'rk') or (level.lower() == 'rok') or (level.lower() == 'rookie'):
        sched_df = get_milb_schedule(season, 'rk')

    sched_df = sched_df.loc[sched_df['status_abstract_game_state'] == 'Final']
    sched_df = sched_df.loc[(sched_df['game_month'] == month) & (
        sched_df['status_detailed_state'] != 'Cancelled') & (
        sched_df['status_detailed_state'] != 'Postponed') & (
        sched_df['status_detailed_state'] != 'In Progress') & (
        sched_df['status_detailed_state'] != 'Scheduled')]

    game_ids_arr = sched_df['game_pk'].to_numpy()

    if len(game_ids_arr) > 30 and cache_data == False:
        print('HEY!\nThat\'s a ton of data you want to access.\nPlease cache this data in the future to avoid severe data loss!')

    for game_id in tqdm(game_ids_arr):
        # try:
        #    game_df =  get_milb_player_game_stats(
        #    game_id=game_id, cache_data=cache_data, cache_dir=cache_dir)
        #    stats_df = pd.concat([stats_df, game_df], ignore_index=True)
        # except Exception as e:
        #     print(f'Unhandled use case. Error Details:\n{e}')

        game_df = get_milb_player_game_stats(
            game_id=game_id, cache_data=cache_data, cache_dir=cache_dir)
        stats_df = pd.concat([stats_df, game_df], ignore_index=True)

    if save == True and len(stats_df) > 0:
        stats_df.to_csv(
            f'game_stats/player/{season}_{month}_{level.lower()}_player_game_stats.csv', index=False)

    return stats_df


if __name__ == "__main__":
    print('starting up')

    now = datetime.now()
    c_dir = 'D:/'
    start_month = 1
    # This is to ensure that any game played in December for this level is downloaded.
    end_month = 13

    parser = argparse.ArgumentParser()
    parser.add_argument('--season', type=int, required=True)
    parser.add_argument('--level', type=str, required=True)
    args = parser.parse_args()

    season = args.season

    # if season == now.year and now.day <= 5 and platform.system() == "Windows":
    #     # This is here to ensure that a game being played
    #     # in between 2 months
    #     # (like a game starting on March 31st but ending on April 1st)
    #     # is not skipped,
    #     # and there's multiple opportunities
    #     # to make sure nothing is skipped.
    #     start_month = now.month - 1
    #     end_month = now.month + 1

    # elif season == now.year:
    #     start_month = now.month
    #     end_month = now.month + 1

    lg_level = args.level

    for i in range(start_month, end_month):
        if platform.system() == "Windows":
            print(
                f'Getting {i}/{season} PBP data in the {lg_level} level of MiLB.')
            get_month_milb_player_game_stats(
                season,
                i,
                level=lg_level,
                cache_data=True,
                cache_dir=c_dir
            )
        else:
            print(
                f'Getting {i}/{season} PBP data in the {lg_level} level of MiLB.')
            get_month_milb_player_game_stats(
                season,
                i,
                level=lg_level
            )
