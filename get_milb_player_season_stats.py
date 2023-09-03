import argparse
import json
import time
from datetime import datetime

import numpy as np
import pandas as pd
import requests
from tqdm import tqdm

from get_milb_teams import get_milb_team_list


def get_milb_player_team_season_stats(season: int, level: str, team_id: int, stats_type='batting'):
    """

    """
    now = datetime.now()
    row_df = pd.DataFrame()
    game_df = pd.DataFrame()

    if season < 2005:
        raise ValueError('\n`season` must be greater than 2005')
    elif season > (now.year+1):
        raise ValueError(f'\n`season` cannot be greater than {now.year+1}')

    if level.lower() == 'aaa':
        level_id = 11
        level_abv = "AAA"
    elif level.lower() == 'aa':
        level_id = 12
        level_abv = "AA"
    elif level.lower() == 'a+':
        level_id = 13
        level_abv = "A+"
    elif level.lower() == 'a':
        level_id = 14
        level_abv = "A"
    elif level.lower() == 'a-':
        level_id = 15
        level_abv = "A-"
    elif level.lower() == 'rk':
        level_id = 16
        level_abv = "RK"
    else:
        raise ValueError(
            '\n`level` must be set to \"AAA\", \"AA\", \"A+\", \"A\", \"A-\", or \"RK\".')

    if stats_type.lower() != 'batting' and stats_type.lower() != 'pitching':
        raise ValueError(
            '\n`stats_type` must be set to \"batting\" or \"pitching\".')

    pitching_url = f"https://bdfed.stitch.mlbinfra.com/bdfed/stats/player?stitch_env=prod&season={season}&sportId={level_id}&teamId={team_id}&stats=season&group=pitching&gameType=R&limit=100&offset=0&playerPool=ALL"
    batting_url = f"https://bdfed.stitch.mlbinfra.com/bdfed/stats/player?stitch_env=prod&season={season}&sportId={level_id}&teamId={team_id}&stats=season&group=hitting&gameType=R&limit=100&offset=0&playerPool=ALL"

    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36"}

    if stats_type.lower() == 'batting':
        response = requests.get(batting_url, headers=headers)
        time.sleep(0.1)

        if response.status_code == 200:
            pass
        elif response.status_code == 403:
            raise ConnectionRefusedError(
                '\nThe MiLB API is actively refusing your connection.\nHTTP Error Code:\t403')
        else:
            raise ConnectionError(
                f'\nCould not establish a connection to the MiLB API.\nHTTP Error Code:\t{response.code}')

        json_data = json.loads(response.text)

        if json_data['totalSplits'] == 0:
            # If true, we don't have data.
            print(
                f'\nNo stats found for team ID `{team_id}` in the {season} season.')
            return pd.DataFrame()

        for i in json_data['stats']:
            row_df = pd.DataFrame(
                {'season': season, 'team_id': team_id},
                index=[0]
            )
            row_df['team_abv'] = i['teamAbbrev']
            row_df['team_name'] = i['teamName']
            row_df['team_league_id'] = i['leagueId']
            row_df['team_league'] = i['leagueName']
            row_df['team_level_id'] = level_id
            row_df['team_level_abv'] = level_abv
            row_df['player_id'] = i['playerId']
            row_df['player_full_name'] = i['playerFullName']
            row_df['player_first_name'] = i['playerFirstName']
            row_df['player_last_name'] = i['playerLastName']
            row_df['player_use_name'] = i['playerUseName']
            row_df['player_initial_name'] = i['playerInitLastName']

            if i['positionAbbrev'] == i['primaryPositionAbbrev']:
                row_df['player_position'] = i['primaryPositionAbbrev']
            else:
                row_df['player_position'] = i['primaryPositionAbbrev'] + \
                    "/"+i['positionAbbrev']

            # Batting Stats
            ###################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################

            row_df['G'] = i['gamesPlayed']
            row_df['batting_PA'] = i['plateAppearances']
            row_df['batting_AB'] = i['atBats']
            row_df['batting_H'] = i['hits']
            row_df['batting_2B'] = i['doubles']
            row_df['batting_3B'] = i['triples']
            row_df['batting_HR'] = i['homeRuns']
            row_df['batting_RBI'] = i['rbi']
            row_df['batting_SB'] = i['stolenBases']
            row_df['batting_CS'] = i['caughtStealing']
            row_df['batting_BB'] = i['baseOnBalls']
            row_df['batting_IBB'] = i['intentionalWalks']
            row_df['batting_SO'] = i['strikeOuts']
            row_df['batting_TB'] = i['totalBases']
            row_df['batting_GiDP_Opp'] = i['gidpOpp']
            row_df['batting_GiDP'] = i['groundIntoDoublePlay']
            row_df['batting_SH'] = i['sacBunts']
            row_df['batting_SF'] = i['sacFlies']
            row_df['batting_HBP'] = i['hitByPitch']

            row_df['batting_XBH'] = i['extraBaseHits']
            row_df['batting_GO'] = i['groundOuts']
            row_df['batting_AO'] = i['airOuts']
            row_df['batting_FO'] = i['flyOuts']
            row_df['batting_PO'] = i['popOuts']
            row_df['batting_LO'] = i['lineOuts']
            row_df['batting_CI'] = i['catchersInterference']
            row_df['batting_LOB'] = i['leftOnBase']

            row_df['batting_ground_hits'] = i['groundHits']
            row_df['batting_fly_hits'] = i['flyHits']
            row_df['batting_pop_hits'] = i['popHits']
            row_df['batting_line_hits'] = i['lineHits']

            row_df['batting_pitches_faced'] = i['numberOfPitches']
            row_df['batting_swings'] = i['totalSwings']
            row_df['batting_whiffs'] = i['swingAndMisses']
            row_df['batting_balls_in_play'] = i['ballsInPlay']
            row_df['batting_reached_on_error'] = i['reachedOnError']
            row_df['batting_walkoffs'] = i['walkOffs']

            game_df = pd.concat([game_df, row_df], ignore_index=True)

        game_df.loc[game_df['batting_AB'] > 0,
                    'batting_AVG'] = game_df['batting_H'] / game_df['batting_AB']
        game_df['batting_AVG'] = game_df['batting_AVG'].round(3)

        game_df.loc[game_df['batting_PA'] > 0, 'batting_OBP'] = (game_df['batting_BB'] + game_df['batting_HBP'] + game_df['batting_SF'])/(
            game_df['batting_AB']+game_df['batting_BB'] + game_df['batting_HBP'] + game_df['batting_SF'])
        game_df['batting_OBP'] = game_df['batting_OBP'].round(3)

        game_df.loc[game_df['batting_AB'] > 0,
                    'batting_SLG'] = game_df['batting_TB'] / game_df['batting_AB']
        game_df['batting_SLG'] = game_df['batting_SLG'].round(3)

        game_df['batting_OPS'] = game_df['batting_OBP'] + \
            game_df['batting_SLG']
        game_df['batting_OPS'] = game_df['batting_OPS'].round(3)

        game_df.loc[game_df['batting_AB'] > 0, 'batting_ISO'] = (
            game_df['batting_TB']-game_df['batting_H'])/game_df['batting_AB']
        game_df['batting_ISO'] = game_df['batting_ISO'].round(3)

        game_df.loc[game_df['batting_AB'] > 0, 'batting_BABiP'] = (game_df['batting_H'] - game_df['batting_HR']) / (
            game_df['batting_AB'] - game_df['batting_SO'] - game_df['batting_HR'] + game_df['batting_SF'])
        game_df['batting_BABiP'] = game_df['batting_BABiP'].round(3)

        game_df.loc[game_df['batting_AO'] > 0, 'batting_GO/AO'] = game_df['batting_GO'] / \
            game_df['batting_AO']
        game_df['batting_GO/AO'] = game_df['batting_GO/AO'].round(3)

        game_df.replace([np.inf, -np.inf], None, inplace=True)
        return game_df

    elif stats_type.lower() == 'pitching':
        response = requests.get(pitching_url, headers=headers)
        time.sleep(0.1)

        if response.status_code == 200:
            pass
        elif response.status_code == 403:
            raise ConnectionRefusedError(
                'The MiLB API is actively refusing your connection.\nHTTP Error Code:\t403')
        else:
            raise ConnectionError(
                f'Could not establish a connection to the MiLB API.\nHTTP Error Code:\t{response.code}')

        json_data = json.loads(response.text)

        if json_data['totalSplits'] == 0:
            # If true, we don't have data.
            print(
                f'\nNo stats found for team ID `{team_id}` in the {season} season.')
            return pd.DataFrame()

        for i in json_data['stats']:
            row_df = pd.DataFrame(
                {'season': season, 'team_id': team_id},
                index=[0]
            )
            row_df['team_abv'] = i['teamAbbrev']
            row_df['team_name'] = i['teamName']
            row_df['team_league_id'] = i['leagueId']
            row_df['team_league'] = i['leagueName']
            row_df['team_level_id'] = level_id
            row_df['team_level_abv'] = level_abv
            row_df['player_id'] = i['playerId']
            row_df['player_full_name'] = i['playerFullName']
            row_df['player_first_name'] = i['playerFirstName']
            row_df['player_last_name'] = i['playerLastName']
            row_df['player_use_name'] = i['playerUseName']
            row_df['player_initial_name'] = i['playerInitLastName']

            if i['positionAbbrev'] == i['primaryPositionAbbrev']:
                row_df['player_position'] = i['primaryPositionAbbrev']
            else:
                row_df['player_position'] = i['primaryPositionAbbrev'] + \
                    "/"+i['positionAbbrev']

            row_df['pitching_W'] = int(i['wins'])
            row_df['pitching_L'] = int(i['losses'])
            row_df['pitching_W%'] = None
            row_df['pitching_ERA'] = None
            row_df['pitching_RA9'] = None
            # game_df['pitching_FIP'] = None
            row_df['pitching_G'] = i['gamesPitched']
            row_df['pitching_GS'] = i['gamesStarted']
            row_df['pitching_GF'] = i['gamesFinished']
            row_df['pitching_CG'] = i['completeGames']
            row_df['pitching_QS'] = i['qualityStarts']
            row_df['pitching_SHO'] = i['shutouts']

            row_df['pitching_SVO'] = i['saveOpportunities']
            row_df['pitching_SV'] = i['saves']
            row_df['pitching_HLD'] = i['holds']
            row_df['pitching_BS'] = i['blownSaves']

            row_df['pitching_IP_str'] = i['inningsPitched']
            row_df['pitching_IP'] = i['outs']/3
            row_df['pitching_IP'] = row_df['pitching_IP'].round(3)

            row_df['pitching_BF'] = i['battersFaced']
            row_df['pitching_AB'] = i['atBats']
            row_df['pitching_R'] = i['runs']
            row_df['pitching_H'] = i['hits']
            row_df['pitching_2B'] = i['doubles']
            row_df['pitching_3B'] = i['triples']
            row_df['batting_R'] = i['runs']
            row_df['pitching_ER'] = i['earnedRuns']
            row_df['pitching_HR'] = i['homeRuns']
            row_df['pitching_TB'] = i['totalBases']
            row_df['pitching_BB'] = i['baseOnBalls']
            row_df['pitching_IBB'] = i['intentionalWalks']
            row_df['pitching_SO'] = i['strikeOuts']
            row_df['pitching_HBP'] = i['hitByPitch']
            row_df['pitching_BK'] = i['balks']

            row_df['pitching_GiDP'] = i['groundIntoDoublePlay']
            try:
                row_df['pitching_GiDP_opp'] = i['gidpOpp']
            except:
                row_df['pitching_GiDP_opp'] = None

            row_df['pitching_CI'] = i['catchersInterference']

            row_df['pitching_IR'] = i['inheritedRunners']
            row_df['pitching_IRS'] = i['inheritedRunnersScored']

            row_df['pitching_BqR'] = i['bequeathedRunners']
            row_df['pitching_BqRS'] = i['bequeathedRunnersScored']

            row_df['pitching_RS'] = i['runSupport']

            row_df['pitching_SF'] = i['sacFlies']
            row_df['pitching_SB'] = i['stolenBases']

            try:
                row_df['pitching_CS'] = i['caughtStealing']
            except:
                row_df['pitching_CS'] = None

            row_df['pitching_PK'] = i['pickoffs']

            row_df['pitching_FH'] = i['flyHits']
            row_df['pitching_PH'] = i['popHits']
            row_df['pitching_LH'] = i['lineHits']

            row_df['pitching_FO'] = i['flyOuts']
            row_df['pitching_GO'] = i['groundOuts']
            row_df['pitching_AO'] = i['airOuts']
            row_df['pitching_pop_outs'] = i['popOuts']
            row_df['pitching_line_outs'] = i['lineOuts']

            row_df['pitching_PI'] = i['numberOfPitches']
            row_df['pitching_total_swings'] = i['totalSwings']
            row_df['pitching_swing_and_misses'] = i['swingAndMisses']
            row_df['pitching_balls_in_play'] = i['ballsInPlay']
            row_df['pitching_PI_strikes'] = i['strikes']
            row_df['pitching_PI_balls'] = row_df['pitching_PI'] - \
                row_df['pitching_PI_strikes']
            row_df['pitching_WP'] = i['wildPitches']

            game_df = pd.concat([game_df, row_df], ignore_index=True)

        game_df.loc[(game_df['pitching_W'] + game_df['pitching_L']) > 0,
                    'pitching_W%'] = round(game_df['pitching_W']/(
                        game_df['pitching_W'] + game_df['pitching_L']), 3)

        game_df.loc[game_df['pitching_IP'] > 0,
                    'pitching_ERA'] = round(9 * (game_df['pitching_ER'] / game_df['pitching_IP']), 3)

        # Needs other parts of the repo to be built up before implimentation.
        # game_df['pitching_FIP'] = None

        game_df.loc[game_df['pitching_IP'] > 0, 'pitching_RA9'] = round(9 *
                                                                        (game_df['pitching_R'] / game_df['pitching_IP']), 3)

        game_df.loc[game_df['pitching_IP'] > 0, 'pitching_WHIP'] = round((
            game_df['pitching_BB'] + game_df['pitching_H']) / game_df['pitching_IP'], 3)

        game_df.loc[game_df['pitching_IP'] > 0, 'pitching_H/9'] = round(9 *
                                                                        (game_df['pitching_H'] / game_df['pitching_IP']), 3)

        game_df.loc[game_df['pitching_IP'] > 0, 'pitching_HR/9'] = round(9 *
                                                                         (game_df['pitching_HR'] / game_df['pitching_IP']), 3)

        game_df.loc[game_df['pitching_IP'] > 0, 'pitching_BB/9'] = round(9 *
                                                                         (game_df['pitching_BB'] / game_df['pitching_IP']), 3)

        game_df.loc[game_df['pitching_IP'] > 0, 'pitching_SO/9'] = round(9 *
                                                                         (game_df['pitching_SO'] / game_df['pitching_IP']), 3)

        game_df.loc[game_df['pitching_AB'] > 0, 'pitching_BABiP'] = round((game_df['pitching_H'] - game_df['pitching_HR']) / (
            game_df['pitching_AB'] - game_df['pitching_SO'] - game_df['pitching_HR'] - game_df['pitching_SF']), 3)

        game_df.loc[game_df['pitching_BB'] > 0,
                    'pitching_SO/BB'] = round(game_df['pitching_SO'] / game_df['pitching_BB'], 3)

        game_df.loc[game_df['pitching_AB'] > 0,
                    'pitching_BA'] = round(game_df['pitching_H'] / game_df['pitching_AB'], 3)

        game_df.loc[game_df['pitching_BF'] > 0, 'pitching_OBP'] = round((game_df['pitching_H']+game_df['pitching_BB']+game_df['pitching_HBP'])/(
            game_df['pitching_AB']+game_df['pitching_BB']+game_df['pitching_HBP']+game_df['pitching_SF']), 3)

        game_df.loc[game_df['pitching_AB'] > 0, 'pitching_SLG'] = round((game_df['pitching_H'] + (game_df['pitching_2B'] * 2) + (
            game_df['pitching_3B'] * 3) + (game_df['pitching_HR'] * 4)) / game_df['pitching_AB'], 3)

        game_df.loc['pitching_OPS'] = round(game_df['pitching_OBP'] +
                                            game_df['pitching_SLG'], 3)

        game_df.loc['pitching_ISO'] = round(game_df['pitching_SLG'] -
                                            game_df['pitching_BA'], 3)

        game_df.loc[game_df['pitching_BF'] > 0,
                    'pitching_SO%'] = round(game_df['pitching_SO']/game_df['pitching_BF'], 3)

        game_df.loc[game_df['pitching_SO'] > 0,
                    'pitching_BB/SO'] = round(game_df['pitching_BB']/game_df['pitching_SO'], 3)

        game_df.loc[game_df['pitching_BF'] > 0,
                    'pitching_PI/PA'] = round(game_df['pitching_PI']/game_df['pitching_BF'], 3)

        game_df.loc[game_df['pitching_BF'] > 0, 'pitching_HR/PA'] = round(
            game_df['pitching_HR']/game_df['pitching_BF'], 3)

        # game_df.loc[game_df['pitching_BF'] > 0, 'pitching_SO/PA'] = round(
        #     game_df['pitching_SO']/game_df['pitching_BF'], 3)

        game_df.loc[game_df['pitching_BF'] > 0, 'pitching_BB/PA'] = round(
            game_df['pitching_BB']/game_df['pitching_BF'], 3)

        game_df.loc[game_df['pitching_IP'] > 0, 'pitching_PI/IP'] = round(
            game_df['pitching_PI']/game_df['pitching_IP'], 3)

        game_df.replace([np.inf, -np.inf], None, inplace=True)
        game_df.dropna(subset=['season'], inplace=True)
        return game_df


def get_milb_player_season_stats(season: int, level: str, stats_type='batting', save=False):
    """

    """
    now = datetime.now()
    game_df = pd.DataFrame()
    season_df = pd.DataFrame()

    if season < 2005:
        raise ValueError('`season` must be greater than 2005')
    elif season > now.year:
        raise ValueError(f'`season` cannot be greater than {now.year}')

    if level.lower() == 'aaa':
        level_id = 11
    elif level.lower() == 'aa':
        level_id = 12
    elif level.lower() == 'a+':
        level_id = 13
    elif level.lower() == 'a':
        level_id = 14
    elif level.lower() == 'a-':
        level_id = 15
    elif level.lower() == 'rk':
        level_id = 16
    else:
        raise ValueError(
            '`level` must be set to \"AAA\", \"AA\", \"A+\", \"A\", \"A-\", or \"RK\".')

    if stats_type.lower() != 'batting' and stats_type.lower() != 'pitching':
        raise ValueError(
            '`stats_type` must be set to \"batting\" or \"pitching\".')

    print(f'\nGetting a list of teams in the MLB API for the {season} season.')
    teams_df = get_milb_team_list(season=season, save=False)

    print(f'\nFiltering out teams that aren\'t in {level.upper()} baseball.')
    teams_df = teams_df.loc[teams_df['sport_id'] == level_id]

    team_id_arr = teams_df['team_id'].to_numpy()

    print(f'\nGetting {stats_type} season stats in {level.upper()} baseball.')

    for team_id in tqdm(team_id_arr):
        try:
            game_df = get_milb_player_team_season_stats(
                season=season,
                level=level,
                team_id=team_id,
                stats_type=stats_type
            )
        except:
            print(f'\nCould not get player season stats for team ID {team_id}')
            game_df = pd.DataFrame()

        # game_df = get_milb_player_team_season_stats(
        #     season=season,
        #     level=level,
        #     team_id=team_id,
        #     stats_type=stats_type
        # )
        season_df = pd.concat([season_df, game_df], ignore_index=True)

    if save == True and stats_type == 'batting':
        season_df.to_csv(
            f'season_stats/player/{season}_{level.lower()}_season_batting_stats.csv',
            index=False
        )
    elif save == True and stats_type == 'pitching':
        season_df.to_csv(
            f'season_stats/player/{season}_{level.lower()}_season_pitching_stats.csv',
            index=False
        )

    return season_df


if __name__ == "__main__":

    print('Starting up.')
    now = datetime.now()

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--season',
        type=int,
        required=True,
        help='The season you want player season stats from.'
    )

    parser.add_argument(
        '--end_season',
        type=int,
        required=False,
        help='Optional argument. If you want to download data from a range of seasons, set `--end_season` to the final season you want stats from.'
    )

    parser.add_argument(
        '--level',
        type=str,
        required=True,
        help='The MiLB level you want player season stats from.'
    )

    parser.add_argument(
        '--stat_type',
        type=str,
        required=True,
        help='The type of stats you want to download. Valid arguments are `batting` or `pitching`.'
    )

    args = parser.parse_args()

    season = args.season
    end_season = args.end_season
    lg_level = args.level
    stats_type = args.stat_type

    if stats_type.lower() != 'batting' and stats_type.lower() != 'pitching':
        raise ValueError(
            f'`--stats_type` must be set to \"batting\" or \"pitching\".')

    if end_season != None:
        if season > end_season:
            raise ValueError(
                '`--season` cannot be greater than `--end_season`.')
        elif season == end_season:
            raise ValueError('`--end_season` cannot be equal to `--season`.')

        print(
            f'Parsing season player {stats_type} stats at the ' +
            f'{lg_level.upper()} baseball level between the ' +
            f'{season} and {end_season} seasons.')

        for s in range(season, end_season+1):
            get_milb_player_season_stats(
                season=s,
                level=lg_level,
                stats_type=stats_type,
                save=True
            )

    else:
        print(
            f'Parsing season player {stats_type} stats ' +
            f'for the {season} {lg_level.upper()} baseball season.'
        )

        get_milb_player_season_stats(
            season=season,
            level=lg_level,
            stats_type=stats_type,
            save=True
        )
