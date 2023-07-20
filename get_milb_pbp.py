import json
import os
import time
from datetime import datetime
from urllib.request import urlopen

import pandas as pd
from tqdm import tqdm


def get_milb_game_pbp(game_id: int, cache_data=False, cache_dir=""):
    """
    Retrives and parses the play-by-play (PBP) data from a valid
    MiLB game ID.

    Parameters
    ----------
    `game_id`: (int, mandatory):
        The MiLB game ID you want PBP data from.

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
    A pandas `DataFrame` object containing PBP data from 
    the MiLB game ID.
    """
    if cache_data == True and (cache_dir == "" or cache_dir == None):
        home_dir = os.path.expanduser('~')

        try:
            os.mkdir(f"{home_dir}/.milb/")
        except:
            print(f"Cached directory already exists.")

        try:
            os.mkdir(f"{home_dir}/.milb/pbp/")
            os.mkdir(f"{home_dir}/.milb/lineups/")
        except:
            print(
                f"Additional cached directories have been previously created and located.")

    elif cache_data == True and (cache_dir != "" or cache_dir != None):
        try:
            os.mkdir(f"{cache_dir}/.milb/")
        except:
            print(f"Cached directory already exists.")

        try:
            os.mkdir(f"{cache_dir}/.milb/pbp/")
            os.mkdir(f"{cache_dir}/.milb/lineups/")
        except:
            print(
                f"Additional cached directories have been previously created and located.")

    game_df = pd.DataFrame()
    play_df = pd.DataFrame()
    lineups_url = f"https://statsapi.mlb.com/api/v1/schedule?gamePk={game_id}&language=en&hydrate=story,xrefId,lineups,broadcasts(all),probablePitcher(note),game(tickets)&useLatestGames=true&fields=dates,games,teams,probablePitcher,note,id,dates,games,broadcasts,type,name,homeAway,isNational,dates,games,game,tickets,ticketType,ticketLinks,dates,games,lineups,homePlayers,awayPlayers,useName,lastName,primaryPosition,abbreviation,dates,games,xrefIds,xrefId,xrefType,story"
    game_url = f"https://statsapi.mlb.com/api/v1.1/game/{game_id}/feed/live/diffPatch?"

    # Baseball Positions
    ###################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################
    # 1 = Pitcher
    # 2 = Catcher
    # 3 = 1B
    # 4 = 2B
    # 5 = 3B
    # 6 = SS
    # 7 = LF
    # 8 = CF
    # 9 = RF

    # away_fielders = [1, 2, 3, 4, 5, 6, 7, 8, 9]
    # home_fielders = [1, 2, 3, 4, 5, 6, 7, 8, 9]
    away_fielders = [None, 0, 0, 0, 0, 0, 0, 0, 0]
    home_fielders = [None, 0, 0, 0, 0, 0, 0, 0, 0]

    away_score = 0
    home_score = 0

    if cache_data == True and (cache_dir == "" or cache_dir == None):
        # Cached files, default directory
        try:
            with open(f"{home_dir}/.milb/lineups/{game_id}.json", 'r') as f:
                json_string = f.read()

            json_data = json.loads(json_string)
            del json_string
        except:
            response = urlopen(lineups_url)
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
            with open(f"{home_dir}/.milb/lineups/{game_id}.json", 'w+') as f:
                f.write(json.dumps(json_data, indent=2))

    elif cache_data == True and (cache_dir != "" or cache_dir != None):
        # Cached files, custom directory
        try:
            with open(f"{cache_dir}/.milb/lineups/{game_id}.json", 'r') as f:
                json_string = f.read()

            json_data = json.loads(json_string)
            del json_string
        except:
            response = urlopen(lineups_url)
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
            with open(f"{cache_dir}/.milb/lineups/{game_id}.json", 'w+') as f:
                f.write(json.dumps(json_data, indent=2))

    else:
        # No cached files used.
        response = urlopen(lineups_url)
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

    json_data = json_data['dates'][0]['games'][0]['lineups']

    for i in json_data['awayPlayers']:
        player_id = i['id']
        player_pos = i['primaryPosition']['abbreviation']

        match player_pos:
            case "DH":
                # The designated hitter is not a fielder.
                # Thus, we can skip this player.
                continue
            case "C":
                away_fielders[1] = player_id
            case "1B":
                away_fielders[2] = player_id
            case "2B":
                away_fielders[3] = player_id
            case "3B":
                away_fielders[4] = player_id
            case "SS":
                away_fielders[5] = player_id
            case "LF":
                away_fielders[6] = player_id
            case "CF":
                away_fielders[7] = player_id
            case "RF":
                away_fielders[8] = player_id
            case _:
                raise ValueError(
                    f"Unhandled starting player position:\n\t{player_pos}")

    for i in json_data['homePlayers']:
        player_id = i['id']
        player_pos = i['primaryPosition']['abbreviation']

        match player_pos:
            case "DH":
                # The designated hitter is not a fielder.
                # Thus, we can skip this player.
                continue
            case "C":
                home_fielders[1] = player_id
            case "1B":
                home_fielders[2] = player_id
            case "2B":
                home_fielders[3] = player_id
            case "3B":
                home_fielders[4] = player_id
            case "SS":
                home_fielders[5] = player_id
            case "LF":
                home_fielders[6] = player_id
            case "CF":
                home_fielders[7] = player_id
            case "RF":
                home_fielders[8] = player_id
            case _:
                raise ValueError(
                    f"Unhandled starting player position:\n\t{player_pos}")

    if cache_data == True and (cache_dir == "" or cache_dir == None):
        # Cached files, default directory
        try:
            with open(f"{home_dir}/.milb/pbp/{game_id}.json", 'r') as f:
                json_string = f.read()

            json_data = json.loads(json_string)
        except:
            response = urlopen(game_url)
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
            with open(f"{home_dir}/.milb/pbp/{game_id}.json", 'w+') as f:
                f.write(json.dumps(json_data, indent=2))

    elif cache_data == True and (cache_dir != "" or cache_dir != None):
        try:
            with open(f"{cache_dir}/.milb/pbp/{game_id}.json", 'r') as f:
                json_string = f.read()

            json_data = json.loads(json_string)
        except:
            response = urlopen(game_url)
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
            with open(f"{cache_dir}/.milb/pbp/{game_id}.json", 'w+') as f:
                f.write(json.dumps(json_data, indent=2))

    else:
        # No cached files used.
        response = urlopen(game_url)
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

    # json_data = json.loads(response.read())

    game_type = json_data['gameData']['game']['type']
    game_date = json_data['gameData']['datetime']['officialDate']
    game_date = datetime.strptime(game_date, "%Y-%m-%d")
    game_date_str = game_date.strftime("%Y-%m-%d")

    league_id = json_data['gameData']['teams']['away']['league']['id']
    league_name = json_data['gameData']['teams']['away']['league']['name']

    league_level_id = json_data['gameData']['teams']['away']['sport']['id']
    league_level_name = json_data['gameData']['teams']['away']['sport']['name']

    away_team_org_id = json_data['gameData']['teams']['away']['parentOrgId']
    away_team_org_name = json_data['gameData']['teams']['away']['parentOrgName']
    away_team_abv = json_data['gameData']['teams']['away']['abbreviation']

    home_team_org_id = json_data['gameData']['teams']['home']['parentOrgId']
    home_team_org_name = json_data['gameData']['teams']['home']['parentOrgName']
    home_team_abv = json_data['gameData']['teams']['home']['abbreviation']

    for i in tqdm(json_data['liveData']['plays']['allPlays']):

        at_bat_num = i['atBatIndex']
        player_name = i['matchup']['pitcher']['fullName']
        batter_id = i['matchup']['batter']['id']
        batter_side = i['matchup']['batSide']['code']

        pitcher_id = i['matchup']['pitcher']['id']
        pitcher_hand = i['matchup']['pitchHand']['code']
        event = i['result']['event']
        sequence_description = i['result']['description']

        inning = i['about']['inning']
        top_bot = i['about']['halfInning']

        if top_bot.lower() == "top":
            top_bot = "Top"

        elif top_bot.lower() == "bottom":
            top_bot = "Bot"
        else:
            raise IndexError(f'Unhandled baseball inning state:\t{top_bot}')

        post_away_score = i['result']['awayScore']
        post_home_score = i['result']['homeScore']

        if top_bot == "Top":  # Home pitching
            bat_score = away_score
            fld_score = home_score
            post_bat_score = post_away_score
            post_fld_score = post_home_score
        elif top_bot == "Bot":  # Away pitching
            bat_score = home_score
            fld_score = away_score
            post_bat_score = post_home_score
            post_fld_score = post_away_score
        else:
            raise ValueError(f'Unhanlded inning state:\n\t{top_bot}')

        for j in i['playEvents']:
            play_start_datetime = datetime.strptime(
                j['startTime'], "%Y-%m-%dT%H:%M:%S.%fZ")
            play_end_datetime = datetime.strptime(
                j['endTime'], "%Y-%m-%dT%H:%M:%S.%fZ")

            is_pitch = j['isPitch']

            try:
                event_type = j['details']['eventType']
            except:
                event_type = None

            try:
                play_description = j['details']['description']
            except:
                play_description = None

            try:
                play_code = j['details']['code']
            except:
                play_code = None

            if event_type == "pitching_substitution":
                # This should be handeled normaly within this API's structure,
                # but this code is here if for any reason,
                # the MiLB API spits out a play/sequence
                # where a pitcher get's injured during an at bat,
                # and absolutely must be subed out.
                pitcher_id = j['player']['id']

            elif play_code == "AC":
                # Automatic Strike because of a pitch clock violation
                # committed by the batter.
                pass

            elif event_type == "defensive_substitution" and top_bot == "Top":
                subbed_position = int(j['position']['code'])
                new_player_id = j['player']['id']
                home_fielders[(subbed_position-1)] = new_player_id
                del new_player_id, subbed_position

            elif event_type == "defensive_substitution" and top_bot == "Bot":
                subbed_position = int(j['position']['code'])
                new_player_id = j['player']['id']
                away_fielders[(subbed_position-1)] = new_player_id
                del new_player_id, subbed_position

            elif event_type == "defensive_switch" and top_bot == "Top":
                subbed_position = int(j['position']['code'])
                new_player_id = j['player']['id']
                home_fielders[(subbed_position-1)] = new_player_id
                del new_player_id, subbed_position

            elif event_type == "defensive_switch" and top_bot == "Bot":
                subbed_position = int(j['position']['code'])
                new_player_id = j['player']['id']
                away_fielders[(subbed_position-1)] = new_player_id
                del new_player_id, subbed_position

            elif event_type == "offensive_substitution" and top_bot == "Top":
                batter_id = j['player']['id']

            elif event_type == "offensive_substitution":
                batter_id = j['player']['id']

            elif event_type == 'batter_timeout':
                pass

            elif event_type == 'wild_pitch':
                pass

            elif play_description == "Pitcher Step Off":
                pass

            elif play_description == "Pickoff Attempt 1B":
                pass

            elif play_description == "Pickoff Attempt 2B":
                pass

            elif play_description == "Pickoff Attempt 3B":
                pass

            elif event_type == "stolen_base_1b":
                pass

            elif event_type == "stolen_base_2b":
                pass

            elif event_type == "stolen_base_3b":
                pass

            elif event_type == "mound_visit":
                pass

            elif event_type == 'game_advisory':
                # These plays can be skipped for our purposes.
                # This just means that either a delay (of any kind)
                # happened, or the status of the game is being changed
                # (typically from "awaiting for the start of the game"
                # to "game's started now").
                pass

            elif is_pitch == False:
                pass

            else:
                # Fielders

                if top_bot == "Top":  # Home team pitching
                    fielder_2 = home_fielders[1]
                    fielder_3 = home_fielders[2]
                    fielder_4 = home_fielders[3]
                    fielder_5 = home_fielders[4]
                    fielder_6 = home_fielders[5]
                    fielder_7 = home_fielders[6]
                    fielder_8 = home_fielders[7]
                    fielder_9 = home_fielders[8]
                elif top_bot == "Bot":  # Away team pitching
                    fielder_2 = away_fielders[1]
                    fielder_3 = away_fielders[2]
                    fielder_4 = away_fielders[3]
                    fielder_5 = away_fielders[4]
                    fielder_6 = away_fielders[5]
                    fielder_7 = away_fielders[6]
                    fielder_8 = away_fielders[7]
                    fielder_9 = away_fielders[8]
                else:
                    raise ValueError(f'Unhandled inning state:\n\t{top_bot}')
                try:
                    pitch_num = j['pitchNumber']
                except:
                    pitch_num = None

                play_balls = j['count']['balls']
                play_strikes = j['count']['strikes']
                play_outs = j['count']['outs']

                pitch_type = j['details']['code']

                try:
                    pitch_name = j['details']['description']
                except:
                    pitch_name = None

                release_speed = j['pitchData']['startSpeed']
                release_pos_x = j['pitchData']['coordinates']['x0']
                release_pos_y = j['pitchData']['coordinates']['y0']
                release_pos_z = j['pitchData']['coordinates']['z0']

                plate_x = j['pitchData']['coordinates']['pX']
                plate_z = j['pitchData']['coordinates']['pZ']

                pitch_pfx_x = j['pitchData']['coordinates']['pfxX']
                pitch_pfx_z = j['pitchData']['coordinates']['pfxZ']
                pitch_spin_dir = j['pitchData']['breaks']['spinDirection']
                pitch_zone = j['pitchData']['zone']

                pitch_vx0 = j['pitchData']['coordinates']['vX0']
                pitch_vy0 = j['pitchData']['coordinates']['vY0']
                pitch_vz0 = j['pitchData']['coordinates']['vZ0']

                pitch_ax = j['pitchData']['coordinates']['aX']
                pitch_ay = j['pitchData']['coordinates']['aY']
                pitch_az = j['pitchData']['coordinates']['aZ']

                pitch_spin_rate = j['pitchData']['breaks']['spinRate']

                try:
                    pitch_extension = j['pitchData']['extension']
                except:
                    pitch_extension = None

                strikezone_top = j['pitchData']['strikeZoneTop']
                strikezone_bottom = j['pitchData']['strikeZoneBottom']

                play_type = j['details']['code']

                is_in_play = j['details']['isInPlay']

                if is_in_play == True:
                    hit_location = int(j['hitData']['location'])
                    hit_trajectory = j['hitData']['trajectory']
                    hit_distance = int(j['hitData']['totalDistance'])
                    hit_launch_speed = j['hitData']['launchSpeed']
                    hit_launch_angle = j['hitData']['launchAngle']

                    hit_x = j['hitData']['coordinates']['coordX']
                    hit_y = j['hitData']['coordinates']['coordY']

                else:
                    hit_location = None
                    hit_trajectory = None
                    hit_distance = None
                    hit_launch_speed = None
                    hit_launch_angle = None
                    hit_x = None
                    hit_y = None

                play_df = pd.DataFrame(
                    {
                        'play_start_datetime': play_start_datetime,
                        'play_end_datetime': play_end_datetime,
                        'pitch_type': pitch_type,
                        'pitch_name': pitch_name,
                        'game_date': game_date_str,
                        'release_speed': release_speed,
                        'release_pos_x': release_pos_x,
                        'release_pos_y': release_pos_y,
                        'release_pos_z': release_pos_z,
                        'player_name': player_name,
                        'batter': batter_id,
                        'pitcher': pitcher_id,
                        'events': event_type,
                        'description': sequence_description,
                        'spin_dir': pitch_spin_dir,
                        'spin_rate_deprecated': '',
                        'break_angle_deprecated': '',
                        'break_length_deprecated': '',
                        'zone': pitch_zone,
                        'des': sequence_description,
                        'game_type': game_type,
                        'stand': batter_side,
                        'p_throws': pitcher_hand,
                        'home_team': home_team_abv,
                        'away_team': away_team_abv,
                        'type': play_type,
                        'hit_location': hit_location,
                        'bb_type': hit_trajectory,
                        'balls': play_balls,
                        'strikes': play_strikes,
                        'pfx_x': pitch_pfx_x,
                        'pfx_z': pitch_pfx_z,
                        'plate_x': plate_x,
                        'plate_z': plate_z,
                        'on_3b': '',
                        'on_2b': '',
                        'on_1b': '',
                        'outs_when_up': play_outs,
                        'inning': inning,
                        'inning_topbot': top_bot,
                        'hc_x': hit_x,
                        'hc_y': hit_y,
                        'tfs_deprecated': '',
                        'tfs_zulu_deprecated': '',
                        'umpire': '',
                        'sv_id': '',
                        'vx0': pitch_vx0,
                        'vy0': pitch_vy0,
                        'vz0': pitch_vz0,
                        'ax': pitch_ax,
                        'ay': pitch_ay,
                        'az': pitch_az,
                        'sz_top': strikezone_top,
                        'sz_bot': strikezone_bottom,
                        'hit_distance_sc': hit_distance,
                        'launch_speed': hit_launch_speed,
                        'launch_angle': hit_launch_angle,
                        'effective_speed': '',
                        'release_spin_rate': pitch_spin_rate,
                        'release_extension': pitch_extension,
                        'game_pk': game_id,
                        'pitcher_1': pitcher_id,
                        'fielder_2': fielder_2,
                        'fielder_3': fielder_3,
                        'fielder_4': fielder_4,
                        'fielder_5': fielder_5,
                        'fielder_6': fielder_6,
                        'fielder_7': fielder_7,
                        'fielder_8': fielder_8,
                        'fielder_9': fielder_9,
                        'estimated_ba_using_speedangle': '',
                        'estimated_woba_using_speedangle': '',
                        'woba_value': '',
                        'woba_denom': '',
                        'babip_value': '',
                        'iso_value': '',
                        'launch_speed_angle': '',
                        'at_bat_number': at_bat_num,
                        'pitch_number': pitch_num,
                        'home_score': home_score,
                        'away_score': away_score,
                        'bat_score': bat_score,
                        'fld_score': fld_score,
                        'post_away_score': post_away_score,
                        'post_home_score': post_home_score,
                        'post_bat_score': post_bat_score,
                        'post_fld_score': post_fld_score,
                        'if_fielding_alignment': '',
                        'of_fielding_alignment': '',
                        'spin_axis': pitch_spin_dir,
                        'delta_home_win_exp': '',
                        'delta_run_exp': '',
                        'game_month': game_date.month,
                        'game_day': game_date.day,
                        'game_year': game_date.year,
                        'leauge_id': league_id,
                        'leauge_name': league_name,
                        'league_level_id': league_level_id,
                        'league_level_name': league_level_name,
                        'away_team_org_id': away_team_org_id,
                        'away_team_org_name': away_team_org_name,
                        'home_team_org_id': home_team_org_id,
                        'home_team_org_name': home_team_org_name,
                    }, index=[0])

                del pitch_type, pitch_name, release_speed, \
                    release_pos_x, release_pos_y, release_pos_z, pitch_zone, \
                    hit_location, hit_trajectory, hit_launch_angle, is_in_play, \
                    plate_x, plate_z, pitch_pfx_x, pitch_pfx_z, \
                    pitch_spin_dir, play_type, hit_x, hit_y, \
                    pitch_vx0, pitch_vy0, pitch_vz0, \
                    pitch_ax, pitch_az, pitch_ay, \
                    strikezone_top, strikezone_bottom, hit_distance, \
                    pitch_spin_rate, pitch_extension, pitch_num, \
                    play_start_datetime, play_end_datetime, \
                    play_balls, play_strikes, play_outs,\
                    fielder_2, fielder_3, fielder_4,\
                    fielder_5, fielder_6, fielder_7,\
                    fielder_8, fielder_9

                game_df = pd.concat([game_df, play_df], ignore_index=True)

            del is_pitch

        del player_name, batter_id, \
            pitcher_id, sequence_description, \
            inning, top_bot, at_bat_num, \
            batter_side, pitcher_hand, event \

        home_score = post_home_score
        away_score = post_away_score
        del post_home_score, post_away_score

    return game_df


if __name__ == "__main__":
    print('starting up')
    get_milb_game_pbp(725505, cache_data=True, cache_dir='D:/')
