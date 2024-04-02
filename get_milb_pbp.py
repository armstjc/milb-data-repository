import argparse
import json
import os
import platform
import time
from datetime import datetime
from urllib.request import urlopen
import warnings
import numpy as np

import pandas as pd
from tqdm import tqdm

from get_milb_schedule import load_milb_schedule

warnings.filterwarnings("ignore", category=FutureWarning)


def get_milb_game_pbp(game_id: int, cache_data=False, cache_dir=""):
    """
    Retrieves and parses the play-by-play (PBP) data from a valid
    MiLB game ID.

    Parameters
    ----------
    `game_id`: (int, mandatory):
        The MiLB game ID you want PBP data from.

    `cache_data`: (bool, optional) = `False`:
        Optional boolean flag.
        If set to `True`, data downloaded by this function will be cached to
        a folder named `./milb/`.
        This folder will either be located in the user's home directory,
        or in a existing directory
        specified by the optional argument `cache_dir`.

    `cache_dir`: (str, optional) = `""`:
        Optional string. If not set to `""` or `None`,
        this will be the directory used to cache data
        if `cache_data` is set to `True`.
        This directory must exist prior to running this function!

    Returns
    ----------
    A pandas `DataFrame` object containing PBP data from
    the MiLB game ID.
    """
    has_lineups = False
    if cache_data is True \
            and (cache_dir == "" or cache_dir is None):
        home_dir = os.path.expanduser("~")

        try:
            os.mkdir(f"{home_dir}/.milb/")
        except Exception:
            pass

        try:
            os.mkdir(f"{home_dir}/.milb/pbp/")
            os.mkdir(f"{home_dir}/.milb/lineups/")
        except Exception:
            pass

    elif cache_data is True \
            and (cache_dir != "" or cache_dir is not None):
        try:
            os.mkdir(f"{cache_dir}/.milb/")
        except Exception:
            pass

        try:
            os.mkdir(f"{cache_dir}/.milb/pbp/")
            os.mkdir(f"{cache_dir}/.milb/lineups/")
        except Exception:
            pass

    game_df = pd.DataFrame()
    play_df = pd.DataFrame()
    lineups_url = "https://statsapi.mlb.com/api/v1/schedule" +\
        f"?gamePk={game_id}&language=en&hydrate=story,xrefId," +\
        "lineups,broadcasts(all),probablePitcher(note),game(tickets)" + \
        "&useLatestGames=true&fields=dates,games,teams,probablePitcher," + \
        "note,id,dates,games,broadcasts,type,name,homeAway,isNational," +\
        "dates,games,game,tickets,ticketType,ticketLinks,dates,games," +\
        "lineups,homePlayers,awayPlayers,useName,lastName," +\
        "primaryPosition,abbreviation,dates,games," +\
        "xrefIds,xrefId,xrefType,story"
    game_url = f"https://statsapi.mlb.com/api/v1.1/game/{game_id}/feed/live?"

    # Baseball Positions
    ##########################################################################
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
    away_fielders = [None, None, None, None, None, None, None, None, None]
    home_fielders = [None, None, None, None, None, None, None, None, None]

    away_score = 0
    home_score = 0

    if cache_data is True and (cache_dir == "" or cache_dir is None):
        # Cached files, default directory
        try:
            with open(f"{home_dir}/.milb/lineups/{game_id}.json", "r") as f:
                json_string = f.read()

            json_data = json.loads(json_string)
            has_lineups = True

            del json_string
        except Exception:
            response = urlopen(lineups_url)
            time.sleep(1)

            if response.code == 200:
                pass
            elif response.code == 403:
                raise ConnectionRefusedError(
                    "The MiLB API is actively refusing your connection." +
                    "\nHTTP Error Code:\t403"
                )
            else:
                raise ConnectionError(
                    "Could not establish a connection to the MiLB API." +
                    f"\nHTTP Error Code:\t{response.code}"
                )

            json_data = json.loads(response.read())
            try:
                with open(
                    f"{home_dir}/.milb/lineups/{game_id}.json", "w+"
                ) as f:
                    f.write(json.dumps(json_data, indent=2))
                has_lineups = True
            except Exception:
                has_lineups = False

    elif cache_data is True and (cache_dir != "" or cache_dir is not None):
        # Cached files, custom directory
        try:
            with open(f"{cache_dir}/.milb/lineups/{game_id}.json", "r") as f:
                json_string = f.read()

            json_data = json.loads(json_string)
            del json_string
            has_lineups = True

        except Exception:
            response = urlopen(lineups_url)
            time.sleep(1)

            if response.code == 200:
                pass
            elif response.code == 403:
                raise ConnectionRefusedError(
                    "The MiLB API is actively refusing your connection." +
                    "\nHTTP Error Code:\t403"
                )
            else:
                raise ConnectionError(
                    "Could not establish a connection to the MiLB API." +
                    f"\nHTTP Error Code:\t{response.code}"
                )

            try:
                json_data = json.loads(response.read())
                with open(
                    f"{cache_dir}/.milb/lineups/{game_id}.json", "w+"
                ) as f:
                    f.write(json.dumps(json_data, indent=2))

                has_lineups = True
            except Exception:
                has_lineups = False

    else:
        # No cached files used.
        response = urlopen(lineups_url)
        time.sleep(1)

        try:
            if response.code == 200:
                pass
            elif response.code == 403:
                raise ConnectionRefusedError(
                    "The MiLB API is actively refusing your connection." +
                    "\nHTTP Error Code:\t403"
                )
            else:
                raise ConnectionError(
                    "Could not establish a connection to the MiLB API." +
                    f"\nHTTP Error Code:\t{response.code}"
                )

            json_data = json.loads(response.read())
            has_lineups = True
        except Exception:
            has_lineups = False

    if has_lineups is True:
        try:
            json_data = json_data["dates"][0]["games"][0]["lineups"]

            for i in json_data["awayPlayers"]:
                player_id = i["id"]
                player_pos = i["primaryPosition"]["abbreviation"]

                match player_pos:
                    case "DH":
                        # The designated hitter is not a fielder.
                        # Thus, we can skip this player.
                        continue
                    case "P":
                        away_fielders[0] = player_id
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
                            "Unhandled starting player position:" +
                            f"\n\t{player_pos}"
                        )

            for i in json_data["homePlayers"]:
                player_id = i["id"]
                player_pos = i["primaryPosition"]["abbreviation"]

                match player_pos:
                    case "DH":
                        # The designated hitter is not a fielder.
                        # Thus, we can skip this player.
                        continue
                    case "P":
                        away_fielders[0] = player_id
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
                            "Unhandled starting player position:" +
                            f"\n\t{player_pos}"
                        )
        except Exception:
            print(f"Lineups data not found for {game_id}")

    else:
        print(f"Lineups data not found for {game_id}")

    if cache_data is True and (cache_dir == "" or cache_dir is None):
        # Cached files, default directory
        try:
            with open(f"{home_dir}/.milb/pbp/{game_id}.json", "r") as f:
                json_string = f.read()

            json_data = json.loads(json_string)
        except Exception:
            response = urlopen(game_url)
            time.sleep(1)

            if response.code == 200:
                pass
            elif response.code == 403:
                raise ConnectionRefusedError(
                    "The MiLB API is actively refusing your connection." +
                    "\nHTTP Error Code:\t403"
                )
            else:
                raise ConnectionError(
                    "Could not establish a connection to the MiLB API." +
                    f"\nHTTP Error Code:\t{response.code}"
                )

            json_data = json.loads(response.read())
            with open(f"{home_dir}/.milb/pbp/{game_id}.json", "w+") as f:
                f.write(json.dumps(json_data, indent=2))

    elif cache_data is True and (cache_dir != "" or cache_dir is not None):
        try:
            with open(f"{cache_dir}/.milb/pbp/{game_id}.json", "r") as f:
                json_string = f.read()

            json_data = json.loads(json_string)
        except Exception:
            response = urlopen(game_url)
            time.sleep(1)

            if response.code == 200:
                pass
            elif response.code == 403:
                raise ConnectionRefusedError(
                    "The MiLB API is actively refusing your connection." +
                    "\nHTTP Error Code:\t403"
                )
            else:
                raise ConnectionError(
                    "Could not establish a connection to the MiLB API." +
                    f"\nHTTP Error Code:\t{response.code}"
                )

            json_data = json.loads(response.read())
            with open(f"{cache_dir}/.milb/pbp/{game_id}.json", "w+") as f:
                f.write(json.dumps(json_data, indent=2))

    else:
        # No cached files used.
        response = urlopen(game_url)
        time.sleep(1)

        if response.code == 200:
            pass
        elif response.code == 403:
            raise ConnectionRefusedError(
                "The MiLB API is actively refusing your connection." +
                "\nHTTP Error Code:\t403"
            )
        else:
            raise ConnectionError(
                "Could not establish a connection to the MiLB API." +
                f"\nHTTP Error Code:\t{response.code}"
            )

        json_data = json.loads(response.read())

    # json_data = json.loads(response.read())
    if len(json_data) == 0:
        print(f"\nCould not get PBP data for game ID {game_id}")
        return pd.DataFrame()

    game_type = json_data["gameData"]["game"]["type"]
    game_date = json_data["gameData"]["datetime"]["officialDate"]
    game_date = datetime.strptime(game_date, "%Y-%m-%d")
    game_date_str = game_date.strftime("%Y-%m-%d")

    league_id = json_data["gameData"]["teams"]["away"]["league"]["id"]
    league_name = json_data["gameData"]["teams"]["away"]["league"]["name"]

    league_level_id = json_data["gameData"]["teams"]["away"]["sport"]["id"]
    league_level_name = json_data["gameData"]["teams"]["away"]["sport"]["name"]

    try:
        away_team_org_id = json_data[
            "gameData"]["teams"]["away"]["parentOrgId"]
    except Exception:
        away_team_org_id = None

    try:
        away_team_org_name = json_data[
            "gameData"]["teams"]["away"]["parentOrgName"]
    except Exception:
        away_team_org_name = None

    try:
        away_team_abv = json_data["gameData"]["teams"]["away"]["abbreviation"]
    except Exception:
        away_team_abv = None

    try:
        home_team_org_id = json_data[
            "gameData"]["teams"]["home"]["parentOrgId"]
    except Exception:
        home_team_org_id = None

    try:
        home_team_org_name = json_data[
            "gameData"]["teams"]["home"]["parentOrgName"]
    except Exception:
        home_team_org_name = None

    try:
        home_team_abv = json_data["gameData"]["teams"]["home"]["abbreviation"]
    except Exception:
        home_team_abv = None

    for i in tqdm(json_data["liveData"]["plays"]["allPlays"]):

        at_bat_num = i["atBatIndex"]
        player_name = i["matchup"]["pitcher"]["fullName"]
        batter_id = i["matchup"]["batter"]["id"]
        batter_side = i["matchup"]["batSide"]["code"]

        pitcher_id = i["matchup"]["pitcher"]["id"]
        pitcher_hand = i["matchup"]["pitchHand"]["code"]

        try:
            event = i["result"]["event"]
        except Exception:
            event = None

        try:
            sequence_description = i["result"]["description"]
        except Exception:
            sequence_description = None

        inning = i["about"]["inning"]
        top_bot = i["about"]["halfInning"]

        if top_bot.lower() == "top":
            top_bot = "Top"

        elif top_bot.lower() == "bottom":
            top_bot = "Bot"
        else:
            raise IndexError(f"Unhandled baseball inning state:\t{top_bot}")

        post_away_score = i["result"]["awayScore"]
        post_home_score = i["result"]["homeScore"]

        try:
            on_1b = i["matchup"]["postOnFirst"]["id"]
        except Exception:
            on_1b = None

        try:
            on_2b = i["matchup"]["postOnSecond"]["id"]
        except Exception:
            on_2b = None

        try:
            on_3b = i["matchup"]["postOnThird"]["id"]
        except Exception:
            on_3b = None

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
            raise ValueError(f"Unhandled inning state:\n\t{top_bot}")

        for j in i["playEvents"]:
            try:
                play_start_datetime = datetime.strptime(
                    j["startTime"], "%Y-%m-%dT%H:%M:%S.%fZ"
                )
            except Exception:
                play_start_datetime = None

            try:
                play_end_datetime = datetime.strptime(
                    j["endTime"], "%Y-%m-%dT%H:%M:%S.%fZ"
                )
            except Exception:
                play_end_datetime = None

            is_pitch = j["isPitch"]

            try:
                event_type = j["details"]["eventType"]
            except Exception:
                event_type = None

            try:
                play_description = j["details"]["description"]
            except Exception:
                play_description = None

            try:
                play_code = j["details"]["code"]
            except Exception:
                play_code = None

            if event_type == "pitching_substitution":
                # This should be handled normally within this API's structure,
                # but this code is here if for any reason,
                # the MiLB API spits out a play/sequence
                # where a pitcher get's injured during an at bat,
                # and absolutely must be subbed out.
                pitcher_id = j["player"]["id"]

            elif play_code == "AC":
                # Automatic Strike because of a pitch clock violation
                # committed by the batter.
                pass

            elif event_type == "defensive_substitution" and top_bot == "Top":
                subbed_position = int(j["position"]["code"])
                if subbed_position == 10:
                    pass
                else:
                    new_player_id = j["player"]["id"]
                    home_fielders[(subbed_position - 1)] = new_player_id
                    del new_player_id, subbed_position

            elif event_type == "defensive_substitution" and top_bot == "Bot":
                subbed_position = int(j["position"]["code"])
                if subbed_position == 10:
                    pass
                else:
                    new_player_id = j["player"]["id"]
                    away_fielders[(subbed_position - 1)] = new_player_id
                    del new_player_id, subbed_position

            elif event_type == "defensive_switch" and top_bot == "Top":
                subbed_position = int(j["position"]["code"])
                if subbed_position == 10:
                    pass
                else:
                    new_player_id = j["player"]["id"]
                    home_fielders[(subbed_position - 1)] = new_player_id
                    del new_player_id, subbed_position

            elif event_type == "defensive_switch" and top_bot == "Bot":
                subbed_position = int(j["position"]["code"])
                if subbed_position == 10:
                    pass
                else:
                    new_player_id = j["player"]["id"]
                    away_fielders[(subbed_position - 1)] = new_player_id
                    del new_player_id, subbed_position

            elif event_type == "offensive_substitution" and top_bot == "Top":
                batter_id = j["player"]["id"]

            elif event_type == "offensive_substitution":
                batter_id = j["player"]["id"]

            elif event_type == "batter_timeout":
                pass

            elif event_type == "wild_pitch":
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

            elif event_type == "game_advisory":
                # These plays can be skipped for our purposes.
                # This just means that either a delay (of any kind)
                # happened, or the status of the game is being changed
                # (typically from "awaiting for the start of the game"
                # to "game's started now").
                pass

            elif is_pitch is False:
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
                    raise ValueError(f"Unhandled inning state:\n\t{top_bot}")

                try:
                    pitch_num = j["pitchNumber"]
                except Exception:
                    pitch_num = None

                play_balls = j["count"]["balls"]
                play_strikes = j["count"]["strikes"]
                play_outs = j["count"]["outs"]

                try:
                    pitch_type = j["details"]["type"]["code"]
                except Exception:
                    pitch_type = None

                try:
                    pitch_name = j["details"]["type"]["description"]
                except Exception:
                    pitch_name = None

                try:
                    release_speed = j["pitchData"]["startSpeed"]
                except Exception:
                    release_speed = None

                try:
                    release_pos_x = j["pitchData"]["coordinates"]["x0"]
                except Exception:
                    release_pos_x = None

                try:
                    release_pos_y = j["pitchData"]["coordinates"]["y0"]
                except Exception:
                    release_pos_y = None

                try:
                    release_pos_z = j["pitchData"]["coordinates"]["z0"]
                except Exception:
                    release_pos_z = None

                try:
                    plate_x = j["pitchData"]["coordinates"]["pX"]
                except Exception:
                    plate_x = None

                try:
                    plate_z = j["pitchData"]["coordinates"]["pZ"]
                except Exception:
                    plate_z = None

                try:
                    pitch_pfx_x = j["pitchData"]["coordinates"]["pfxX"]
                except Exception:
                    pitch_pfx_x = None

                try:
                    pitch_pfx_z = j["pitchData"]["coordinates"]["pfxZ"]
                except Exception:
                    pitch_pfx_z = None

                try:
                    pitch_spin_dir = j["pitchData"]["breaks"]["spinDirection"]
                except Exception:
                    pitch_spin_dir = None

                try:
                    pitch_zone = j["pitchData"]["zone"]
                except Exception:
                    pitch_zone = None

                try:
                    pitch_vx0 = j["pitchData"]["coordinates"]["vX0"]
                except Exception:
                    pitch_vx0 = None

                try:
                    pitch_vy0 = j["pitchData"]["coordinates"]["vY0"]
                except Exception:
                    pitch_vy0 = None

                try:
                    pitch_vz0 = j["pitchData"]["coordinates"]["vZ0"]
                except Exception:
                    pitch_vz0 = None

                try:
                    pitch_ax = j["pitchData"]["coordinates"]["aX"]
                except Exception:
                    pitch_ax = None

                try:
                    pitch_ay = j["pitchData"]["coordinates"]["aY"]
                except Exception:
                    pitch_ay = None

                try:
                    pitch_az = j["pitchData"]["coordinates"]["aZ"]
                except Exception:
                    pitch_az = None

                try:
                    pitch_spin_rate = j["pitchData"]["breaks"]["spinRate"]
                except Exception:
                    pitch_spin_rate = None

                try:
                    pitch_extension = j["pitchData"]["extension"]
                except Exception:
                    pitch_extension = None

                strike_zone_top = j["pitchData"]["strikeZoneTop"]
                strike_zone_bottom = j["pitchData"]["strikeZoneBottom"]

                play_type = j["details"]["code"]

                is_in_play = j["details"]["isInPlay"]

                if is_in_play is True:
                    try:
                        hit_location = int(j["hitData"]["location"])
                    except Exception:
                        hit_location = None

                    try:
                        hit_trajectory = j["hitData"]["trajectory"]
                    except Exception:
                        hit_trajectory = None

                    try:
                        hit_distance = int(j["hitData"]["totalDistance"])
                    except Exception:
                        hit_distance = None

                    try:
                        hit_launch_speed = j["hitData"]["launchSpeed"]
                    except Exception:
                        hit_launch_speed = None

                    try:
                        hit_launch_angle = j["hitData"]["launchAngle"]
                    except Exception:
                        hit_launch_angle = None

                    try:
                        hit_x = j["hitData"]["coordinates"]["coordX"]
                    except Exception:
                        hit_x = None

                    try:
                        hit_y = j["hitData"]["coordinates"]["coordY"]
                    except Exception:
                        hit_y = None

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
                        "play_start_datetime": play_start_datetime,
                        "play_end_datetime": play_end_datetime,
                        "pitch_type": pitch_type,
                        "pitch_name": pitch_name,
                        "game_date": game_date_str,
                        "release_speed": release_speed,
                        "release_pos_x": release_pos_x,
                        "release_pos_y": release_pos_y,
                        "release_pos_z": release_pos_z,
                        "player_name": player_name,
                        "batter": batter_id,
                        "pitcher": pitcher_id,
                        "events": event_type,
                        "description": sequence_description,
                        "spin_dir": pitch_spin_dir,
                        "spin_rate_deprecated": "",
                        "break_angle_deprecated": "",
                        "break_length_deprecated": "",
                        "zone": pitch_zone,
                        "des": sequence_description,
                        "game_type": game_type,
                        "stand": batter_side,
                        "p_throws": pitcher_hand,
                        "home_team": home_team_abv,
                        "away_team": away_team_abv,
                        "type": play_type,
                        "hit_location": hit_location,
                        "bb_type": hit_trajectory,
                        "balls": play_balls,
                        "strikes": play_strikes,
                        "pfx_x": pitch_pfx_x,
                        "pfx_z": pitch_pfx_z,
                        "plate_x": plate_x,
                        "plate_z": plate_z,
                        "on_3b": on_3b,
                        "on_2b": on_2b,
                        "on_1b": on_1b,
                        "outs_when_up": play_outs,
                        "inning": inning,
                        "inning_top_bot": top_bot,
                        "hc_x": hit_x,
                        "hc_y": hit_y,
                        "tfs_deprecated": "",
                        "tfs_zulu_deprecated": "",
                        "umpire": "",
                        "sv_id": "",
                        "vx0": pitch_vx0,
                        "vy0": pitch_vy0,
                        "vz0": pitch_vz0,
                        "ax": pitch_ax,
                        "ay": pitch_ay,
                        "az": pitch_az,
                        "sz_top": strike_zone_top,
                        "sz_bot": strike_zone_bottom,
                        "hit_distance_sc": hit_distance,
                        "launch_speed": hit_launch_speed,
                        "launch_angle": hit_launch_angle,
                        "effective_speed": "",
                        "release_spin_rate": pitch_spin_rate,
                        "release_extension": pitch_extension,
                        "game_pk": game_id,
                        "pitcher_1": pitcher_id,
                        "fielder_2": fielder_2,
                        "fielder_3": fielder_3,
                        "fielder_4": fielder_4,
                        "fielder_5": fielder_5,
                        "fielder_6": fielder_6,
                        "fielder_7": fielder_7,
                        "fielder_8": fielder_8,
                        "fielder_9": fielder_9,
                        "estimated_ba_using_speedangle": "",
                        "estimated_woba_using_speedangle": "",
                        "woba_value": "",
                        "woba_denom": "",
                        "babip_value": "",
                        "iso_value": "",
                        "launch_speed_angle": "",
                        "at_bat_number": at_bat_num,
                        "pitch_number": pitch_num,
                        "home_score": home_score,
                        "away_score": away_score,
                        "bat_score": bat_score,
                        "fld_score": fld_score,
                        "post_away_score": post_away_score,
                        "post_home_score": post_home_score,
                        "post_bat_score": post_bat_score,
                        "post_fld_score": post_fld_score,
                        "if_fielding_alignment": "",
                        "of_fielding_alignment": "",
                        "spin_axis": pitch_spin_dir,
                        "delta_home_win_exp": "",
                        "delta_run_exp": "",
                        "game_month": game_date.month,
                        "game_day": game_date.day,
                        "game_year": game_date.year,
                        "league_id": league_id,
                        "league_name": league_name,
                        "league_level_id": league_level_id,
                        "league_level_name": league_level_name,
                        "away_team_org_id": away_team_org_id,
                        "away_team_org_name": away_team_org_name,
                        "home_team_org_id": home_team_org_id,
                        "home_team_org_name": home_team_org_name,
                    },
                    index=[0],
                )

                del (
                    pitch_type,
                    pitch_name,
                    release_speed,
                    release_pos_x,
                    release_pos_y,
                    release_pos_z,
                    pitch_zone,
                    hit_location,
                    hit_trajectory,
                    hit_launch_angle,
                    is_in_play,
                    plate_x,
                    plate_z,
                    pitch_pfx_x,
                    pitch_pfx_z,
                    pitch_spin_dir,
                    play_type,
                    hit_x,
                    hit_y,
                    pitch_vx0,
                    pitch_vy0,
                    pitch_vz0,
                    pitch_ax,
                    pitch_az,
                    pitch_ay,
                    strike_zone_top,
                    strike_zone_bottom,
                    hit_distance,
                    pitch_spin_rate,
                    pitch_extension,
                    pitch_num,
                    play_start_datetime,
                    play_end_datetime,
                    play_balls,
                    play_strikes,
                    play_outs,
                    fielder_2,
                    fielder_3,
                    fielder_4,
                    fielder_5,
                    fielder_6,
                    fielder_7,
                    fielder_8,
                    fielder_9,
                )

                game_df.fillna(value=np.NaN, inplace=True)
                game_df = pd.concat([game_df, play_df], ignore_index=True)

            del is_pitch

        del (
            player_name,
            batter_id,
            pitcher_id,
            sequence_description,
            inning,
            top_bot,
            at_bat_num,
            batter_side,
            pitcher_hand,
            event,
            on_1b,
            on_2b,
            on_3b,
        )

        home_score = post_home_score
        away_score = post_away_score
        del post_home_score, post_away_score

    return game_df


def get_month_milb_pbp(
    season: int,
    month: int,
    level="AAA",
    cache_data=False,
    cache_dir="",
    save=True
):
    """ """

    game_df = pd.DataFrame()
    pbp_df = pd.DataFrame()
    sched_df = pd.DataFrame()

    if (
        (level.lower() == "aaa")
        or (level.lower() == "triple-a")
        or (level.lower() == "triple a")
    ):
        sched_df = load_milb_schedule(season, "AAA")
    elif (
        (level.lower() == "aa")
        or (level.lower() == "double-a")
        or (level.lower() == "double a")
    ):
        if season == 2010:
            sched_df = pd.read_csv(
                "https://github.com/armstjc/milb-data-repository/" +
                f"releases/download/schedule/{season}_aa_schedule.csv"
            )
        else:
            sched_df = load_milb_schedule(season, "AA")
    elif (
        (level.lower() == "a+")
        or (level.lower() == "high-a")
        or (level.lower() == "high a")
    ):
        if season == 2011:
            sched_df = pd.read_csv(
                "https://github.com/armstjc/milb-data-repository/" +
                f"releases/download/schedule/{season}_a+_schedule.csv"
            )
        else:
            sched_df = load_milb_schedule(season, "A+")
    elif (
        (level.lower() == "a")
        or (level.lower() == "single-a")
        or (level.lower() == "single-a")
    ):
        if season == 2010 or season == 2013 or season == 2014:
            sched_df = pd.read_csv(
                "https://github.com/armstjc/milb-data-repository/" +
                f"releases/download/schedule/{season}_a_schedule.csv"
            )
        else:
            sched_df = load_milb_schedule(season, "A")
    elif (
        (level.lower() == "a-")
        or (level.lower() == "short-a")
        or (level.lower() == "short a")
    ):
        sched_df = load_milb_schedule(season, "A-")
    elif (
        (level.lower() == "rk")
        or (level.lower() == "rok")
        or (level.lower() == "rookie")
    ):
        sched_df = load_milb_schedule(season, "rk")

    print(sched_df)
    
    sched_df = sched_df[sched_df["game_month"] == month]
    print(sched_df)
    arr_check = sched_df["status_abstract_game_state"].to_numpy()
    arr_check = np.unique(arr_check)
    print(arr_check)
    sched_df = sched_df[sched_df["status_abstract_game_state"] == "Final"]
    # sched_df = sched_df.loc[
    #     (sched_df["game_month"] == month)
    #     & (sched_df["status_detailed_state"] != "Cancelled")
    #     & (sched_df["status_detailed_state"] != "Postponed")
    #     & (sched_df["status_detailed_state"] != "In Progress")
    #     & (sched_df["status_detailed_state"] != "Scheduled")
    # ]
    print(sched_df["status_abstract_game_state"])

    game_ids_arr = sched_df["game_pk"].to_numpy()

    if len(game_ids_arr) > 30 and cache_data is False:
        print(
            "HEY!\nThat's a ton of data you want to access." +
            "\nPlease cache this data in the future to avoid severe data loss!"
        )

    for game_id in tqdm(game_ids_arr):
        try:
            game_df = get_milb_game_pbp(
                game_id=game_id, cache_data=cache_data, cache_dir=cache_dir
            )
            pbp_df = pd.concat([pbp_df, game_df], ignore_index=True)
        except Exception as e:
            print(f"Unhandled use case. Error Details:\n{e}")

        # game_df = get_milb_game_pbp(
        #     game_id=game_id, cache_data=cache_data, cache_dir=cache_dir)
        pbp_df = pd.concat([pbp_df, game_df], ignore_index=True)

    if save is True and len(pbp_df) > 0:
        pbp_df.to_csv(
            f"pbp/{season}_{month}_{level.lower()}_pbp.csv",
            index=False
        )

    return pbp_df


if __name__ == "__main__":
    print("starting up")

    now = datetime.now()
    c_dir = "D:/"
    start_month = 1
    # This is to ensure that any game played in December
    # for this level is downloaded.
    end_month = 13

    parser = argparse.ArgumentParser()
    parser.add_argument("--season", type=int, required=True)
    parser.add_argument("--level", type=str, required=True)
    args = parser.parse_args()

    season = args.season

    if season == now.year and now.month >= 11:
        start_month = now.month - 5
        end_month = now.month - 4
    elif season == now.year and now.month <= 3:
        start_month = now.month + 3
        end_month = now.month + 4
        season -= 1
    elif season == now.year \
            and now.day <= 7 and platform.system() == "Windows":
        # This is here to ensure that a game being played
        # in between 2 months
        # (like a game starting on March 31st but ending on April 1st)
        # is not skipped,
        # and there's multiple opportunities
        # to make sure nothing is skipped.
        start_month = now.month - 1
        end_month = now.month + 1

    elif season == now.year:
        start_month = now.month
        end_month = now.month + 1

    lg_level = args.level


    for i in range(start_month, end_month):
        # if platform.system() == "Windows":
        #     print(
        #         f"Getting {i}/{season} PBP data " +
        #         f"in the {lg_level} level of MiLB."
        #     )
        #     get_month_milb_pbp(
        #         season, i, level=lg_level, cache_data=True, cache_dir=c_dir
        #     )
        # else:
        #     print(
        #         f"Getting {i}/{season} PBP data " +
        #         f"in the {lg_level} level of MiLB."
        #     )
        #     get_month_milb_pbp(season, i, level=lg_level)
        get_month_milb_pbp(season, i, level=lg_level)

    # for i in range(start_month, end_month):
    #     get_month_milb_pbp(
    #         2009,
    #         i,
    #         level="aaa",
    #         cache_data=True,
    #         cache_dir=c_dir
    #     )
