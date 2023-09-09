import json
import os
import time
from datetime import datetime
from urllib.request import urlopen

import pandas as pd
import requests
from tqdm import tqdm


def get_alt_schedule(season: int, level: str = "a"):
    """
    DO NOT CALL DIRECTLY! 
    Only intended to be called when getting 
    the 2010, 2013, and/or 2014 single A schedules due to a HTTP 500 error that pops up when 
    attempting to use `get_milb_schedule()` in this specific edge case.
    """
    schedule_df = pd.DataFrame()
    row_df = pd.DataFrame()

    if season >= 2010 or season <= 2014:
        pass
    else:
        raise ValueError(
            'See function description. Do not call this function unless you want 2010, 2013, and/or 2014 single A schedules.')

    for i in range(4, 11):
        if i < 9:
            first_month = f"0{i}"
            last_month = f"0{i+1}"
        elif i == 9:
            first_month = f"9"
            last_month = f"10"
        elif i > 9:
            first_month = f"{i}"
            last_month = f"{i+1}"

        if level.lower() == "a":
            url = f"https://bdfed.stitch.mlbinfra.com/bdfed/transform-milb-schedule?stitch_env=prod&sortTemplate=5&sportId=14&startDate={season}-{first_month}-01&endDate={season}-{last_month}-01&gameType=E&&gameType=S&&gameType=R&&gameType=F&&gameType=D&&gameType=L&&gameType=W&&gameType=A&&gameType=C&language=en&leagueId=&contextTeamId=milb&teamId="
            print(
                f'\nGetting the Single-A schedule for {first_month}/{season}.')

        elif level.lower() == "a+":
            url = f"https://bdfed.stitch.mlbinfra.com/bdfed/transform-milb-schedule?stitch_env=prod&sortTemplate=5&sportId=13&startDate={season}-{first_month}-01&endDate={season}-{last_month}-01&gameType=E&&gameType=S&&gameType=R&&gameType=F&&gameType=D&&gameType=L&&gameType=W&&gameType=A&&gameType=C&language=en&leagueId=&contextTeamId=milb&teamId="
            print(f'\nGetting the High-A schedule for {first_month}/{season}.')

        elif level.lower() == "aa":
            print(
                f'\nGetting the Double-A schedule for {first_month}/{season}.')
            url = f"https://bdfed.stitch.mlbinfra.com/bdfed/transform-milb-schedule?stitch_env=prod&sortTemplate=5&sportId=12&startDate={season}-{first_month}-01&endDate={season}-{last_month}-01&gameType=E&&gameType=S&&gameType=R&&gameType=F&&gameType=D&&gameType=L&&gameType=W&&gameType=A&&gameType=C&language=en&leagueId=&contextTeamId=milb&teamId="

        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36"}
        response = requests.get(url, headers=headers)

        time.sleep(2)

        if response.status_code == 200:
            pass
        elif response.status_code == 403:
            raise ConnectionRefusedError(
                'The MiLB API is actively refusing your connection.\nHTTP Error Code:\t403')
        else:
            raise ConnectionError(
                f'Could not establish a connection to the MiLB API.\nHTTP Error Code:\t{response.code}')

        json_data = json.loads(response.text)

        for d in tqdm(json_data['dates']):
            game_date = d['date']
            game_year, game_month, game_day = str(game_date).split('-')
            game_year = int(game_year)
            game_month = int(game_month)
            game_day = int(game_day)

            for i in d['games']:
                game_id = int(i['gamePk'])
                game_link = i['link']
                game_type = i['gameType']
                try:
                    game_season = int(i['season'])
                except:
                    game_season = float(i['season'])
                    game_season = int(game_season)
                game_date = i['gameDate']
                game_official_date = i['officialDate']

                try:
                    is_tie = i['isTie']
                except:
                    is_tie = False

                try:
                    game_number = i['seriesStatus']['gameNumber']
                except:
                    game_number = None

                try:
                    games_in_series = i['gamesInSeries']
                except:
                    games_in_series = None

                try:
                    series_game_number = i['seriesGameNumber']
                except:
                    series_game_number = None

                public_facing = i['publicFacing']
                double_header = i['doubleHeader']
                gameday_type = i['gamedayType']
                tiebreaker = i['tiebreaker']
                calendar_event_id = i['calendarEventID']
                day_night = i['dayNight']
                scheduled_innings = i['scheduledInnings']
                reverse_home_away_status = i['reverseHomeAwayStatus']

                try:
                    inning_break_length = i['inningBreakLength']
                except:
                    inning_break_length = None

                series_description = i['seriesDescription']
                record_source = i['recordSource']
                try:
                    if_necessary = i['ifNecessary']
                except:
                    if_necessary = None

                try:
                    if_necessary_description = i['ifNecessaryDescription']
                except:
                    if_necessary_description = None

                try:
                    game_description = i['description']
                except:
                    game_description = None

                status_abstract_game_state = i['status']['abstractGameState']
                status_coded_game_state = i['status']['codedGameState']
                status_detailed_state = i['status']['detailedState']
                status_status_code = i['status']['statusCode']
                status_start_time_tbd = i['status']['startTimeTBD']
                status_abstract_game_code = i['status']['abstractGameCode']

                try:
                    teams_away_score = i['teams']['away']['score']
                except:
                    teams_away_score = None

                try:
                    teams_away_is_winner = i['teams']['away']['isWinner']
                except:
                    teams_away_is_winner = None

                teams_away_spit_squad = i['teams']['away']['splitSquad']

                try:
                    teams_away_series_number = i['teams']['away']['seriesNumber']
                except:
                    teams_away_series_number = None

                teams_away_league_record_wins = int(
                    i['teams']['away']['leagueRecord']['wins'])
                teams_away_league_record_losses = int(
                    i['teams']['away']['leagueRecord']['losses'])
                teams_away_league_record_pct = float(
                    i['teams']['away']['leagueRecord']['pct'])
                teams_away_team_id = i['teams']['away']['team']['id']
                teams_away_team_name = i['teams']['away']['team']['name']
                teams_away_team_link = i['teams']['away']['team']['link']

                try:
                    teams_home_score = i['teams']['home']['score']
                except:
                    teams_home_score = None

                try:
                    teams_home_is_winner = i['teams']['home']['isWinner']
                except:
                    teams_home_is_winner = None

                teams_home_spit_squad = i['teams']['home']['splitSquad']

                try:
                    teams_home_series_number = i['teams']['home']['seriesNumber']
                except:
                    teams_home_series_number = None

                teams_home_league_record_wins = int(
                    i['teams']['home']['leagueRecord']['wins'])
                teams_home_league_record_losses = int(
                    i['teams']['home']['leagueRecord']['losses'])
                teams_home_league_record_pct = float(
                    i['teams']['home']['leagueRecord']['pct'])
                teams_home_team_id = i['teams']['home']['team']['id']
                teams_home_team_name = i['teams']['home']['team']['name']
                teams_home_team_link = i['teams']['home']['team']['link']

                league_level_id = int(
                    i['teams']['home']['team']['sport']['id'])
                league_level_link = i['teams']['home']['team']['sport']['link']
                league_level_name = i['teams']['home']['team']['sport']['name']

                row_df = pd.DataFrame({
                    'game_pk': game_id,
                    'link': game_link,
                    'game_type': game_type,
                    'season': game_season,
                    'game_date': game_date,  # 2019-04-04T22:30:00Z
                    'official_date': game_official_date,
                    'is_tie': is_tie,
                    'game_number': game_number,
                    'public_facing': public_facing,
                    'double_header': double_header,
                    'gameday_type': gameday_type,
                    'tiebreaker': tiebreaker,
                    'calendar_event_id': calendar_event_id,
                    'season_display': game_season,
                    'day_night': day_night,
                    'scheduled_innings': scheduled_innings,
                    'reverse_home_away_status': reverse_home_away_status,
                    'inning_break_length': inning_break_length,
                    'games_in_series': games_in_series,
                    'series_game_number': series_game_number,
                    'series_description': series_description,
                    'record_source': record_source,
                    'if_necessary': if_necessary,
                    'if_necessary_description': if_necessary_description,
                    'description': game_description,  # at Spectrum Field
                    'status_abstract_game_state': status_abstract_game_state,
                    'status_coded_game_state': status_coded_game_state,
                    'status_detailed_state': status_detailed_state,
                    'status_status_code': status_status_code,
                    'status_start_time_tbd': status_start_time_tbd,
                    'status_abstract_game_code': status_abstract_game_code,
                    'teams_away_score': teams_away_score,
                    'teams_away_is_winner': teams_away_is_winner,
                    'teams_away_spit_squad': teams_away_spit_squad,
                    'teams_away_series_number': teams_away_series_number,
                    'teams_away_league_record_wins': teams_away_league_record_wins,
                    'teams_away_league_record_losses': teams_away_league_record_losses,
                    'teams_away_league_record_pct': teams_away_league_record_pct,
                    'teams_away_team_id': teams_away_team_id,
                    'teams_away_team_name': teams_away_team_name,
                    'teams_away_team_link': teams_away_team_link,  # /api/v1/teams/566
                    'teams_home_score': teams_home_score,
                    'teams_home_is_winner': teams_home_is_winner,
                    'teams_home_spit_squad': teams_home_spit_squad,
                    'teams_home_series_number': teams_home_series_number,
                    'teams_home_league_record_wins': teams_home_league_record_wins,
                    'teams_home_league_record_losses': teams_home_league_record_losses,
                    'teams_home_league_record_pct': teams_home_league_record_pct,
                    'teams_home_team_id': teams_home_team_id,
                    'teams_home_team_name': teams_home_team_name,
                    'teams_home_team_link': teams_home_team_link,  # /api/v1/teams/566
                    'game_year': game_year,
                    'game_month': game_month,
                    'game_day': game_day,
                    'league_level_id': league_level_id,
                    'league_level_link': league_level_link,
                    'league_level_name': league_level_name
                }, index=[0]
                )

                schedule_df = pd.concat(
                    [schedule_df, row_df], ignore_index=True)

                del row_df
                del game_id, game_link, game_type, game_season, \
                    game_date, game_official_date, is_tie, \
                    public_facing, double_header, gameday_type, \
                    tiebreaker, calendar_event_id, day_night, \
                    scheduled_innings, reverse_home_away_status, \
                    inning_break_length, games_in_series, series_game_number, \
                    series_description, record_source, if_necessary,\
                    if_necessary_description, game_description, \
                    status_abstract_game_state, status_coded_game_state, \
                    status_detailed_state, status_status_code, \
                    status_start_time_tbd, status_abstract_game_code, \
                    teams_away_score, teams_away_is_winner, \
                    teams_away_spit_squad, teams_away_series_number, \
                    teams_away_league_record_wins, teams_away_league_record_losses, \
                    teams_away_league_record_pct, teams_away_team_id, \
                    teams_away_team_name, teams_home_score, teams_home_is_winner, \
                    teams_home_spit_squad, teams_home_series_number, \
                    teams_home_league_record_wins, teams_home_league_record_losses, \
                    teams_home_league_record_pct, teams_home_team_id, \
                    teams_home_team_name, league_level_id, \
                    league_level_link, league_level_name

    return schedule_df


def get_milb_schedule(season: int, level="AAA", cache_data=False, cache_dir=""):
    """
    Gets and parses a list of MiLB games that happened between two dates.

    Parameters
    ----------
    `season` (int, mandatory):
        The season you want MiLB schedule info from.

    `level` (str, semi-optional) = `AAA`:
        The MiLB level you want schedule info from. 
        The following inputs work for each level, regardles of case:
        - All Levels = 'all'
        - AAA = 'triple-a', 'triple a', 'aaa'
        - AA = 'double-a', 'double a', 'aa'
        - A, A+, A- = 'single-a', 'single a', 'a'
        - Rookie ball (any league) = 'rk', 'rok', 'rookie'

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
    """

    if level.lower() == "a" and season == 2010:
        df = get_alt_schedule(2010)
        return df
    if level.lower() == "a" and season == 2013:
        df = get_alt_schedule(2013)
        return df
    elif level.lower() == "a" and season == 2014:
        df = get_alt_schedule(2014)
        return df
    elif level.lower() == "a+" and season == 2011:
        df = get_alt_schedule(2011, "a+")
        return df
    elif level.lower() == "aa" and season == 2010:
        df = get_alt_schedule(2010, "aa")
        return df

    if cache_data == True and (cache_dir == "" or cache_dir == None):
        home_dir = os.path.expanduser('~')

        try:
            os.mkdir(f"{home_dir}/.milb/")
        except:
            print(f"Cached directory already exists.")

        try:
            os.mkdir(f"{home_dir}/.milb/schedule/")
        except:
            print(
                f"Additional cached directories have been previously created and located.")

    elif cache_data == True and (cache_dir != "" or cache_dir != None):
        try:
            os.mkdir(f"{cache_dir}/.milb/")
        except:
            print(f"Cached directory already exists.")

        try:
            os.mkdir(f"{cache_dir}/.milb/schedule/")
        except:
            print(
                f"Additional cached directories have been previously created and located.")

    # season = 2023
    schedule_df = pd.DataFrame()
    row_df = pd.DataFrame()

    if cache_data == True and (cache_dir == "" or cache_dir == None):
        try:
            with open(f"{home_dir}/.milb/schedule/{d}.json", 'r') as f:
                json_string = f.read()

            json_data = json.loads(json_string)
            del json_string
        except:
            url = f"https://statsapi.mlb.com/api/v1/schedule?lang=en&sportId=11&sportID=12&sportID=13&sportID=14&sportID=15&sportID=16&hydrate=team(venue(timezone)),venue(timezone),game(seriesStatus,seriesSummary,tickets,promotions,sponsorships,content(summary,media(epg))),seriesStatus,seriesSummary,linescore&season={season}&eventTypes=primary&scheduleTypes=games,events,xref"
            response = urlopen(url)
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
            with open(f"{home_dir}/.milb/schedule/{d}.json", 'w+') as f:
                f.write(json.dumps(json_data, indent=2))

    elif cache_data == True and (cache_dir != "" or cache_dir != None):
        try:
            with open(f"{cache_data}/.milb/schedule/{d}.json", 'r') as f:
                json_string = f.read()

            json_data = json.loads(json_string)
            del json_string
        except:
            url = f"https://statsapi.mlb.com/api/v1/schedule?lang=en&sportId=11&sportID=12&sportID=13&sportID=14&sportID=15&sportID=16&hydrate=team(venue(timezone)),venue(timezone),game(seriesStatus,seriesSummary,tickets,promotions,sponsorships,content(summary,media(epg))),seriesStatus,seriesSummary,linescore&season={season}&&eventTypes=primary&scheduleTypes=games,events,xref"
            response = urlopen(url)
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
            with open(f"{cache_data}/.milb/schedule/{d}.json", 'w+') as f:
                f.write(json.dumps(json_data, indent=2))

    else:
        if level.lower() == 'all':
            url = f"https://statsapi.mlb.com/api/v1/schedule?lang=en&sportId=11&sportID=12&sportID=13&sportID=14&sportID=15&sportID=16&hydrate=team(venue(timezone)),venue(timezone),game(seriesStatus,seriesSummary,tickets,promotions,sponsorships,content(summary,media(epg))),seriesStatus,seriesSummary,linescore&season={season}&eventTypes=primary&scheduleTypes=games,events,xref"
        elif (level.lower() == 'aaa') or (level.lower() == 'triple-a') or (level.lower() == 'triple a'):
            url = f"https://statsapi.mlb.com/api/v1/schedule?lang=en&sportId=11&hydrate=team(venue(timezone)),venue(timezone),game(seriesStatus,seriesSummary,tickets,promotions,sponsorships,content(summary,media(epg))),seriesStatus,seriesSummary,linescore&season={season}&eventTypes=primary&scheduleTypes=games,events,xref"
        elif (level.lower() == 'aa') or (level.lower() == 'double-a') or (level.lower() == 'double a'):
            url = f"https://statsapi.mlb.com/api/v1/schedule?lang=en&sportId=12&hydrate=team(venue(timezone)),venue(timezone),game(seriesStatus,seriesSummary,tickets,promotions,sponsorships,content(summary,media(epg))),seriesStatus,seriesSummary,linescore&season={season}&eventTypes=primary&scheduleTypes=games,events,xref"
        elif (level.lower() == 'a+') or (level.lower() == 'high-a') or (level.lower() == 'high a'):
            url = f"https://statsapi.mlb.com/api/v1/schedule?lang=en&sportId=13&hydrate=team(venue(timezone)),venue(timezone),game(seriesStatus,seriesSummary,tickets,promotions,sponsorships,content(summary,media(epg))),seriesStatus,seriesSummary,linescore&season={season}&eventTypes=primary&scheduleTypes=games,events,xref"
        elif (level.lower() == 'a') or (level.lower() == 'single-a') or (level.lower() == 'single a'):
            url = f"https://statsapi.mlb.com/api/v1/schedule?lang=en&sportId=14&hydrate=team(venue(timezone)),venue(timezone),game(seriesStatus,seriesSummary,tickets,promotions,sponsorships,content(summary,media(epg))),seriesStatus,seriesSummary,linescore&season={season}&eventTypes=primary&scheduleTypes=games,events,xref"
        elif (level.lower() == 'a-') or (level.lower() == 'short-a') or (level.lower() == 'short a'):
            url = f"https://statsapi.mlb.com/api/v1/schedule?lang=en&sportId=15&hydrate=team(venue(timezone)),venue(timezone),game(seriesStatus,seriesSummary,tickets,promotions,sponsorships,content(summary,media(epg))),seriesStatus,seriesSummary,linescore&season={season}&eventTypes=primary&scheduleTypes=games,events,xref"
        elif (level.lower() == 'rk') or (level.lower() == 'rok') or (level.lower() == 'rookie'):
            url = f"https://statsapi.mlb.com/api/v1/schedule?lang=en&sportId=16&hydrate=team(venue(timezone)),venue(timezone),game(seriesStatus,seriesSummary,tickets,promotions,sponsorships,content(summary,media(epg))),seriesStatus,seriesSummary,linescore&season={season}&eventTypes=primary&scheduleTypes=games,events,xref"
        else:
            raise ValueError(f'Unhandled MiLB level:\n\t{level}')

        # url = f""
        response = urlopen(url)
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

        # with open('test.json', 'w+') as f:
        #     f.write(json.dumps(json_data, indent=2))

    for d in tqdm(json_data['dates']):
        game_date = d['date']
        game_year, game_month, game_day = str(game_date).split('-')
        game_year = int(game_year)
        game_month = int(game_month)
        game_day = int(game_day)

        for i in d['games']:
            game_id = int(i['gamePk'])
            game_link = i['link']
            game_type = i['gameType']
            try:
                game_season = int(i['season'])
            except:
                game_season = float(i['season'])
                game_season = int(game_season)
            game_date = i['gameDate']
            game_official_date = i['officialDate']

            try:
                is_tie = i['isTie']
            except:
                is_tie = False

            try:
                game_number = i['seriesStatus']['gameNumber']
            except:
                game_number = None

            try:
                games_in_series = i['gamesInSeries']
            except:
                games_in_series = None

            try:
                series_game_number = i['seriesGameNumber']
            except:
                series_game_number = None

            public_facing = i['publicFacing']
            double_header = i['doubleHeader']
            gameday_type = i['gamedayType']
            tiebreaker = i['tiebreaker']
            calendar_event_id = i['calendarEventID']
            day_night = i['dayNight']
            scheduled_innings = i['scheduledInnings']
            reverse_home_away_status = i['reverseHomeAwayStatus']

            try:
                inning_break_length = i['inningBreakLength']
            except:
                inning_break_length = None

            series_description = i['seriesDescription']
            record_source = i['recordSource']
            try:
                if_necessary = i['ifNecessary']
            except:
                if_necessary = None

            try:
                if_necessary_description = i['ifNecessaryDescription']
            except:
                if_necessary_description = None

            try:
                game_description = i['description']
            except:
                game_description = None

            status_abstract_game_state = i['status']['abstractGameState']
            status_coded_game_state = i['status']['codedGameState']
            status_detailed_state = i['status']['detailedState']
            status_status_code = i['status']['statusCode']
            status_start_time_tbd = i['status']['startTimeTBD']
            status_abstract_game_code = i['status']['abstractGameCode']

            try:
                teams_away_score = i['teams']['away']['score']
            except:
                teams_away_score = None

            try:
                teams_away_is_winner = i['teams']['away']['isWinner']
            except:
                teams_away_is_winner = None

            teams_away_spit_squad = i['teams']['away']['splitSquad']

            try:
                teams_away_series_number = i['teams']['away']['seriesNumber']
            except:
                teams_away_series_number = None

            teams_away_league_record_wins = int(
                i['teams']['away']['leagueRecord']['wins'])
            teams_away_league_record_losses = int(
                i['teams']['away']['leagueRecord']['losses'])
            teams_away_league_record_pct = float(
                i['teams']['away']['leagueRecord']['pct'])
            teams_away_team_id = i['teams']['away']['team']['id']
            teams_away_team_name = i['teams']['away']['team']['name']
            teams_away_team_link = i['teams']['away']['team']['link']

            try:
                teams_home_score = i['teams']['home']['score']
            except:
                teams_home_score = None

            try:
                teams_home_is_winner = i['teams']['home']['isWinner']
            except:
                teams_home_is_winner = None

            teams_home_spit_squad = i['teams']['home']['splitSquad']

            try:
                teams_home_series_number = i['teams']['home']['seriesNumber']
            except:
                teams_home_series_number = None

            teams_home_league_record_wins = int(
                i['teams']['home']['leagueRecord']['wins'])
            teams_home_league_record_losses = int(
                i['teams']['home']['leagueRecord']['losses'])
            teams_home_league_record_pct = float(
                i['teams']['home']['leagueRecord']['pct'])
            teams_home_team_id = i['teams']['home']['team']['id']
            teams_home_team_name = i['teams']['home']['team']['name']
            teams_home_team_link = i['teams']['home']['team']['link']

            league_level_id = int(i['teams']['home']['team']['sport']['id'])
            league_level_link = i['teams']['home']['team']['sport']['link']
            league_level_name = i['teams']['home']['team']['sport']['name']

            row_df = pd.DataFrame({
                'game_pk': game_id,
                'link': game_link,
                'game_type': game_type,
                'season': game_season,
                'game_date': game_date,  # 2019-04-04T22:30:00Z
                'official_date': game_official_date,
                'is_tie': is_tie,
                'game_number': game_number,
                'public_facing': public_facing,
                'double_header': double_header,
                'gameday_type': gameday_type,
                'tiebreaker': tiebreaker,
                'calendar_event_id': calendar_event_id,
                'season_display': game_season,
                'day_night': day_night,
                'scheduled_innings': scheduled_innings,
                'reverse_home_away_status': reverse_home_away_status,
                'inning_break_length': inning_break_length,
                'games_in_series': games_in_series,
                'series_game_number': series_game_number,
                'series_description': series_description,
                'record_source': record_source,
                'if_necessary': if_necessary,
                'if_necessary_description': if_necessary_description,
                'description': game_description,  # at Spectrum Field
                'status_abstract_game_state': status_abstract_game_state,
                'status_coded_game_state': status_coded_game_state,
                'status_detailed_state': status_detailed_state,
                'status_status_code': status_status_code,
                'status_start_time_tbd': status_start_time_tbd,
                'status_abstract_game_code': status_abstract_game_code,
                'teams_away_score': teams_away_score,
                'teams_away_is_winner': teams_away_is_winner,
                'teams_away_spit_squad': teams_away_spit_squad,
                'teams_away_series_number': teams_away_series_number,
                'teams_away_league_record_wins': teams_away_league_record_wins,
                'teams_away_league_record_losses': teams_away_league_record_losses,
                'teams_away_league_record_pct': teams_away_league_record_pct,
                'teams_away_team_id': teams_away_team_id,
                'teams_away_team_name': teams_away_team_name,
                'teams_away_team_link': teams_away_team_link,  # /api/v1/teams/566
                'teams_home_score': teams_home_score,
                'teams_home_is_winner': teams_home_is_winner,
                'teams_home_spit_squad': teams_home_spit_squad,
                'teams_home_series_number': teams_home_series_number,
                'teams_home_league_record_wins': teams_home_league_record_wins,
                'teams_home_league_record_losses': teams_home_league_record_losses,
                'teams_home_league_record_pct': teams_home_league_record_pct,
                'teams_home_team_id': teams_home_team_id,
                'teams_home_team_name': teams_home_team_name,
                'teams_home_team_link': teams_home_team_link,  # /api/v1/teams/566
                'game_year': game_year,
                'game_month': game_month,
                'game_day': game_day,
                'league_level_id': league_level_id,
                'league_level_link': league_level_link,
                'league_level_name': league_level_name
            }, index=[0]
            )

            schedule_df = pd.concat([schedule_df, row_df], ignore_index=True)

            del row_df
            del game_id, game_link, game_type, game_season, \
                game_date, game_official_date, is_tie, \
                public_facing, double_header, gameday_type, \
                tiebreaker, calendar_event_id, day_night, \
                scheduled_innings, reverse_home_away_status, \
                inning_break_length, games_in_series, series_game_number, \
                series_description, record_source, if_necessary,\
                if_necessary_description, game_description, \
                status_abstract_game_state, status_coded_game_state, \
                status_detailed_state, status_status_code, \
                status_start_time_tbd, status_abstract_game_code, \
                teams_away_score, teams_away_is_winner, \
                teams_away_spit_squad, teams_away_series_number, \
                teams_away_league_record_wins, teams_away_league_record_losses, \
                teams_away_league_record_pct, teams_away_team_id, \
                teams_away_team_name, teams_home_score, teams_home_is_winner, \
                teams_home_spit_squad, teams_home_series_number, \
                teams_home_league_record_wins, teams_home_league_record_losses, \
                teams_home_league_record_pct, teams_home_team_id, \
                teams_home_team_name, league_level_id, \
                league_level_link, league_level_name

    return schedule_df


if __name__ == "__main__":
    now = datetime.now()
    # for season in range(2005, now.year+1):
    #     try:
    #         print(f'Getting {season} Triple-A schedules.')
    #         aaa_df = get_milb_schedule(season, 'aaa')
    #         if len(aaa_df) > 0:
    #             aaa_df.to_csv(
    #                 f'schedule/{season}_aaa_schedule.csv', index=False)

    #         del aaa_df

    #     except Exception as e:
    #         print(
    #             f'Could not download {season} Triple-A schedules.\nReason:\n{e}')

    #     try:
    #         print(f'Getting {season} Double-A schedules.')
    #         aa_df = get_milb_schedule(season, 'aa')
    #         if len(aa_df) > 0:
    #             aa_df.to_csv(f'schedule/{season}_aa_schedule.csv', index=False)

    #         del aa_df

    #     except Exception as e:
    #         print(
    #             f'Could not download {season} Double-A schedules.\nReason:\n{e}')

    #     try:
    #         print(f'Getting {season} High-A schedules.')
    #         a_df = get_milb_schedule(season, 'a+')
    #         if len(a_df) > 0:
    #             a_df.to_csv(f'schedule/{season}_a+_schedule.csv', index=False)

    #         del a_df

    #     except Exception as e:
    #         print(
    #             f'Could not download {season} High-A schedules.\nReason:\n{e}')

    #     try:
    #         if season != 2013 and season != 2014:
    #             print(f'Getting {season} Single-A schedules.')
    #             a_df = get_milb_schedule(season, 'a')
    #             if len(a_df) > 0:
    #                 a_df.to_csv(
    #                     f'schedule/{season}_a_schedule.csv', index=False)

    #             del a_df

    #     except Exception as e:
    #         print(
    #             f'Could not download {season} Single-A schedules.\nReason:\n{e}')

    #     try:
    #         print(f'Getting {season} Low-A schedules.')
    #         a_df = get_milb_schedule(season, 'a-')
    #         if len(a_df) > 0:
    #             a_df.to_csv(f'schedule/{season}_a-_schedule.csv', index=False)

    #         del a_df

    #     except Exception as e:
    #         print(
    #             f'Could not download {season} Low-A schedules.\nReason:\n{e}')

    #     try:
    #         print(f'Getting {season} Rookie Ball schedules.')
    #         rookie_df = get_milb_schedule(season, 'rk')
    #         if len(rookie_df) > 0:
    #             rookie_df.to_csv(
    #                 f'schedule/{season}_rookie_schedule.csv', index=False)

    #         del rookie_df

    #     except Exception as e:
    #         print(
    #             f'Could not download {season} Rookie Ball schedules.\nReason:\n{e}')

    # a_df = get_milb_schedule(2010, 'a')
    # if len(a_df) > 0:
    #     a_df.to_csv(
    #         f'schedule/{2010}_a_schedule.csv', index=False)

    # a_df = get_milb_schedule(2013, 'a')
    # if len(a_df) > 0:
    #     a_df.to_csv(
    #         f'schedule/{2013}_a_schedule.csv', index=False)

    # a_df = get_milb_schedule(2014, 'a')
    # if len(a_df) > 0:
    #     a_df.to_csv(
    #         f'schedule/{2014}_a_schedule.csv', index=False)

    a_df = get_milb_schedule(2011, 'a+')
    if len(a_df) > 0:
        a_df.to_csv(
            f'schedule/{2011}_a+_schedule.csv', index=False)

    a_df = get_milb_schedule(2010, 'aa')
    if len(a_df) > 0:
        a_df.to_csv(
            f'schedule/{2010}_aa_schedule.csv', index=False)
