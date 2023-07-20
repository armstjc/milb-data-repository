import json
import os
import time
from datetime import datetime
from urllib.request import urlopen

import pandas as pd
from tqdm import tqdm


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

    season = 2023
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
        elif (level.lower() == 'a') or (level.lower() == 'single-a') or (level.lower() == 'single-a'):
            # Put all levels of single A in here to avoid confusion.
            url = f"https://statsapi.mlb.com/api/v1/schedule?lang=en&sportId=13&sportId=14&sportId=15&hydrate=team(venue(timezone)),venue(timezone),game(seriesStatus,seriesSummary,tickets,promotions,sponsorships,content(summary,media(epg))),seriesStatus,seriesSummary,linescore&season={season}&eventTypes=primary&scheduleTypes=games,events,xref"
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
            game_season = int(i['season'])
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
            inning_break_length = i['inningBreakLength']

            series_description = i['seriesDescription']
            record_source = i['recordSource']
            if_necessary = i['ifNecessary']
            if_necessary_description = i['ifNecessaryDescription']

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

            row_df = pd.DataFrame({
                'date': game_date,
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
                teams_home_team_name

    return schedule_df


if __name__ == "__main__":

    for season in range(2021, 2024):
        print(f'Getting Triple-A schedules.')
        aaa_df = get_milb_schedule(season, 'aaa')
        aaa_df.to_csv(f'schedule/{season}_aaa_schedule.csv', index=False)
        del aaa_df

        print(f'Getting Double-A schedules.')
        aa_df = get_milb_schedule(season, 'aa')
        aa_df.to_csv(f'schedule/{season}_aa_schedule.csv', index=False)
        del aa_df

        print(f'Getting Single-A schedules.')
        a_df = get_milb_schedule(season, 'a')
        a_df.to_csv(f'schedule/{season}_a_schedule.csv', index=False)
        del a_df

        print(f'Getting Rookie Ball schedules.')
        rookie_df = get_milb_schedule(season, 'rk')
        rookie_df.to_csv(f'schedule/{season}_rookie_schedule.csv', index=False)
        del rookie_df
