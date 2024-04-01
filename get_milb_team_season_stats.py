import argparse
import json
import time
from datetime import datetime

import numpy as np
import pandas as pd
import requests
from tqdm import tqdm

# from get_milb_teams import get_milb_team_list


def get_milb_team_season_stats(
    season: int, level: str, stats_type: str = "batting", save: bool = False
) -> pd.DataFrame:
    """ """
    now = datetime.now()
    row_df = pd.DataFrame()
    season_df = pd.DataFrame()

    if season < 2005:
        raise ValueError("\n`season` must be greater than 2005")
    elif season > (now.year + 1):
        raise ValueError(f"\n`season` cannot be greater than {now.year+1}")

    if level.lower() == "aaa":
        level_id = 11
        level_abv = "AAA"
    elif level.lower() == "aa":
        level_id = 12
        level_abv = "AA"
    elif level.lower() == "a+":
        level_id = 13
        level_abv = "A+"
    elif level.lower() == "a":
        level_id = 14
        level_abv = "A"
    elif level.lower() == "a-":
        level_id = 15
        level_abv = "A-"
    elif level.lower() == "rk":
        level_id = 16
        level_abv = "RK"
    else:
        raise ValueError(
            '\n`level` must be set to "AAA", "AA", "A+", "A", "A-", or "RK".'
        )

    if stats_type.lower() != "batting" and stats_type.lower() != "pitching":
        raise ValueError(
            '\n`stats_type` must be set to "batting" or "pitching".'
        )

    pitching_url = "https://bdfed.stitch.mlbinfra.com/bdfed/stats/team" +\
        f"?stitch_env=prod&season={season}&sportId={level_id}&gameType=R" +\
        "&group=pitching&order=desc&sortStat=onBasePlusSlugging" +\
        "&stats=season&limit=200&offset=0"
    batting_url = "https://bdfed.stitch.mlbinfra.com/bdfed/stats/team" +\
        f"?stitch_env=prod&season={season}&sportId={level_id}&gameType=R" +\
        "&group=hitting&order=desc&sortStat=onBasePlusSlugging" +\
        "&stats=season&limit=200&offset=0"

    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4) " +
        "AppleWebKit/537.36 (KHTML, like Gecko) " +
        "Chrome/83.0.4103.97 Safari/537.36"
    }

    if stats_type.lower() == "batting":
        response = requests.get(batting_url, headers=headers)
        time.sleep(0.1)

        if response.status_code == 200:
            pass
        elif response.status_code == 403:
            raise ConnectionRefusedError(
                "\nThe MiLB API is actively refusing your connection." +
                "\nHTTP Error Code:\t403"
            )
        else:
            raise ConnectionError(
                "\nCould not establish a connection to the MiLB API." +
                f"\nHTTP Error Code:\t{response.code}"
            )

        json_data = json.loads(response.text)

        if json_data["totalSplits"] == 0:
            # If true, we don't have data.
            print(f"\nNo stats found in the {season} season.")
            return pd.DataFrame()

        # if len(json_data["stats"]) == 0:
        if "stats" in json_data:
            # If true, we don't have data.
            print(f"\nNo stats found in the {season} season.")
            return pd.DataFrame()

        for i in json_data["stats"]:
            row_df = pd.DataFrame({"season": season}, index=[0])
            row_df["level_abv"] = level_abv
            row_df["team_id"] = i["teamId"]
            row_df["team_abv"] = i["teamAbbrev"]
            row_df["team_name"] = i["teamName"]
            row_df["team_short_name"] = i["shortName"]
            row_df["team_nickname"] = i["teamShortName"]

            row_df["league_abv"] = i["leagueAbbrev"]
            row_df["league_name"] = i["leagueName"]
            row_df["league_short_name"] = i["leagueShortName"]

            row_df["batting_G"] = i["gamesPlayed"]

            row_df["batting_PA"] = i["plateAppearances"]
            row_df["batting_AB"] = i["atBats"]
            row_df["batting_R"] = i["runs"]
            row_df["batting_H"] = i["hits"]
            row_df["batting_2B"] = i["doubles"]
            row_df["batting_3B"] = i["triples"]
            row_df["batting_HR"] = i["homeRuns"]
            row_df["batting_RBI"] = i["rbi"]
            row_df["batting_SB"] = i["stolenBases"]
            row_df["batting_CS"] = i["caughtStealing"]

            row_df["batting_BB"] = i["baseOnBalls"]
            row_df["batting_IBB"] = i["intentionalWalks"]
            row_df["batting_SO"] = i["strikeOuts"]

            row_df["batting_TB"] = i["totalBases"]
            row_df["batting_GiDP"] = i["groundIntoDoublePlay"]
            row_df["batting_HBP"] = i["hitByPitch"]

            row_df["batting_PI_faced"] = i["numberOfPitches"]
            row_df["batting_LOB"] = i["leftOnBase"]
            row_df["batting_CI"] = i["catchersInterference"]

            row_df["batting_GO"] = i["groundOuts"]
            row_df["batting_FO"] = i["airOuts"]
            row_df["batting_SH"] = i["sacBunts"]
            row_df["batting_SF"] = i["sacFlies"]

            season_df = pd.concat([season_df, row_df], ignore_index=True)

        # season_df.loc[season_df['batting_AB'] > 0, 'batting_BA'] = round(
        #     season_df['batting_H']
        #     / season_df['batting_AB'],
        #     3
        # )

        # season_df.loc[season_df['batting_AB'] > 0, 'batting_OBP'] = round(
        #     (season_df['batting_H'] +
        #      season_df['batting_BB'] +
        #      season_df['batting_HBP'])
        #     / (
        #         season_df['batting_AB'] +
        #         season_df['batting_BB'] +
        #         season_df['batting_HBP'] +
        #         season_df['batting_SF']),
        #     3
        # )

        # season_df.loc[season_df['batting_AB'] > 0, 'batting_SLG'] = round(
        #     season_df['batting_TB']
        #     / season_df['batting_AB'],
        #     3
        # )

        # season_df['batting_OPS'] = season_df['batting_OBP'] + \
        #     season_df['batting_SLG']

        # season_df.loc[season_df['batting_PA'] > 0, 'batting_PI/PA'] = round(
        #     season_df['batting_PI'] / season_df['batting_PA'],
        #     3
        # )

        # season_df['batting_ISO'] = round(
        #     season_df['batting_SLG'] -
        #     season_df['batting_BA'],
        #     3
        # )
        season_df.loc[season_df["batting_AB"] > 0, "batting_AVG"] = round(
            season_df["batting_H"] / season_df["batting_AB"], 3
        )

        season_df.loc[season_df["batting_PA"] > 0, "batting_OBP"] = round(
            (
                season_df["batting_BB"]
                + season_df["batting_HBP"]
                + season_df["batting_SF"]
            )
            / (
                season_df["batting_AB"]
                + season_df["batting_BB"]
                + season_df["batting_HBP"]
                + season_df["batting_SF"]
            ),
            3,
        )

        season_df.loc[season_df["batting_AB"] > 0, "batting_SLG"] = round(
            season_df["batting_TB"] / season_df["batting_AB"], 3
        )

        season_df["batting_OPS"] = round(
            season_df["batting_OBP"] + season_df["batting_SLG"], 3
        )

        season_df.loc[season_df["batting_AB"] > 0, "batting_ISO"] = round(
            (season_df["batting_TB"] - season_df["batting_H"])
            / season_df["batting_AB"],
            3,
        )

        season_df.loc[season_df["batting_AB"] > 0, "batting_BABiP"] = round(
            (season_df["batting_H"] - season_df["batting_HR"])
            / (
                season_df["batting_AB"]
                - season_df["batting_SO"]
                - season_df["batting_HR"]
                + season_df["batting_SF"]
            ),
            3,
        )

        season_df.loc[season_df["batting_FO"] > 0, "batting_GO/FO"] = round(
            season_df["batting_GO"] / season_df["batting_FO"], 3
        )

        # season_df['wRAA']

        # season_df['wOBA']

        # season_df['wRC+']

    elif stats_type.lower() == "pitching":
        response = requests.get(pitching_url, headers=headers)
        time.sleep(0.1)

        if response.status_code == 200:
            pass
        elif response.status_code == 403:
            raise ConnectionRefusedError(
                "The MiLB API is actively refusing your connection." +
                "\nHTTP Error Code:\t403"
            )
        else:
            raise ConnectionError(
                "Could not establish a connection to the MiLB API." +
                f"\nHTTP Error Code:\t{response.code}"
            )

        json_data = json.loads(response.text)

        if json_data["totalSplits"] == 0:
            # If true, we don't have data.
            print(f"\nNo stats found in the {season} season.")
            return pd.DataFrame()

        # if len(json_data["stats"]) == 0:
        if "stats" in json_data:
            # If true, we don't have data.
            print(f"\nNo stats found in the {season} season.")
            return pd.DataFrame()

        for i in json_data["stats"]:
            row_df = pd.DataFrame({"season": season}, index=[0])

            row_df["team_id"] = i["teamId"]
            row_df["team_abv"] = i["teamAbbrev"]
            row_df["team_name"] = i["teamName"]
            row_df["team_short_name"] = i["shortName"]
            row_df["team_nickname"] = i["teamShortName"]

            row_df["league_abv"] = i["leagueAbbrev"]
            row_df["league_name"] = i["leagueName"]
            row_df["league_short_name"] = i["leagueShortName"]

            row_df["pitching_W"] = i["wins"]
            row_df["pitching_L"] = i["losses"]
            row_df["pitching_W%"] = None

            row_df["pitching_G"] = i["gamesPlayed"]
            row_df["pitching_GS"] = i["gamesStarted"]
            row_df["pitching_ERA"] = None

            row_df["pitching_GF"] = i["gamesFinished"]
            row_df["pitching_CG"] = i["completeGames"]
            row_df["pitching_SHO"] = i["shutouts"]

            row_df["pitching_SVO"] = i["saveOpportunities"]
            row_df["pitching_SV"] = i["saves"]
            row_df["pitching_BS"] = i["blownSaves"]
            row_df["pitching_HLD"] = i["holds"]

            row_df["pitching_IP"] = round(i["outs"] / 3, 3)
            row_df["pitching_IP_str"] = i["inningsPitched"]
            row_df["pitching_BF"] = i["battersFaced"]
            row_df["pitching_AB"] = i["atBats"]
            row_df["pitching_H"] = i["hits"]
            row_df["pitching_R"] = i["runs"]
            row_df["pitching_ER"] = i["earnedRuns"]
            row_df["pitching_2B"] = i["doubles"]
            row_df["pitching_3B"] = i["triples"]
            row_df["pitching_HR"] = i["homeRuns"]
            row_df["pitching_BB"] = i["baseOnBalls"]
            row_df["pitching_IBB"] = i["intentionalWalks"]
            row_df["pitching_SO"] = i["strikeOuts"]
            row_df["pitching_TB"] = i["totalBases"]
            row_df["pitching_HBP"] = i["hitByPitch"]
            row_df["pitching_SB"] = i["stolenBases"]
            row_df["pitching_CS"] = i["caughtStealing"]
            row_df["pitching_GiDP"] = i["groundIntoDoublePlay"]
            row_df["pitching_BK"] = i["balks"]
            row_df["pitching_WP"] = i["wildPitches"]
            row_df["pitching_CI"] = i["catchersInterference"]
            row_df["pitching_PK"] = i["pickoffs"]

            row_df["pitching_GO"] = i["groundOuts"]
            row_df["pitching_FO"] = i["airOuts"]
            row_df["pitching_SH"] = i["sacBunts"]
            row_df["pitching_SF"] = i["sacFlies"]
            row_df["pitching_PI"] = i["numberOfPitches"]
            row_df["pitching_PI_strikes"] = i["strikes"]
            row_df["pitching_PI_balls"] = i["numberOfPitches"] - i["strikes"]

            season_df = pd.concat([season_df, row_df], ignore_index=True)

        season_df.loc[season_df["pitching_IP"] > 0, "pitching_RA9"] = round(
            9 * (season_df["pitching_R"] / season_df["pitching_IP"]), 3
        )

        season_df.loc[season_df["pitching_IP"] > 0, "pitching_WHIP"] = round(
            (season_df["pitching_BB"] + season_df["pitching_H"])
            / season_df["pitching_IP"],
            3,
        )

        season_df.loc[season_df["pitching_IP"] > 0, "pitching_H/9"] = round(
            9 * (season_df["pitching_H"] / season_df["pitching_IP"]), 3
        )

        season_df.loc[season_df["pitching_IP"] > 0, "pitching_HR/9"] = round(
            9 * (season_df["pitching_HR"] / season_df["pitching_IP"]), 3
        )

        season_df.loc[season_df["pitching_IP"] > 0, "pitching_BB/9"] = round(
            9 * (season_df["pitching_BB"] / season_df["pitching_IP"]), 3
        )

        season_df.loc[season_df["pitching_IP"] > 0, "pitching_SO/9"] = round(
            9 * (season_df["pitching_SO"] / season_df["pitching_IP"]), 3
        )

        season_df.loc[season_df["pitching_AB"] > 0, "pitching_BABiP"] = round(
            (season_df["pitching_H"] - season_df["pitching_HR"])
            / (
                season_df["pitching_AB"]
                - season_df["pitching_SO"]
                - season_df["pitching_HR"]
                - season_df["pitching_SF"]
            ),
            3,
        )

        season_df.loc[season_df["pitching_BB"] > 0, "pitching_SO/BB"] = round(
            season_df["pitching_SO"] / season_df["pitching_BB"], 3
        )

        season_df.loc[season_df["pitching_AB"] > 0, "pitching_BA"] = round(
            season_df["pitching_H"] / season_df["pitching_AB"], 3
        )

        season_df.loc[season_df["pitching_BF"] > 0, "pitching_OBP"] = round(
            (
                season_df["pitching_H"]
                + season_df["pitching_BB"]
                + season_df["pitching_HBP"]
            )
            / (
                season_df["pitching_AB"]
                + season_df["pitching_BB"]
                + season_df["pitching_HBP"]
                + season_df["pitching_SF"]
            ),
            3,
        )

        season_df.loc[season_df["pitching_AB"] > 0, "pitching_SLG"] = round(
            (
                season_df["pitching_H"]
                + (season_df["pitching_2B"] * 2)
                + (season_df["pitching_3B"] * 3)
                + (season_df["pitching_HR"] * 4)
            )
            / season_df["pitching_AB"],
            3,
        )

        season_df.loc["pitching_OPS"] = round(
            season_df["pitching_OBP"] + season_df["pitching_SLG"], 3
        )

        season_df.loc["pitching_ISO"] = round(
            season_df["pitching_SLG"] - season_df["pitching_BA"], 3
        )

        season_df.loc[season_df["pitching_BF"] > 0, "pitching_SO%"] = round(
            season_df["pitching_SO"] / season_df["pitching_BF"], 3
        )

        season_df.loc[season_df["pitching_SO"] > 0, "pitching_BB/SO"] = round(
            season_df["pitching_BB"] / season_df["pitching_SO"], 3
        )

        season_df.loc[season_df["pitching_BF"] > 0, "pitching_PI/PA"] = round(
            season_df["pitching_PI"] / season_df["pitching_BF"], 3
        )

        season_df.loc[season_df["pitching_BF"] > 0, "pitching_HR/PA"] = round(
            season_df["pitching_HR"] / season_df["pitching_BF"], 3
        )

        season_df.loc[season_df["pitching_BF"] > 0, "pitching_BB/PA"] = round(
            season_df["pitching_BB"] / season_df["pitching_BF"], 3
        )

        season_df.loc[season_df["pitching_IP"] > 0, "pitching_PI/IP"] = round(
            season_df["pitching_PI"] / season_df["pitching_IP"], 3
        )

        season_df.replace([np.inf, -np.inf], None, inplace=True)
        season_df.dropna(subset=["season"], inplace=True)

    if save is True and stats_type == "batting":
        season_df.to_csv(
            f"season_stats/team/{season}_{level.lower()}" +
            "_season_batting_stats.csv",
            index=False,
        )
    elif save is True and stats_type == "pitching":
        season_df.to_csv(
            f"season_stats/team/{season}_{level.lower()}"
            + "_season_pitching_stats.csv",
            index=False,
        )

    return season_df


if __name__ == "__main__":

    print("Starting up.")
    now = datetime.now()

    # parser = argparse.ArgumentParser()
    # parser.add_argument(
    #     "--season",
    #     type=int,
    #     required=True,
    #     help="The season you want player season stats from.",
    # )

    # parser.add_argument(
    #     "--end_season",
    #     type=int,
    #     required=False,
    #     help="Optional argument. "
    #     + "If you want to download data from a range of seasons, "
    #     + "set `--end_season` to the final season you want stats from.",
    # )

    # parser.add_argument(
    #     "--level",
    #     type=str,
    #     required=True,
    #     help="The MiLB level you want player season stats from.",
    # )

    # parser.add_argument(
    #     "--stat_type",
    #     type=str,
    #     required=True,
    #     help="The type of stats you want to download. "
    #     + "Valid arguments are `batting` or `pitching`.",
    # )

    # args = parser.parse_args()

    # season = args.season
    # end_season = args.end_season
    # lg_level = args.level
    # stats_type = args.stat_type

    # if stats_type.lower() != "batting" and stats_type.lower() != "pitching":
    #     raise ValueError(
    #         '`--stats_type` must be set to "batting" or "pitching".'
    #     )

    # if end_season is not None:
    #     if season > end_season:
    #         raise ValueError(
    #             "`--season` cannot be greater than `--end_season`."
    #         )
    #     elif season == end_season:
    #         raise ValueError("`--end_season` cannot be equal to `--season`.")

    #     print(
    #         f"Parsing season player {stats_type} stats at the "
    #         + f"{lg_level.upper()} baseball level between the "
    #         + f"{season} and {end_season} seasons."
    #     )

    #     for s in tqdm(range(season, end_season + 1)):
    #         try:
    #             get_milb_team_season_stats(
    #                 season=s, level=lg_level, stats_type=stats_type, save=True
    #             )
    #         except Exception:
    #             print(
    #                 f"\nCould not get {lg_level.upper()} {stats_type} " +
    #                 f"stats for the {s} season."
    #             )
    # else:
    #     print(
    #         f"Parsing season player {stats_type} stats "
    #         + f"for the {season} {lg_level.upper()} baseball season."
    #     )

    #     get_milb_team_season_stats(
    #         season=season, level=lg_level, stats_type=stats_type, save=True
    #     )

    get_milb_team_season_stats(
        season=2024,
        level="aa",
        stats_type="pitching",
        save=True
    )
