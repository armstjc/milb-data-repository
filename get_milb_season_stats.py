import json
import os
import time
from datetime import datetime
from urllib.request import urlopen

import pandas as pd
from tqdm import tqdm


def get_milb_season_stats(season: int, level: str):
    """
    """
