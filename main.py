import json
import logging

import ijson
import click
import botometer

import numpy as np
import pandas as pd

from io import TextIOWrapper
from typing import Dict, List, Generator

from User import User

BOTOMETER_API_URL: str = "https://botometer-pro.p.mashape.com"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('')


def validate_config_file(config_data: Dict[str, str]) -> None:
    rapidapi_key_message: str = "No rapidapi_key found in the config file."
    twitter_app_auth: str = "No twitter_app_auth found in the config file."

    if 'rapidapi_key' not in config_data:
        logger.critical(rapidapi_key_message)
        raise Exception(rapidapi_key_message)

    if 'twitter_app_auth' not in config_data:
        logger.critical(twitter_app_auth)
        raise Exception("No twitter_app_auth found in the config file.")


def generate_config_file(outfile: TextIOWrapper) -> None:
    click.echo('''{
      "rapidapi_key": "YOUR_RAPID_API_KEY",
      "twitter_app_auth": {
            "consumer_key": "CONSUMER_KEY",
            "consumer_secret": "CONSUMER_SECRET",
            "access_token": "ACCESS_TOKEN",
            "access_token_secret": "ACCESS_TOKEN_SECRET"
        }
}''', file=outfile)


@click.command()
@click.option('--infile', required=False, type=click.File('rb'))
@click.option('--outfile', required=False, type=click.File('w'))
@click.option('--limit', required=False, default=40, type=click.INT)
@click.option('--config', required=False,  type=click.File('r'))
@click.option('--generate_config', required=False,  type=click.File('w'))
def main(infile: TextIOWrapper,
         outfile: TextIOWrapper,
         limit: int,
         config: TextIOWrapper,
         generate_config: TextIOWrapper) -> None:
    """
    Extract tweet ids from tweet JSON.
    """

    if generate_config is not None:
        logger.info("Generating config information file.")
        generate_config_file(generate_config)
        exit(0)

    config_data: Dict or None = None
    try:
        config_data= json.load(config)

    except json.decoder.JSONDecodeError:
        logger.critical("The config file is empty or it has an invalid value.")
        exit(1)

    validate_config_file(config_data)
    rapidapi_key: str = config_data['rapidapi_key']
    twitter_app_auth: Dict[str, str] = config_data['twitter_app_auth']

    botometer_manager: botometer.Botometer = botometer.Botometer(wait_on_ratelimit=True,
                                                                 rapidapi_key=rapidapi_key,
                                                                 **twitter_app_auth)

    json_file = ijson.items(infile, '', multiple_values=True)
    tweet_tuple: Generator = (o for o in json_file)
    results: List = [tweet for tweet in tweet_tuple]
    datadf: pd.DataFrame = pd.json_normalize(results)

    activity_ordered_users: pd.DataFrame = datadf.groupby('author.name')['id'] \
        .nunique().sort_values(ascending=False) \
        .reset_index(name='count')

    maus: List = []
    tot_tweets: int = datadf['id'].count()

    logger.info("Getting user information from Botometer")

    user_count: int = 0
    for row1 in activity_ordered_users.head(limit).iterrows():
        rslt_df: pd.DataFrame = datadf[datadf['author.name'] == row1[1]['author.name']]
        new_user: User = User(rslt_df.iloc[0]['author.username'], rslt_df, tot_tweets, botometer_manager)
        new_user.botometerscore()
        maus.append(new_user)
        user_count += 1
        if user_count % 2 == 0:
            logger.info("{}/{}".format(user_count, limit))

    mausdf: pd.DataFrame = pd.DataFrame.from_records([s.to_dict() for s in maus])

    mausdf['activity share(%)'].sum()

    mausdf.index = np.arange(1, len(mausdf) + 1)
    mausdf.to_csv(outfile)


if __name__ == '__main__':
    main()
