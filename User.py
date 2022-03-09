import botometer
import pandas as pd


class User(object):
    def __init__(self, name: str, tweets, tot_tweets, botometer_manager: botometer.Botometer):
        self.username = name
        self.profile_image = tweets.iloc[0]['author.profile_image_url']
        self.id = tweets.iloc[0]['author.id']
        self.bom = botometer_manager
        self.created_at = tweets.iloc[0]['author.created_at']
        self.description = tweets.iloc[0]['author.description']
        self.tot_activity = tweets['text'].count() / float(tot_tweets) * 100
        self.rt = User.__rt_c(tweets)
        self.t = int(tot_tweets * self.tot_activity / 100 - self.rt)
        self.botscore = None

    @staticmethod
    def __rt_c(tweets):
        count = 0
        for row in tweets.iterrows():
            if type(row[1]['referenced_tweets']) == list and len(row[1]['referenced_tweets']) == 1:
                if not pd.isna(row[1]['referenced_tweets']):
                    if row[1]['referenced_tweets'][0]['type'] == 'retweeted':
                        count += 1
        return count

    def botometerscore(self):
        result = self.bom.check_account(self.id)
        self.botscore = result['cap']['universal']

    def to_dict(self):
        return {
            'username': self.username,
            'profile mage': self.profile_image,
            'twitter id': self.id,
            'created_at': self.created_at,
            'description': self.description,
            'activity share(%)': round(self.tot_activity, 2),
            'retweets': self.rt,
            'tweets': self.t,
            'botometer score': self.botscore
        }
