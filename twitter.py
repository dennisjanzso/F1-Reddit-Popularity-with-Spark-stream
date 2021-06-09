import credentials
import requests
import time

class TwitterScanner():
    def __init__(self, conn):
        self.last_id = None
        self.conn = conn

    def get_tweets(self):
        if self.last_id == None:
            url = 'https://api.twitter.com/2/tweets/search/recent?max_results=10&query=f1 OR formula1'
        else:
            url = 'https://api.twitter.com/2/tweets/search/recent?since_id=' + self.last_id + '&max_results=10&query=f1 OR formula1'
        headers = {"Authorization": "Bearer "+credentials.twitter_bearer_token}
        response = requests.get(url, headers=headers).json()
        return response

    def send_comment(self, comment):
        self.conn.send(comment.encode())

    def runScanner(self):
        print("Scanning Twitter...")
        while True:
            self.collect_newest()
            time.sleep(10)

    def collect_newest(self):
        text = ""
        res = self.get_tweets()
        try:
            self.last_id = res['meta']['newest_id']
            if len(res['data']) > 0:
                for tweet in res['data']:
                    text += tweet['text']
        except:
            pass
        self.send_comment(text)


