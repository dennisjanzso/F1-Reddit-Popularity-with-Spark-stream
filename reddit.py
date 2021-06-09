import praw
import credentials

class RedditScanner():
    def __init__(self, conn):
        self.conn = conn
        self.reddit = praw.Reddit(
            client_id=credentials.client_id,
            client_secret=credentials.client_secret,
            user_agent="testscript by f1-popularity-contest",
        )
        self.reddit.read_only = True

    def send_comment(self, comment):
        self.conn.send(comment.encode())

    def runScanner(self):
        print("Scanning reddit..")
        for comment in self.reddit.subreddit('formula1').stream.comments():
            text = 'none'
            try:
                cmt = str(comment.body_html).split('<p>')
                cmt = cmt[1].split('</p>')
                text = cmt[0]
            except:
                continue
            self.send_comment(text + '\n')