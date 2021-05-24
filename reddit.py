import praw
import socket
import credentials

TCP_IP = "localhost"
TCP_PORT = 9009
conn = None
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind((TCP_IP, TCP_PORT))
s.listen(1)
print("Waiting for TCP connection...")
conn, addr = s.accept()
print("Connected... Starting getting comments.")

reddit = praw.Reddit(
    client_id=credentials.client_id,
    client_secret=credentials.client_secret,
    user_agent="testscript by f1-popularity-contest",
)
reddit.read_only = True

def send_comment(comment, conn):
    conn.send(comment.encode())

print('Reddit connected')
for comment in reddit.subreddit('formula1').stream.comments():
    text = 'none'
    try:
        cmt = str(comment.body_html).split('<p>')
        cmt = cmt[1].split('</p>')
        text = cmt[0]
    except:
        continue
    send_comment(text + '\n', conn)