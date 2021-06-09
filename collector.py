from reddit import RedditScanner
import socket
from twitter import TwitterScanner
from threading import Thread


class Collector():

    def __init__(self):
        self.TCP_IP = "localhost"
        self.TCP_PORT = 9009
        self.conn = None
        self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.s.bind((self.TCP_IP, self.TCP_PORT))
        self.s.listen(1)
        print("Waiting for TCP connection...")
        self.conn, addr = self.s.accept()
        print("Connected... Starting Stream")
        self.TS = TwitterScanner(self.conn)
        self.RS = RedditScanner(self.conn)
        self.runListeners()

    def runListeners(self):
        t1 = Thread(target = self.TS.runScanner())
        t2 = Thread(target = self.RS.runScanner())

        t1.start()
        t2.start()

C = Collector()