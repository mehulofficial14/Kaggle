import psycopg2
from dotenv import load_dotenv
from pathlib import Path
import os

class Connection:
    def __init__(self):
        dotenv_path = Path('./connection.env')
        load_dotenv(dotenv_path=dotenv_path)
        try:
            self.connection = psycopg2.connect(
                dbname = os.getenv("dbname"),
                user = os.getenv("user"),
                password = os.getenv("password"),
                host = os.getenv("host"),
                port = os.getenv("port")
            )
            print("Connected to database successfully.")
        except psycopg2.Error as e:
            print("Error connecting to database:", e)
        self.cursor = None
    def start(self):
        self.cursor = self.connection.cursor()
        print('\n\n########Cursor has started########\n')
    def run(self, q):
        print('########Executing Query########\n')
        print(q)
        self.cursor.execute(q)      
        print('########Finished Execution Query########\n')
    def clear_res(self):
        print('########Clearing Resources########\n')
        self.connection.commit()
        print('########Cleared All########\n')
    def stop(self):
        self.cursor.close()
        print('########Cursor has stopped########\n')
        self.connection.close()
        print('########Connection has stopped########\n\n')