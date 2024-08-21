import psycopg2
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

class PostgresDB:
    def __init__(self, dbname, user_name, password, host, table_name):
        self.dbname = dbname
        self.user_name = user_name
        self.password = password
        self.host = host
        self.table = table_name
        self.conn = None
        self.cursor = None
        self.connect_to_db()

    def connect_to_db(self):
        try:
            self.conn = psycopg2.connect(
                dbname=self.dbname,
                user=self.user_name,
                password=self.password,
                host=self.host
            )
            self.cursor = self.conn.cursor()
            logger.info(f"Connection to {self.dbname} database established...")
        except Exception as err:
            logger.error(f"Error connecting to database: {err}")
            raise

    def create_table(self):
        if self.table_exists():
            # logger.info("Table Exists..")
            return
        try:
            self.cursor.execute(f'''
                CREATE TABLE IF NOT EXISTS {self.table} (
                    date TEXT,
                    title TEXT,
                    link TEXT,
                    description TEXT,
                    news TEXT,
                    summary TEXT,
                    sentiment TEXT,
                    confidence REAL
                )
            ''')
            logger.info("Table doesn't exist... created successfully...")
        except Exception as err:
            logger.error(f"Error occurred: {err}")
            raise

    def insert_news(self, news_items):
        self.create_table()
        try:
            self.cursor.execute("""INSERT INTO news_analysis (date, title, link, description, news, summary, sentiment, confidence) VALUES
                                (%s, %s, %s, %s, %s, %s, %s, %s)""",
                                (news_items["Date"], news_items["Title"], news_items["Link"], news_items["Short Description"], 
                                 news_items["News"], news_items["News Summary"], news_items["Sentiment"], news_items["Sentiment Confidence"]))
            # logger.info("Data Inserted Successfully......")
            self.conn.commit()
        except Exception as err:
            logger.error(f"Error inserting data: {err}")
            self.conn.rollback()
            raise

    def table_exists(self):
        try:
            self.cursor.execute("""
                SELECT EXISTS (
                    SELECT 1 
                    FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = %s
                );
            """, (self.table,))
            exists = self.cursor.fetchone()[0]
            return exists
        except Exception as error:
            logger.error(f"Error checking table existence: {error}")
            raise

    def query_data(self):
        try:
            self.cursor.execute(f"SELECT * FROM {self.table}")
            rows = self.cursor.fetchall()
            return rows
        except Exception as error:
            logger.error(f"Error querying data: {error}")
            raise

    def close_connection(self):
        try:
            if self.cursor:
                self.cursor.close()
            if self.conn:
                self.conn.close()
            logger.info("Connection closed successfully...")
        except Exception as err:
            logger.error(f"Error closing connection: {err}")
            raise

    def __del__(self):
        self.close_connection()

if __name__ == "__main__":
    # Example usage
    db = PostgresDB(dbname="mydb", user_name="rahul", password="password", host="localhost", table_name="news_analysis")
    print(f"Table exists? : {db.table_exists()}")
    # db.create_table()
    # print(f"Table exists? : {db.table_exists()}")
    rows = db.query_data()
    for i in rows:
        print(i)
        print("-"*88)
