import random
import sys
import time
from datetime import datetime
from json import dumps

from cassandra.cluster import Cluster
from kafka import KafkaProducer
import mysql.connector

CASSANDRA_HOST = 'localhost'
CASSANDRA_KEYSPACE = 'trading'
CASSANDRA_TABLE = 'real_time_data'

MYSQL_HOST = 'localhost'
MYSQL_PORT = 3307
MYSQL_DATABASE = 'trading'
MYSQL_TABLE = 'profile'
MYSQL_USERNAME = 'root'
MYSQL_PASSWORD = 'root'

KAFKA_BOOTSTRAP_SERVER = 'localhost:29092'

def get_last_id():
    cluster = Cluster([CASSANDRA_HOST])
    session = cluster.connect(CASSANDRA_KEYSPACE)
    query = f"SELECT MAX(id) AS last_id FROM {CASSANDRA_TABLE}"
    result = session.execute(query)
    last_id = result.one().last_id
    return last_id if last_id else 0

def get_tickers():
    config = {
            'user': MYSQL_USERNAME,
            'password': MYSQL_PASSWORD,
            'host': MYSQL_HOST,
            'port': MYSQL_PORT,
            'database': MYSQL_DATABASE,
            'raise_on_warnings': True
            }

    cnx = mysql.connector.connect(**config)

    with cnx.cursor(dictionary=True) as c:
        result = c.execute(f"SELECT * FROM {MYSQL_TABLE}")

        rows = c.fetchall()

    cnx.close()

    return rows

def produce_message(id, ticker, shares, sector, min_price=500, max_price=600):
    message = {}
    price = random.randint(min_price, max_price)
    message["id"] = id
    message["ticker"] = ticker
    message["created_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    message['price'] = price
    message['market_cap'] = price * shares
    message['volume'] = random.randint(10e4, 10e5)
    message['sector'] = sector
    return message

def main():
    tickers = get_tickers()
    KAFKA_TOPIC = sys.argv[1]
    producer = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVER,
                            value_serializer=lambda x: dumps(x).encode('utf-8'))
    last_id = get_last_id()
    print("Kafka producer application started.")

    try:
        while True:
            id = last_id + 1
            message = []
            for ticker in tickers:
                id = last_id + 1
                message.append(produce_message(id, ticker['ticker'], ticker['shares'], ticker['sector']))
                last_id = id  
            print(f"Produced message: {message}")
            producer.send(KAFKA_TOPIC, message)
            time.sleep(5)  
    except KeyboardInterrupt:
        producer.flush()
        producer.close()
        print("Kafka producer application completed.")

if __name__ == "__main__":
    main()
