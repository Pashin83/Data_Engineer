from kafka import KafkaConsumer
import psycopg2
import json

consumer = KafkaConsumer(
    "user_events_data",
    bootstrap_servers="localhost:9092",
    group_id="user_logins_consumer",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

conn = psycopg2.connect(
    dbname="test_db", user="admin", password="admin", host="localhost", port=5432
)
cursor = conn.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS user_logins (
    id SERIAL PRIMARY KEY,
    username TEXT,
    event_type TEXT,
    event_time TIMESTAMP,
    sent_to_kafka BOOLEAN DEFAULT FALSE
)
""")
conn.commit()

for message in consumer:
    data = message.value
    print("Received:", data)

    cursor.execute(
        "INSERT INTO user_logins (username, event_type, event_time) VALUES (%s, %s, to_timestamp(%s))",
        (data["user"], data["event"], data["timestamp"])
    )
    conn.commit()