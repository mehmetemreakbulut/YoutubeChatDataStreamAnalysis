from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import pandas as pd

import json

sc = SparkContext("local[2]", "ChatHeatmap")
ssc = StreamingContext(sc, 10)  # 10-second mini-batches

# Connect to the socket stream
lines = ssc.socketTextStream("localhost", 9999)

# Parse the incoming JSON data
def parse_json(line):
    return json.loads(line)

# Apply the parsing function to the stream
chat_data = lines.map(parse_json)

def process_rdd(rdd):
    if not rdd.isEmpty():
        df = rdd.collect()

        # Example 1: Count messages per user
        user_message_counts = {}
        for record in df:
            user = record['user']
            user_message_counts[user] = user_message_counts.get(user, 0) + 1

        print("Message Count per User in this Batch:")
        for user, count in user_message_counts.items():
            print(f"{user}: {count} messages")

# Process each RDD in the stream
chat_data.foreachRDD(process_rdd)

# Start streaming
ssc.start()
ssc.awaitTermination()


# To test this, you can use netcat to send messages to the socket
# $ nc -lk 9999
