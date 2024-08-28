from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import json
import pandas as pd

# Set up Spark Context and Streaming Context
sc = SparkContext("local[2]", "YouTubeChatHeatmap")
ssc = StreamingContext(sc, 10)  # 10-second mini-batches

# Initialize a DataFrame to store user message counts
message_counts = pd.DataFrame(columns=["user", "message_count"])
messages = pd.DataFrame(columns=['id','user',"message","timestamp"])

# Define file path for storing data
file_path = "data/chat_message_counts.csv"

message_file_path = "data/messages.csv"

# Function to save data to CSV
def save_to_csv(df, path):
    df.to_csv(path, index=False)



from sklearn.feature_extraction.text import CountVectorizer
from sklearn.decomposition import LatentDirichletAllocation

ids = set()
# Process each RDD in the stream
def process_rdd(rdd):
    global message_counts
    global messages
    global ids
    if not rdd.isEmpty():
        # Convert RDD to DataFrame
        rdd_df = pd.DataFrame(rdd.collect())

        # Remove duplicate messages by checking the message ID
        rdd_df = rdd_df[~rdd_df['id'].isin(ids)]

        #add the ids to the set
        ids.update(rdd_df['id'])

        # Group by user and count the number of messages
        user_message_counts = rdd_df.groupby('user').size().reset_index(name='message_count')

        # Merge with the existing message counts
        message_counts = pd.concat([message_counts, user_message_counts], ignore_index=True)

        # Sum message counts if the user already exists in the DataFrame
        message_counts = message_counts.groupby('user').sum().reset_index()
        # Save the message counts to a CSV file
        save_to_csv(message_counts, file_path)

        messages = pd.concat([messages, rdd_df], ignore_index=True)
        # Apply topic modeling on the collected messages

        # Save the topics to a CSV file
        save_to_csv(messages, message_file_path)


# Apply the processing function to each RDD
lines = ssc.socketTextStream("localhost", 9999)
chat_data = lines.map(lambda line: json.loads(line))
chat_data.foreachRDD(process_rdd)

# Start Spark Streaming
ssc.start()
ssc.awaitTermination()
