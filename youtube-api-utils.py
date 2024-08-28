import googleapiclient.discovery
import time
import os
import json
import socket
#Get variable from .env file
from dotenv import load_dotenv
load_dotenv()
api_key = os.environ.get('API_KEY')

youtube = googleapiclient.discovery.build('youtube', 'v3', developerKey=api_key)


#get LINK as input
LINK = input("Enter the link of the live stream: ")

video_id = LINK.split('=')[1]

def get_live_chat_id():
    response = youtube.videos().list(
        part='liveStreamingDetails',
        id=video_id
    ).execute()
    return response['items'][0]['liveStreamingDetails']['activeLiveChatId']

live_chat_id = get_live_chat_id()

def fetch_chat_messages():
    response = youtube.liveChatMessages().list(
        liveChatId=live_chat_id,
        part='snippet,authorDetails',
        maxResults=200
    ).execute()
    return response['items']

# Socket setup to send data to Spark
server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server_socket.bind(('localhost', 9999))
server_socket.listen(1)
print("Waiting for a connection from Spark Streaming...")
client_socket, addr = server_socket.accept()
print(f"Connected to {addr}")

# Continuously fetch and send data to Spark
try:
    while True:
        chat_messages = fetch_chat_messages()
        for item in chat_messages:
            message_data = {
                'id': item['id'],
                'user': item['authorDetails']['displayName'],
                'message': item['snippet']['textMessageDetails']['messageText'] if 'textMessageDetails' in item['snippet'] else '',
                'timestamp': item['snippet']['publishedAt']
            }
            print(message_data)
            client_socket.send((json.dumps(message_data) + "\n").encode('utf-8'))

        time.sleep(10)  # Fetch new messages every 5 seconds
except KeyboardInterrupt:
    print("Stopping data stream...")
finally:
    client_socket.close()
    server_socket.close()
