import streamlit as st
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import time
import altair as alt

# Function to load and process data
def load_data():
    try:
        df = pd.read_csv('data/chat_message_counts.csv')
        return df
    except FileNotFoundError:
        return pd.DataFrame(columns=["user", "message_count"])

# Function to display a bar chart of message counts
def display_message_counts(df):
    if df.empty:
        st.write("No data available.")
        return

    # Sort users by message count
    df = df.sort_values(by='message_count', ascending=False)
    # Display the top 10 users
    df = df.head(10)

    # Create a bar chart
    # Set the color palette
    print(df)
    st.write("Top 10 Users by Message Count")
    st.write(alt.Chart(df).mark_bar().encode(
        x=alt.X('user', sort=None),
        y='message_count',
    ).properties(
        width=600,
        height=400
    ))






# Streamlit UI
st.title("YouTube Live Chat Message Counts")

# Auto-refresh mechanism
REFRESH_INTERVAL = 10  # Time in seconds

placeholder = st.empty()

while True:
    df = load_data()
    with placeholder.container():
        display_message_counts(df)
    print("Refreshing...")
    time.sleep(REFRESH_INTERVAL)
