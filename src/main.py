from analytics.emotion import EmotionDataInserter, EmotionAnalyzer
from analytics.toxicity import ToxicityAnalyzer, ToxicityDataInserter
from data.analytics_repository import AnalyticsRepository
from data.twitch_messages_repository import TwitchMessagesRepository


def emotion():
    twitch_messages_repository = TwitchMessagesRepository()
    analytics_repository = AnalyticsRepository()

    emotion_analyzer = EmotionAnalyzer()
    data_inserter = EmotionDataInserter(analytics_repository)

    # Fetch the data
    df = twitch_messages_repository.fetch_twitch_messages()

    # Process messages through the model
    emotion_scores = emotion_analyzer.predict_emotions(df['message_text'].tolist())
    emotion_labels = ['sadness', 'joy', 'love', 'anger', 'fear', 'surprise']
    df['emotions'] = [emotion_labels[scores.argmax()] for scores in emotion_scores]

    # Aggregate emotion counts by user
    user_emotions = df.groupby('user')['emotions'].value_counts().unstack(fill_value=0)

    # Display the emotion counts per user
    print(user_emotions.head())

    insert_query = f"INSERT INTO analytics.emotion_report (user_id, sadness, joy, love, anger, fear, surprise) VALUES (%s, %s, %s, %s, %s, %s, %s)"
    data_inserter.insert_emotion_report(user_emotions, insert_query)


def toxicity():
    twitch_messages_repository = TwitchMessagesRepository()
    analytics_repository = AnalyticsRepository()

    toxicity_analyzer = ToxicityAnalyzer()
    data_inserter = ToxicityDataInserter(analytics_repository)

    # Fetch the data
    df = twitch_messages_repository.fetch_twitch_messages()

    # Analyze toxicity
    df['toxicity_score'] = toxicity_analyzer.predict_toxicity(df['message_text'].tolist(), batch_size=1000)

    # Compute user toxicity
    toxicity_threshold = 0.5
    user_toxicity = df.groupby('user').agg(
        average_toxicity=('toxicity_score', 'mean'),
        toxic_message_count=('toxicity_score', lambda x: (x > toxicity_threshold).sum())
    )

    most_toxic_users = user_toxicity.sort_values(by='average_toxicity', ascending=False)
    print(most_toxic_users.head())

    # Insert the results into the Cassandra table
    insert_query = "INSERT INTO analytics.toxicity_report (user_id, average_toxicity, toxic_message_count) VALUES (%s, %s, %s)"
    data_inserter.insert_toxicity_report(most_toxic_users, insert_query)


if __name__ == "__main__":
    emotion()
    # toxicity()
