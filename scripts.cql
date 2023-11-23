CREATE TABLE IF NOT EXISTS analytics.emotion_report
(
    user_id  text PRIMARY KEY,
    sadness  int,
    joy      int,
    love     int,
    anger    int,
    fear     int,
    surprise int
);

select *
from analytics.toxicity_report
where toxic_message_count > 1 allow filtering;


CREATE KEYSPACE IF NOT EXISTS spark_output WITH replication = {
    'class': 'SimpleStrategy',
    'replication_factor': '1'
    };

CREATE TABLE IF NOT EXISTS spark_output.twitch_messages_
(
    streamer_id    text,
    message_id     text PRIMARY KEY,
    channel        text,
    message_text   text,
    timestamp      bigint,
    user           text,
    words          list<text>,
    filtered_words list<text>
);
