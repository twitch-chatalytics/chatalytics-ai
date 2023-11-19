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
where toxic_message_count > 1 allow filtering