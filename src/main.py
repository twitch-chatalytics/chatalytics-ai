from data.repository import TwitchMessagesRepository
from emotion.emotion_job import EmotionAnalysisJob
from utils.job_runner import JobRunner
from toxicity.toxicity_job import ToxicityAnalysisJob

if __name__ == "__main__":
    twitch_messages_repository = TwitchMessagesRepository()

    # Choose which job to run: 'emotion_analysis' or 'toxicity_analysis'
    chosen_job = 'emotion_analysis'

    if chosen_job == 'emotion_analysis':
        job = EmotionAnalysisJob(twitch_messages_repository)
        job_type = 'emotion_analysis'
    elif chosen_job == 'toxicity_analysis':
        job = ToxicityAnalysisJob(twitch_messages_repository)
        job_type = 'toxicity_analysis'
    else:
        raise ValueError(f"Unknown job type: {chosen_job}")

    job_runner = JobRunner(twitch_messages_repository, job_type)
    job_runner.run(job)
