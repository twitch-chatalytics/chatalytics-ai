from data.repository import Repository
from emotion.emotion_job import EmotionAnalysisJob
from gpt.gpt_job import GPTAnalysisJob
from utils.job_runner import JobRunner
from toxicity.toxicity_job import ToxicityAnalysisJob

if __name__ == "__main__":
    repository = Repository()

    # Choose which job to run: 'emotion_analysis', 'toxicity_analysis', 'gpt_analysis'
    chosen_job = 'gpt_analysis'

    if chosen_job == 'emotion_analysis':
        job = EmotionAnalysisJob(repository)
        job_type = 'emotion_analysis'
    elif chosen_job == 'toxicity_analysis':
        job = ToxicityAnalysisJob(repository)
        job_type = 'toxicity_analysis'
    elif chosen_job == 'gpt_analysis':
        job = GPTAnalysisJob(repository)
        job_type = 'gpt_analysis'

    else:
        raise ValueError(f"Unknown job type: {chosen_job}")

    job_runner = JobRunner(repository, job_type)
    job_runner.run(job)
