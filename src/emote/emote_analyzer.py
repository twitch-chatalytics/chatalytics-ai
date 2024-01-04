import requests


class EmoteAnalyzer:

    def __init__(self):
        pass

    def get_channel_emotes(self, channel_login):
        headers = {
            'Client-ID': 'j9i0u85lh015x1b77yofg57lxx2mph',
            'Authorization': f'Bearer 0dlqt7mkf053sccsh4cxqwoxb0i8zq'
        }
        url = f'https://api.twitch.tv/helix/chat/emotes?broadcaster_id={channel_login}'

        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            return response.json()
        else:
            raise ValueError(f'Error: {response.status_code}')

    def get_twitch_default_emotes(self):
        headers = {
            'Client-ID': 'j9i0u85lh015x1b77yofg57lxx2mph',
            'Authorization': f'Bearer 0dlqt7mkf053sccsh4cxqwoxb0i8zq'
        }
        url = 'https://api.twitch.tv/helix/chat/emotes/global'

        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            return response.json()
        else:
            raise ValueError(f'Error: {response.status_code}')
