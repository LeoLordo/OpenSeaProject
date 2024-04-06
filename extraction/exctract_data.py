import requests


class Extract:
   def __init__(self, api_key: str, url: str):
        self.api_key = api_key
        self.url = url

   def extract_collections(self) -> list:
       original_url = self.url
       headers = {
           "accept": "application/json",
           "x-api-key": self.api_key
       }
       response = requests.get(original_url, headers=headers)
       next_token = response.json()['next']
       data_list = []
       # count = 0
       while next_token:
           next_token = response.json()["next"]
           url = original_url + "?next=" + next_token
           response = requests.get(url, headers=headers)
           data_list.append(response.json())
       return data_list
