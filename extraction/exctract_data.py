import requests
import json


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
       count = 0
       while count != 2:
           next_token = response.json()["next"]
           url = original_url + "?next=" + next_token
           response = requests.get(url, headers=headers)
           count += 1
           data_list.append(response.json())
       return data_list


# data_list = Extract(api_key="6afc226105fd474b85998fe4ea531ac9", url="https://api.opensea.io/api/v2/collections").extract_collections()
# print(data_list[0])
# print(data_list[1])