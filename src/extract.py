import os
import requests
import time
from bs4 import BeautifulSoup as bs


def get_opensky_urls():
    csv_file_urls = []
    pageUrl = "https://zenodo.org/record/4485741#.YDn7jdyIby2"
    page = requests.get(pageUrl)
    soup = bs(page.content, 'html.parser')
    files_link = soup.find_all("a", attrs={"class": "filename"})
    for link in files_link:
        csv_href = "https://zenodo.org{0}".format(link.get('href').split('?')[0])
        print('Found href:', csv_href)
        csv_file_urls.append(csv_href)
    return csv_file_urls

