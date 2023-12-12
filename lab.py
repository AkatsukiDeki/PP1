import time
from bs4 import BeautifulSoup
import requests
import csv


def get_url(year, month):


    url = f"https://www.gismeteo.ru/diary/4618/{year}/{month}/"
    return url


def get_data(url):
    headers = {
        "Accept": "*/*",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36 OPR/102.0.0.0 (Edition Yx GX)"
    }
    req = requests.get(url, headers=headers)
    src = req.text
    return src


def get_table_data(src, year, month):

    soup = BeautifulSoup(src, "lxml")
    try:
        table = soup.find("table").find("tbody").find_all("tr")
    except Exception:
        return []
    data_table = []
    for item in table:
        data_td = item.find_all('td')
        day = data_td[0].text
        temp_morning = data_td[1].text
        presure_morning = data_td[2].text
        wind_morning = data_td[5].text
        temp_evening = data_td[6].text
        wind_evening = data_td[10].text
        presure_evening = data_td[7].text
        data_table.append(
            {
                "day": str(year) + "-" + str(month) + "-" + day,
                "temp_morning": temp_morning,
                "presure_morning": presure_morning,
                "wind_morning": wind_morning,
                "temp_evening": temp_evening,
                "presure_evening": presure_evening,
                "wind_evening": wind_evening,

            }
        )
    return data_table


def write_to_csv(data_table):

    with open('dataset.csv', 'a', newline='', encoding='utf-8') as csvfile:
        for item in data_table:
            writer = csv.writer(csvfile, delimiter=",")
            writer.writerow(
                [item["day"], item["temp_morning"], item["presure_morning"], item["wind_morning"], item["temp_evening"],
                 item["presure_evening"], item["wind_evening"]])


for year in range(2007, 2024):
    for month in range(1, 13):
        url = get_url(year, month)
        src = get_data(url)
        data_table = get_table_data(src, year, month)
        write_to_csv(data_table)
