import time
from datetime import datetime, timedelta
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


def get_data_for_date(current_date, data_file):
    pass


class WeatherDataIterator:
    def __init__(self, start_date: datetime, end_date: datetime, data_file: str):
        self.start_date = start_date
        self.end_date = end_date
        self.current_date = start_date
        self.data_file = data_file

    def __iter__(self):
        return self

    def __next__(self):
        # Проверяем, что текущая дата не больше конечной даты
        if self.current_date > self.end_date:
            raise StopIteration

        # Извлекаем данные для текущей даты
        data = get_data_for_date(self.current_date, self.data_file)

        # Увеличиваем текущую дату на один день
        self.current_date += timedelta(days=1)

        # Пропускаем даты, для которых нет данных
        while data is None and self.current_date <= self.end_date:
            data = get_data_for_date(self.current_date, self.data_file)
            self.current_date += timedelta(days=1)

        # Возвращаем кортеж с текущей датой и соответствующими данными
        return (self.current_date - timedelta(days=1), data)

start_date = datetime(2023, 1, 1)
end_date = datetime(2023, 12, 31)
data_file = 'weather_data.csv'

weather_data_iter = WeatherDataIterator(start_date, end_date, data_file)


def split_csv_by_weeks(input_file, num_files):
    # Читаем исходный csv файл
    with open(input_file, 'r') as file:
        reader = csv.reader(file)
        header = next(reader)  # Пропускаем заголовок

        # Создаем словарь для хранения данных по неделям
        weeks_data = {}

        # Обрабатываем каждую строку в csv файле
        for row in reader:
            date_str = row[0]  
            data = row[1:]  

            # Преобразуем строку с датой в объект datetime
            date = datetime.strptime(date_str, '%Y-%m-%d')

            # Определяем номер недели для данной даты
            week_number = date.isocalendar()[1]

            # Добавляем данные в соответствующую неделю в словаре
            if week_number in weeks_data:
                weeks_data[week_number].append((date, data))
            else:
                weeks_data[week_number] = [(date, data)]

    # Создаем отдельные файлы для каждой недели
    for week_number, data in weeks_data.items():
        start_date = data[0][0]
        end_date = data[-1][0]
        file_name = f"{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}.csv"

        with open(file_name, 'w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(header)  

            for date, row_data in data:
                writer.writerow([date.strftime('%Y-%m-%d')] + row_data)

        print(f"Файл {file_name} успешно создан.")


split_csv_by_weeks('dataset.csv', 6)


