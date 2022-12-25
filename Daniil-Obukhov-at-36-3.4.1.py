import os
import csv
import pathlib
import re
from multiprocessing import Pool
from os import path
from typing import List, Dict
import concurrent.futures
import xml.etree.ElementTree as ET

import mnist
import numpy as np
import pandas as pd
import requests
from requests.adapters import HTTPAdapter, Retry


class ProcessValutes:
    def __init__(self, date, salary_currency):
        self.date = date
        self.salary_currency = salary_currency

    def get_currency_valute(self):
        if self.salary_currency == "RUR":
            return 1
        valutes = pd.read_csv("valutes.csv")
        valute = valutes.loc[valutes["date"] == self.date]
        if valute.__contains__(self.salary_currency):
            return float(valute[self.salary_currency])
        return 0


class Salary:
    """
    Класс для представления зарплат
    Attributes:
        salary_from (str): Нижняя граница оклада
        :type (str or int or float)
        salary_to: Верхняя граница оклада
        :type (str or int or float)
        salary_currency: Валюта оклада
        :type (str)
        published_at: Дата публикации
        :type (str)
    """
    def __init__(self, salary_from : str or int or float, salary_to : str or int or float, salary_currency : str, published_at : str):
        """
        @param salary_from: Нижняя граница оклада
        :type (str or int or float)
        @param salary_to: Верхняя граница оклада
        :type (str or int or float)
        @param salary_currency: Валюта оклада
        :type (str)
        @param published_at: Дата публикации
        :type (str)
        """
        self.salary_from = self.__check_void_value(salary_from)
        self.salary_to = self.__check_void_value(salary_to)
        self.salary_currency = salary_currency
        self.published_at = published_at
        self.month_year = f"{self.published_at[5:7]}/{self.published_at[:4]}"

    @staticmethod
    def __check_void_value(value: str or int or float) -> float:
        if type(value) == str and value == "":
            return 0
        return float(value)

    def get_average_salary(self):
        return round(((self.salary_from + self.salary_to) * ProcessValutes(self.month_year, self.salary_currency).get_currency_valute()) / 2, 4)


class Vacancy:
    """
    Класс для представления вакансий
    Attributes:
        name: Название
        :type (str)
        salary: Зарплата для данной вакансии
        :type (Salary)
        area_name: Местоположение
        :type (str)
        published_at: Дата публикации
        :type (str)
        year: Год публикации
        :type (str)
    """
    def __init__(self, vacancy: Dict[str, str]):
        """
        @param vacancy: Отдельная вакансия в виде словаря: атрибут - значение
        :type (Dict[str, str])
        >>> vac = {"name" :"Инженер", "salary_from" : 35000.0,"salary_to" : 45000.0, "salary_currency" : "RUR", "area_name" : "Moscow","published_at" :"2007-12-03T17:47:55+0300"}
        >>> vac = Vacancy(vac)
        >>> vac.area_name
        'Moscow'
        >>> vac.year
        '2007'
        """
        self.name = vacancy["name"]
        self.salary = Salary(salary_from=vacancy["salary_from"],
                             salary_to=vacancy["salary_to"],
                             salary_currency=vacancy["salary_currency"],
                             published_at=vacancy["published_at"])
        self.area_name = vacancy["area_name"]
        self.published_at = vacancy["published_at"]
        self.year = self.published_at[:4]

    def get_array_vacancy(self) -> List[str]:
        return [self.name, self.salary.get_average_salary(), self.area_name, self.published_at]


class SplitCsvFileByYear:
    """
    Класс для раделения набора вакансий по годам
    Attributes:
        file_name: Название файла
        :type (str)
        dir_name: Название папки, в которой хранятся итоговые csv-файлы
        :type (List[Vacancy])
        headlines: Названия загаловков
        :type (List[str])
        vacancies: Набор вакансий
        :type (List[List[str]])
    """
    def __init__(self, file_name : str, directory : str):
        """
        @param file_name: Название файла
        :type (str)
        @param file_name: Название папки, в которой хранятся итоговые csv-файлы
        :type (str)
        """
        self.file_name = file_name
        self.dir_name = directory
        self.headlines, self.vacancies = self.__csv_reader()
        self.__csv_process(self.headlines, self.vacancies)

    def __csv_reader(self) -> (List[str], List[List[str]]):
        """
        Читает из csv файла вакансии и возвращает в виде списка загаловков и набора вакансий
        @return: Список загаловков и набора вакансий
        :type (List[str], List[List[str]])
        """
        with open(self.file_name, encoding='utf-8-sig') as file:
            file_reader = csv.reader(file)
            lines = [row for row in file_reader]
        return lines[0], lines[1:]

    def __csv_process(self, headlines : List[str], vacancies : List[List[str]]) -> None:
        """
        Обрабатывает полученный набор вакансий и загаловков
        @param headlines: Названия загаловков
        :type (List[str])
        @param vacancies: Набор вакансий
        :type (List[List[str]])
        @return: None
        """
        cur_year = "0"
        self.first_vacancy = ""
        os.mkdir(self.dir_name)
        vacancies_cur_year = []
        for vacancy in vacancies:
            if (len(vacancy) == len(headlines)) and ((all([v != "" for v in vacancy])) or (vacancy[1] == "" and vacancy[2] != "") or (vacancy[1] != "" and vacancy[2] == "")):
                vacancy = [" ".join(re.sub("<.*?>", "", value).replace('\n', '; ').split()) for value in vacancy]
                if len(self.first_vacancy) == 0:
                    self.first_vacancy = vacancy
                vacancy_list = [v for v in vacancy]
                if vacancy[-1][:4] != cur_year:
                    if len(vacancies_cur_year) != 0:
                        self.__csv_writer(headlines, vacancies_cur_year, cur_year)
                        vacancies_cur_year.clear()
                    cur_year = vacancy[-1][:4]
                vacancies_cur_year.append(vacancy_list)
                self.last_vacancy = vacancy
        self.__csv_writer(headlines, vacancies_cur_year, cur_year)

    def __csv_writer(self, headlines : List[str], vacancies : List[List[str]], cur_year : str) -> None:
        """
        Записывает данные в csv-файл
        @param headlines: Названия загаловков
        :type (List[str])
        @param vacancies: Набор вакансий
        :type (List[List[str]])
        @param cur_year: Текущий год обработки
        :type (str)
        @return: None
        """
        name = path.splitext(self.file_name)
        vacancies = pd.DataFrame(vacancies, columns=headlines)
        vacancies.to_csv(f'{self.dir_name}/{name[0]}_{cur_year}.csv', index=False)


class DataSet:
    def __init__(self, file_name: str):
        """
        @param file_name: Название файла
        :type (str)
        """
        self.file_name = file_name
        self.vacancies_objects = self.__csv_reader()

    def __csv_reader(self) -> (List[Vacancy]):
        """
        Читает из csv файла вакансии и возвращает в виде списка вакансий
        @return: Список вакансий
        :type (List[Vacancy])
        """
        with open(self.file_name, encoding='utf-8-sig') as file:
            file_reader = csv.reader(file)
            lines = [row for row in file_reader]
        return self.__process_vacancies(lines[0], lines[1:])

    def __process_vacancies(self, headlines: List[str], vacancies: List[List[str]]) -> (List[Vacancy]):
        """
        Отбирает правильно заполненные вакансии и конвертирует в класс Vacancy
        :param headlines: Названия заголовков
        :type (List[str])
        :param vacancies: Список из списокв вакансий
        :type (List[List[str]])
        :return: Правильно заполненные вакансии
        :type (List[Vacancy])
        """
        result = []
        self.valutes = {}
        for vacancy in vacancies:
            vacancy = [" ".join(re.sub("<.*?>", "", value).replace('\n', '; ').split()) for value in vacancy]
            if self.valutes.__contains__(vacancy[3]):
                self.valutes[vacancy[3]] += 1
            else:
                self.valutes[vacancy[3]] = 1
            result.append(Vacancy({x: y for x, y in zip([r for r in headlines], [v for v in vacancy])}).get_array_vacancy())
        return result


class InputConnect:
    """
    Класс для обработки и иницилизации данных
    Attributes:
        input_data: Данные представленные пользователем (Запрос, имя файла, название нужной профессии)
        :type (List[str])
    """
    def __init__(self):
        """
        Иницилизация данных
        """
        input_data = []
        for question in ["Введите название csv-файла: ", "Введите название директории: ", "Введите название профессии: "]:
            print(question, end="")
            input_data.append(input())
        self.csv_file = input_data[0]
        self.directory = input_data[1]
        self.profession = input_data[2]


class ProcessData:
    def __init__(self, data: List[DataSet]):
        self.data = data

    def process_valutes(self):
        valutes = {}
        for el in self.data:
            for valute in el.valutes:
                if valutes.__contains__(valute):
                    valutes[valute] += el.valutes[valute]
                else:
                    valutes[valute] = el.valutes[valute]
        return valutes


class GetValutesValues:
    def __init__(self, valutes):
        self.valutes = valutes

    def get_valutes(self, date):
        session = requests.Session()
        retry = Retry(connect=3, backoff_factor=0.5)
        adapter = HTTPAdapter(max_retries=retry)
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        url = f"https://www.cbr.ru/scripts/XML_daily.asp?date_req=01/{date}d=1"
        res = session.get(url)
        cur_df = pd.read_xml(res.text)
        values = []
        for valute in self.valutes:
            if valute in cur_df["CharCode"].values:
                values.append(round(float(cur_df.loc[cur_df["CharCode"] == valute]["Value"].values[0].replace(',', ".")) / float(cur_df.loc[cur_df["CharCode"] == valute]["Nominal"]), 4))
            else:
                values.append(0)
        return [date] + values

    @staticmethod
    def get_date(first_date, second_date):
        res = []
        for year in range(int(first_date[:4]), int(second_date[:4]) + 1):
            num = 1
            if str(year) == first_date[:4]:
                num = int(first_date[-2:])
            for month in range(num, 13):
                if len(str(month)) == 2:
                    res.append(f"{month}/{year}")
                else:
                    res.append(f"0{month}/{year}")
                if str(year) == second_date[:4] and (str(month) == second_date[-2:] or f"0{month}" == second_date[-2:]):
                    break
        return res


if __name__ == '__main__':
    inp = InputConnect()
    spl = SplitCsvFileByYear(inp.csv_file, inp.directory)
    files = [str(file) for file in pathlib.Path(f"./{inp.directory}").iterdir()]
    with concurrent.futures.ProcessPoolExecutor() as executor:
        r = list(executor.map(DataSet, files))
    res = pd.concat([pd.DataFrame(el.vacancies_objects, columns=["name", "salary", "area_name", "published_at"]) for el in r])
    print(res)
    res.to_csv("vacancies.csv", index=False)


