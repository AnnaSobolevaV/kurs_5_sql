from abc import ABC, abstractmethod
from src.my_exeption import RequestErrorException
import requests


class Parser(ABC):
    """
    Абстрактный класс для работы с API различных сайтов по поиску вакансий
    """

    def __init__(self, url: str, params: dict):
        self.params = params
        self.url = url

    @abstractmethod
    def load_data_via_api(self, params_for_load_data: dict):
        pass


class HH(Parser):
    """
    Класс для работы с API HeadHunter:
        Атрибуты:
            __data_lst: list   - список данных, полученных в ответах на запросы;
        Методы:
            load_data_via_api(self, keywords): Метод получает список данных, по запросу к API,
                                            используя указанное максимальное количество страниц.
                                            По умолчанию 20 страниц;

    """
    __data_lst: list[dict]

    def __init__(self, url: str, params: dict):
        self.__data_lst = []
        super().__init__(url, params)

    def __repr__(self):
        return f"<{self.__class__}, {self.__dict__}>"

    def __str__(self):
        return f"<{self.__data_lst}>"

    def __len__(self):
        return len(self.__data_lst)

    @property
    def data_lst(self) -> list[dict]:
        data_lst = []
        for item in self.__data_lst:
            data_lst.append(item)
        return data_lst

    def load_data_via_api(self, pages_max=20) -> str:
        """
        Метод получает список данных, по запросу к API, используя указанное максимальное количество страниц.
        По умолчанию 20 страниц. Добавляет данные в self.__data_lst
        Возвращает строку: 'По запросу найдено {found} элементов'
        """
        found = 'Что-то пошло не так!'
        pages = 1
        while self.params['page'] < pages:
            if self.params['page'] >= pages_max:
                break
            try:
                response = requests.get(self.url, self.params)
            except requests.exceptions.ConnectionError as e:
                print("ConnectionError ", e)
            else:
                if response.status_code == 200:
                    found = response.json()['found']
                    page = response.json()['page']
                    pages = response.json()['pages']
                    items = response.json()['items']
                    self.__data_lst.extend(items)
                    self.params['page'] = page + 1
                else:
                    raise RequestErrorException(f"**{response.status_code}, **{response.json()}")
        return f"По запросу найдено {found} элементов"
