from src.parser import HH
from dotenv import load_dotenv
from src.dbmanager import DBManager
import os

url_area = 'https://api.hh.ru/areas'
url_vacancies = 'https://api.hh.ru/vacancies'
url_employers = 'https://api.hh.ru/employers'
url_professional_roles = 'https://api.hh.ru/professional_roles'

load_dotenv()
db_config = {
    'user': os.getenv('POSTGRES_USER'),
    'password': os.getenv('POSTGRES_PASSWORD'),
    'host': os.getenv('POSTGRES_HOST'),
    'port': os.getenv('POSTGRES_PORT'),
    'dbname': os.getenv('POSTGRES_DB')
}


def main():
    db = DBManager(db_config)
    end = False  # флаг выхода из программы
    while not end:  # пока не конец программы
        print("Получаем список работодателей, отсортированных по количеству открытых вакансий")
        params = {"only_with_vacancies": True, "sort_by": "by_vacancies_open", "page": 0, "per_page": 100}
        employers = HH(url_employers, params)
        print(employers.load_data_via_api(1))
        print("Получаем топ 10 работодателей по количеству открытых вакансий")
        employers_lst = employers.data_lst[:10]
        #вносим полученные данные в таблицу
        db.insert_data('employers', employers_lst)

        print("Для каждого из 10 работодателей получаем список вакансий (ограничение API - максимум 2000 вакансий")
        for emplr in employers_lst:
            params = {"page": 0, "per_page": 100}
            employer_vacancies = HH(emplr['vacancies_url'], params)
            print("Для работодателя ", emplr["name"])
            print(employer_vacancies.load_data_via_api(1))
            employer_vacancies_lst = employer_vacancies.data_lst
            # вносим полученные данные в таблицу
            print(f"Вносим в таблицу {len(employer_vacancies_lst)} вакансий")
            db.insert_data('vacancies', employer_vacancies_lst)

        print("--------get_companies_and_vacancies_count--------")
        data_lst = db.get_companies_and_vacancies_count()
        [print(item, '\n') for item in data_lst]
        print("--------get_all_vacancies--------")
        data_lst = db.get_all_vacancies()
        print(len(data_lst))
        [print(item, '\n') for item in data_lst]
        print("--------get_avg_salary--------")
        data_lst = db.get_avg_salary()
        [print(item, '\n') for item in data_lst]
        print("--------get_vacancies_with_higher_salary--------")
        data_lst = db.get_vacancies_with_higher_salary()
        [print(item, '\n') for item in data_lst]
        print("--------get_vacancies_with_keyword--------")
        data_lst = db.get_vacancies_with_keyword('продавец')
        [print(item, '\n') for item in data_lst]

        end = True
    db.close_conn()


if __name__ == '__main__':
    main()
