import psycopg2


class DBManager:
    """
    Класс DBManager
    подключается к БД PostgresSQL,
    имеет следующие методы:

    create_database(params: dict)
    - создание базы данных и необходимых таблиц по переданным параметрам

    insert_data(table_name, data: list[dict])
    - добавление данных в указанную таблицу

    get_companies_and_vacancies_count()
     — получение списка всех компаний и количество вакансий у каждой компании.

    get_all_vacancies()
     — получение списка всех вакансий с указанием названия компании,
    названия вакансии и зарплаты и ссылки на вакансию.

    get_avg_salary()
     — получение средней зарплаты по вакансиям. (В разрезе currency, gross).

    get_vacancies_with_higher_salary()
     — получение списка всех вакансий, у которых зарплата выше средней по всем вакансиям. (В разрезе currency, gross).


    get_vacancies_with_keyword()
     — получение списка всех вакансий, в названии которых содержатся переданные
    в метод слова, например python.

    close_conn()
    - завершение коннекции


    """

    def __init__(self, params: dict):
        try:
            self.conn = psycopg2.connect(dbname=params['dbname'], user=params['user'], host=params['host'],
                                         password=params['password'], port=params['port'])
        except psycopg2.OperationalError:
            self.create_database(params)

    def close_conn(self):
        self.conn.close()

    def create_database(self, params: dict):
        """Создание базы данных и таблиц для сохранения данных."""
        print("Создание базы данных и таблиц для сохранения данных.")
        self.conn = psycopg2.connect(dbname='postgres', user=params['user'], host=params['host'],
                                     password=params['password'], port=params['port'])
        self.conn.autocommit = True
        cur = self.conn.cursor()

        cur.execute(f"CREATE DATABASE {params['dbname']}")

        self.conn.close()

        self.conn = psycopg2.connect(dbname=params['dbname'], user=params['user'], host=params['host'],
                                     password=params['password'], port=params['port'])

        with self.conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE employers (
                    employer_id SERIAL PRIMARY KEY,
                    employer_hh_id INT UNIQUE,
                    alternate_url VARCHAR,
                    name VARCHAR(50) NOT NULL,
                    url VARCHAR(50),
                    vacancies_url VARCHAR,
                    open_vacancies INTEGER
                )
            """)

        with self.conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE vacancies (
                    vacancies_id SERIAL PRIMARY KEY,
                    vacancy_hh_id INT,
                    employer_id INT REFERENCES employers(employer_id),
                    name VARCHAR NOT NULL,
                    area VARCHAR,
                    salary_from INT,
                    salary_to INT,
                    currency VARCHAR,
                    gross bool,
                    type VARCHAR,
                    address VARCHAR,
                    published_at DATE,
                    created_at DATE,
                    url VARCHAR,
                    alternate_url VARCHAR,
                    snippet_requirement TEXT,
                    snippet_responsibility TEXT,
                    schedule VARCHAR,
                    professional_roles VARCHAR,
                    experience VARCHAR,
                    employment VARCHAR
                )
            """)

        self.conn.commit()

    def insert_data(self, table_name: str, data: list[dict]):
        #Вставляем данные в таблицу vacancies
        if table_name == 'vacancies':
            for item in data:
                count = 0
                with self.conn.cursor() as cur:
                    #находим id работодателя в таблице employers
                    cur.execute(f"SELECT employer_id FROM employers "
                                f"WHERE employers.employer_hh_id = {item['employer']['id']}")
                    employer_id = cur.fetchall()

                    #если данные не были переданы в запросе, объявляем их None
                    if item["address"]:
                        address = item["address"]["raw"]
                    else:
                        address = None
                    if item["salary"]:
                        if item["salary"]["from"]:
                            salary_from = item["salary"]["from"]
                        else:
                            salary_from = None
                        if item["salary"]["to"]:
                            salary_to = item["salary"]["to"]
                        else:
                            salary_to = None
                        currency = item["salary"]["currency"]
                        if item["salary"]["gross"]:
                            gross = item["salary"]["gross"]
                        else:
                            gross = None

                    else:
                        salary_from = None
                        salary_to = None
                        currency = None
                        gross = None

                    #запрос для проверки наличия дубля в таблице
                    try:
                        sql_str = (f"SELECT COUNT(*) FROM {table_name} WHERE vacancy_hh_id = {item['id']} AND "
                                   f"employer_id = {employer_id[0][0]} AND {table_name}.name = '{item['name']}' AND "
                                   f"area = '{item['area']['name']}' AND type = '{item['type']['name']}' AND "
                                   f"published_at = '{item['published_at']}' AND "
                                   f"created_at = '{item['created_at']}' AND "
                                   f"url = '{item['url']}' AND alternate_url = '{item['alternate_url']}' AND "
                                   f"snippet_requirement = '{item['snippet']['requirement']}' AND "
                                   f"snippet_responsibility = '{item['snippet']['responsibility']}' AND "
                                   f"schedule = '{item['schedule']['name']}' AND "
                                   f"professional_roles = '{item['professional_roles'][0]['name']}' AND "
                                   f"experience = '{item['experience']['name']}' AND "
                                   f"employment = '{item['employment']['name']}'")
                        # print(sql_str)
                        cur.execute(sql_str)
                    except Exception as e:
                        print("Пыталась проверять наличие дубля. ", e)
                        self.conn.rollback()
                    else:
                        count = cur.fetchall()[0][0]

                    #если дубль найден, то не вставляем данные в таблицу и переходим к следующей записи
                    if count > 0:
                        break
                    # если дубль не найден, то вставляем данные в таблицу
                    else:
                        try:
                            cur.execute(
                                f"INSERT INTO {table_name} "
                                f"(vacancy_hh_id, employer_id, "
                                f"name, area, "
                                f"salary_from, salary_to, currency, gross, type, address, "
                                f"published_at, created_at, url, alternate_url, "
                                f"snippet_requirement, snippet_responsibility, "
                                f"schedule, "
                                f"professional_roles,"
                                f"experience, employment) "
                                f"VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, "
                                f"%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                                (item["id"], employer_id[0][0],
                                 item["name"], item["area"]["name"],
                                 salary_from, salary_to,
                                 currency, gross,
                                 item["type"]["name"], address,
                                 item["published_at"], item["created_at"], item["url"],
                                 item["alternate_url"], item["snippet"]["requirement"],
                                 item["snippet"]["responsibility"],
                                 item["schedule"]["name"], item["professional_roles"][0]["name"],
                                 item["experience"]["name"], item["employment"]["name"]))
                        except psycopg2.errors.UniqueViolation as ex:
                            print("Duplicate: ", ex)
                            self.conn.rollback()
                        except psycopg2.errors.InFailedSqlTransaction as ex1:
                            print("error: ", ex1)
                            self.conn.rollback()

        # Вставляем данные в таблицу employers
        if table_name == 'employers':
            for item in data:
                with self.conn.cursor() as cur:
                    try:
                        cur.execute(
                            f"INSERT INTO {table_name} "
                            f"(employer_hh_id, alternate_url, name, url, "
                            f"vacancies_url, open_vacancies) "
                            f"VALUES (%s, %s, %s, %s, %s, %s)",
                            (item["id"], item["alternate_url"], item["name"], item["url"],
                             item["vacancies_url"], item["open_vacancies"]))
                    except psycopg2.errors.UniqueViolation as ex:
                        print("Duplicate: ", ex)
                        self.conn.rollback()
                    except psycopg2.errors.InFailedSqlTransaction as ex1:
                        print("error: ", ex1)
                        self.conn.rollback()

        self.conn.commit()

    def get_companies_and_vacancies_count(self) -> list[dict]:
        with self.conn.cursor() as cur:
            cur.execute(f"SELECT employers.name, COUNT(*) FROM employers "
                        f"JOIN vacancies USING (employer_id) GROUP BY employer_id "
                        f"ORDER BY COUNT(*) DESC")
            data = cur.fetchall()
            data_dict = [{"employers_name": d[0], "total_vacancies_in_db": d[1]} for d in data]
        return data_dict

    def get_all_vacancies(self) -> list[dict]:
        with self.conn.cursor() as cur:
            cur.execute(f"SELECT vacancies.name, employers.name, salary_from, salary_to,"
                        f" currency, gross, vacancies.alternate_url FROM vacancies "
                        f"JOIN employers USING(employer_id)")
            data = cur.fetchall()
            data_dict = [{"vacancies_name": d[0], "employers_name": d[1], "salary_from": d[2],
                          "salary_to": d[3], "currency": d[4], "gross": d[5], "vacancies_alternate_url": d[6]}
                         for d in data]
        return data_dict

    def get_avg_salary(self) -> list[dict]:
        with self.conn.cursor() as cur:
            cur.execute(f"SELECT AVG(salary_from), AVG(salary_to), currency, gross FROM vacancies "
                        f"WHERE salary_from IS NOT NULL OR salary_to IS NOT NULL "
                        f"GROUP BY currency, gross")
            data = cur.fetchall()
            data_dict = [{"AVG(salary_from)": d[0], "AVG(salary_to)": d[1], "currency": d[2],
                          "gross": d[3]} for d in data]
        return data_dict

    def get_vacancies_with_higher_salary(self) -> list[dict]:
        data_dict = []
        with self.conn.cursor() as cur:
            cur.execute(f"SELECT AVG(salary_from), AVG(salary_to), currency, gross FROM vacancies "
                        f"WHERE salary_from IS NOT NULL OR salary_to IS NOT NULL "
                        f"GROUP BY currency, gross")
            data = cur.fetchall()
            data_dict_1 = [{"AVG(salary_from)": d[0], "AVG(salary_to)": d[1], "currency": d[2],
                            "gross": d[3]} for d in data]
        for item in data_dict_1:
            if not item["gross"]:
                item["gross"] = 'null'
            with self.conn.cursor() as cur:
                cur.execute(f"SELECT vacancies_id, name, salary_from, salary_to, currency, gross FROM vacancies "
                            f"WHERE (salary_from >= (SELECT AVG(salary_from) FROM vacancies "
                            f"WHERE currency = '{item["currency"]}' AND gross = {item["gross"]} "
                            f"GROUP BY currency, gross) "
                            f"OR (SELECT AVG(salary_from) FROM vacancies "
                            f"WHERE currency = '{item["currency"]}' AND gross = {item["gross"]} "
                            f"GROUP BY currency, gross) IS NULL) "
                            f"AND (salary_to >= (SELECT AVG(salary_to) FROM vacancies "
                            f"WHERE currency = '{item["currency"]}' AND gross = {item["gross"]} "
                            f"GROUP BY currency, gross) "
                            f"OR (SELECT AVG(salary_to) FROM vacancies "
                            f"WHERE currency = '{item["currency"]}' AND gross = {item["gross"]} "
                            f"GROUP BY currency, gross) IS NULL) "
                            f"AND currency = '{item["currency"]}' AND gross = {item["gross"]}")
                data = cur.fetchall()
                data_dict_2 = [{"vacancies_id": d[0], "vacancies_name": d[1], "salary_from": d[2], "salary_to": d[3],
                                "currency": d[4], "gross": d[5]} for d in data]

            data_dict.extend(data_dict_2)
        return data_dict

    def get_vacancies_with_keyword(self, keyword: str) -> list[dict]:
        keyword_title = keyword.lower().title()
        with self.conn.cursor() as cur:
            cur.execute(f"SELECT vacancies.name, employers.name, vacancies.alternate_url, "
                        f"vacancies.professional_roles, vacancies.snippet_requirement, "
                        f"vacancies.snippet_responsibility "
                        f"FROM vacancies JOIN employers USING(employer_id) "
                        f"WHERE vacancies.name LIKE '%{keyword}%' OR "
                        f"vacancies.name LIKE '%{keyword_title}%' OR "
                        f"vacancies.professional_roles LIKE '%{keyword}%' OR "
                        f"vacancies.professional_roles LIKE '%{keyword_title}%' OR "
                        f"vacancies.snippet_requirement LIKE '%{keyword}%' OR "
                        f"vacancies.snippet_responsibility LIKE '%{keyword}%' OR "
                        f"vacancies.snippet_requirement LIKE '%{keyword_title}%' OR "
                        f"vacancies.snippet_responsibility LIKE '%{keyword_title}%' ")
            data = cur.fetchall()
            data_dict = [{"vacancies_name": d[0], "employers_name": d[1], "vacancies_alternate_url": d[2],
                          "vacancies_professional_roles": d[3], "vacancies_snippet_requirement": d[4],
                          "vacancies_snippet_responsibility": d[5]} for d in data]
            return data_dict
