import psycopg2
from pprint import pprint


def create_table(cur):
    cur.execute("""
            CREATE TABLE IF NOT EXISTS client(
            client_id SERIAL PRIMARY KEY,
            first_name VARCHAR(60) NOT NULL,
            second_name VARCHAR(60) NOT NULL,
            email VARCHAR(60) UNIQUE NOT NULL);
    """)
    cur.execute("""
            CREATE TABLE IF NOT EXISTS phone(
            phone_id SERIAL PRIMARY KEY,
            client_id INTEGER REFERENCES client(client_id),
            phone_number VARCHAR(12));
            """)
    return "Таблица была создана"


def delete_tables(cur):
    cur.execute("""
            DROP TABLE IF EXISTS client, phone CASCADE;
            """)
    return "Таблицы были удалены"


def add_client(cur, first_name, second_name, email, phone_number=None):
    cur.execute("""
        INSERT INTO client(first_name, second_name, email)
        VALUES(%s,%s,%s)
        RETURNING client_id, first_name, second_name, email;""",
                (first_name, second_name, email))
    cur.execute("""
    SELECT client_id FROM client
    ORDER BY client_id DESC
    LIMIT 1""")
    id_c = cur.fetchone()[0]
    if phone_number is None:
        return id_c
    else:
        add_phone(cur, id_c, phone_number)
        return id_c


def add_phone(cur, client_id, phone_number=None):
    cur.execute("""
    INSERT INTO phone(client_id, phone_number)
    VALUES(%s, %s);""",
                (client_id, phone_number))
    return client_id


def all_clients(cur):
    cur.execute("""
    SELECT c.client_id, c.first_name, c.second_name, c.email, p.phone_number 
    FROM client c
    LEFT JOIN phone p ON c.client_id = p.client_id
    ORDER by c.client_id;
    """)
    pprint(cur.fetchall())


def change_client(cur, c_id, first_name=None, second_name=None, email=None):
    cur.execute("""
    SELECT * FROM client
    WHERE client_id = %s;
    """, (c_id,))
    client = cur.fetchone()
    if first_name is None:
        first_name = client[1]
    if second_name is None:
        second_name = client[2]
    if email is None:
        email = client[3]
    cur.execute("""
        UPDATE client
        SET first_name = %s, second_name = %s, email =%s 
        WHERE client_id = %s;
        """, (first_name, second_name, email, c_id))
    return c_id


def del_phone(cur, number):
    cur.execute("""
    DELETE FROM phone
    WHERE phone_number = %s;
    """, (number,))
    return number


def del_client(cur, client):
    cur.execute("""
    DELETE FROM phone
    WHERE client_id = %s;
    """, (client,))
    cur.execute("""
    DELETE FROM client
    WHERE client_id = %s;
    """, (client,))


def find_client(cur, first_name=None, second_name=None, email=None, phone_number=None):
    cur.execute("""
        SELECT c.first_name, c.second_name, c.email, p.phone_number FROM client c
        LEFT JOIN phone p ON c.client_id = p.client_id
        WHERE (c.first_name = %s OR %s IS NULL) AND
              (c.second_name = %s OR %s IS NULL) AND
              (c.email = %s OR %s IS NULL) AND
              (p.phone_number = %s OR %s IS NULL);
        """, (first_name, first_name, second_name, second_name, email, email, phone_number, phone_number))
    return cur.fetchone()


if __name__ == '__main__':
    with psycopg2.connect(database='',
                          user='', password='') as conn:
        with conn.cursor() as curs:
            # удаляем таблицы
            print(delete_tables(curs))
            # Функция, создающая структуру БД (таблицы)
            print(create_table(curs))
            # Функция, позволяющая добавить нового клиента.
            print(add_client(curs, "Egor", "Logunov", "fnidnvid@mail.ru", '+70000000000'))
            print(add_client(curs, "Lionel", "Ronaldo", "dfnivnwyv@mail.ru", '+79999999999'))
            print(add_client(curs, "Evgeni", "Runin", "ndicfvb@mail.ru", '+75555555555'))
            print(add_client(curs, "Nikolai", "Kolov", "dmncuf@mail.ru", '+71111111111'))
            print(add_client(curs, "Maksim", "Panov", "dconivi@mail.ru"))
            all_clients(curs)
            # Функция, позволяющая добавить телефон для существующего клиента.
            print(add_phone(curs, 5, '+74444444444'))
            print(add_phone(curs, 2, '+74444444446'))
            all_clients(curs)
            # Функция, позволяющая изменить данные о клиенте.
            print(change_client(curs, 1, "WHO", "AM", 'i@lol.com'))
            print(change_client(curs, 2, None, "MESSI", None))
            all_clients(curs)
            # Функция, позволяющая удалить телефон для существующего клиента.
            del_phone(curs, '+74444444446')
            # all_clients(curs)
            # Функция, позволяющая удалить существующего клиента.
            del_client(curs, 1)
            # all_clients(curs)
            # Функция, позволяющая найти клиента по его данным: имени, фамилии, email или телефону.
            print(find_client(curs, None, 'MESSI', 'dfnivnwyv@mail.ru', None))
            # all_clients(curs)
