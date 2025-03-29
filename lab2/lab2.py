import psycopg2
import threading
import time

STR = "dbname=lab2 user=postgres password=1234 host=localhost"

def setup_database():
    conn = psycopg2.connect(STR)
    cursor = conn.cursor()

    # Створюємо таблицю, якщо її немає
    cursor.execute("CREATE TABLE IF NOT EXISTS user_counter (user_id INTEGER PRIMARY KEY, counter INTEGER, version INTEGER)")

    # Перевіряємо, чи є запис із user_id = 1. Якщо запис є, оновлюємо каунтер та версію, якщо запису немає, створюємо його
    cursor.execute("SELECT COUNT(*) FROM user_counter WHERE user_id = 1")
    row_count = cursor.fetchone()[0]
    if row_count > 0:
        cursor.execute("UPDATE user_counter SET counter = 0, version = 0 WHERE user_id = 1")
    else:
        cursor.execute("INSERT INTO user_counter (user_id, counter, version) VALUES (1, 0, 0)")

    conn.commit()
    cursor.close()
    conn.close()
    
def reset_counter():
    conn = psycopg2.connect(STR)
    cursor = conn.cursor()
    cursor.execute("UPDATE user_counter SET counter = 0 WHERE user_id = 1")
    conn.commit()
    cursor.close()
    conn.close()

def get_counter():
    conn = psycopg2.connect(STR)
    cursor = conn.cursor()
    cursor.execute("SELECT counter FROM user_counter WHERE user_id = 1")
    counter = cursor.fetchone()[0]
    cursor.close()
    conn.close()
    return counter


def lost_update():
    conn = psycopg2.connect(STR)
    cursor = conn.cursor()

    for _ in range(10000):
        cursor.execute("SELECT counter FROM user_counter WHERE user_id = 1")
        counter = cursor.fetchone()[0]
        counter += 1
        cursor.execute("UPDATE user_counter SET counter = %s WHERE user_id = %s", (counter, 1))
        conn.commit()

    cursor.close()
    conn.close()


def in_place_update():
    conn = psycopg2.connect(STR)
    cursor = conn.cursor()

    for _ in range(10000):
        cursor.execute("UPDATE user_counter SET counter = counter + 1 WHERE user_id = %s", (1,))
        conn.commit()

    cursor.close()
    conn.close()


def row_level_locking():
    conn = psycopg2.connect(STR)
    cursor = conn.cursor()

    for _ in range(10000):
        cursor.execute("SELECT counter FROM user_counter WHERE user_id = 1 FOR UPDATE")
        counter = cursor.fetchone()[0]
        counter += 1
        cursor.execute("UPDATE user_counter SET counter = %s WHERE user_id = %s", (counter, 1))
        conn.commit()

    cursor.close()
    conn.close()


def optimistic_concurrency_control():
    conn = psycopg2.connect(STR)
    cursor = conn.cursor()

    for _ in range(10000):
        while True:
            cursor.execute("SELECT counter, version FROM user_counter WHERE user_id = 1")
            (counter, version) = cursor.fetchone()
            counter += 1
            cursor.execute("UPDATE user_counter SET counter = %s, version = %s WHERE user_id = %s AND version = %s", 
            (counter, version + 1, 1, version))

            conn.commit()
            if cursor.rowcount > 0:  # Перевіряємо, чи оновлення пройшло
                break

    cursor.close()
    conn.close()


def run_test(method, name):
    reset_counter()
    start_time = time.time()
    threads = []

    # 10 потоків
    for _ in range(10): 
        t = threading.Thread(target=method)
        threads.append(t)
        t.start()

    # Чекаєм на завершення всіх потоків
    for t in threads:
        t.join()

    end_time = time.time()
    print(f"{name} виконано за {end_time - start_time:.2f} секунд")
    print(f"Значення каунтера: {get_counter()}\n")


setup_database()
run_test(lost_update, "Lost Update")
run_test(in_place_update, "In-Place Update")
run_test(row_level_locking, "Row-Level Locking")
run_test(optimistic_concurrency_control, "Optimistic Concurrency Control")
