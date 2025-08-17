import mariadb
import time
import random
import string
import sys
import datetime

# --- Konfiguracja połączenia ---
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': 'xxx', 
    'database': 'mgr_test_maria'
}

czas_Insert = []
czas_avg_Insert = []
czas_Select = []
czas_avg_Select = []

# --- Funkcje pomocnicze ---
def random_string(length=1000):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

def generate_data(n, value_size=1000):
    return [(i, random_string(value_size)) for i in range(n)]

def setup_database():
    try:
        conn = mariadb.connect(**DB_CONFIG)
        cursor = conn.cursor()
        cursor.execute("DROP TABLE IF EXISTS kv")
        cursor.execute("""
            CREATE TABLE kv (
                k INT PRIMARY KEY,
                v TEXT
            ) ENGINE = InnoDB;
        """)
        conn.commit()
        cursor.close()
        conn.close()
    except mariadb.Error as e:
        print(f"Błąd połączenia z MariaDB: {e}")
        sys.exit(1)

def benchmark_insert(data):
    conn = mariadb.connect(**DB_CONFIG)
    cursor = conn.cursor()

    start = time.perf_counter()
    for k, v in data:
        cursor.execute("INSERT INTO kv (k, v) VALUES (?, ?)", (k, v))
    conn.commit()
    end = time.perf_counter()

    cursor.close()
    conn.close()
    total_time = end - start
    return total_time, total_time / len(data)

def benchmark_select(keys):
    conn = mariadb.connect(**DB_CONFIG)
    cursor = conn.cursor()

    start = time.perf_counter()
    for k in keys:
        cursor.execute("SELECT v FROM kv WHERE k = ?", (k,))
    conn.commit()
    end = time.perf_counter()

    cursor.close()
    conn.close()
    total_time = end - start
    return total_time, total_time / len(keys)


# --- Główna część programu ---

if __name__ == "__main__":
    for i in range(100):
        N = 500000
        VALUE_SIZE = 1000

        print("Tworzenie tabeli...")
        setup_database()

        print("Generowanie danych...")
        data = generate_data(N, VALUE_SIZE)

        print("Wstawianie danych...")
        total, avg = benchmark_insert(data)
        #print(f"[INSERT] Czas całkowity: {total:.2f} s | Średni czas/operacja: {avg*1e6:.2f} µs")

        print("Wyszukiwanie danych (losowych)...")
        keys = random.sample(range(N), 100000)

        total_s, avg_s = benchmark_select(keys)
        #print(f"[SELECT] Czas całkowity: {total_s:.2f} s | Średni czas/operacja: {avg_s*1e6:.2f} µs")

        czas_Insert.append(total)
        czas_avg_Insert.append(avg*1e6)
        czas_Select.append(total_s)
        czas_avg_Select.append(avg_s*1e6)

ct = datetime.datetime.now()
with open('test_B.txt', 'a') as file: 
    file.write(f"\n{N} SELECT 100000 current time:, {ct}")
    file.write(f"\nczas_Insert_B: {czas_Insert}")
    file.write(f"\nczas_avg_Insert_B = {czas_avg_Insert}")
    file.write(f"\nczas_Select_B: {czas_Select}")
    file.write(f"\nczas_avg_Select_B = {czas_avg_Select}")

# print(f"czas_Insert_B: {czas_Insert}")
# print(f"czas_avg_Insert_B = {czas_avg_Insert}")
# print(f"czas_Select_B: {czas_Select}")
# print(f"czas_avg_Select_B = {czas_avg_Select}")
