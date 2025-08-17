import mariadb
import time
import random
import string
import sys
import uuid
import multiprocessing


# --- Konfiguracja połączenia ---
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': 'xxx', 
    'database': 'mgr_test_maria'
}

N = 100_000
VALUE_SIZE = 1000

def generate_db_key():
    return uuid.uuid4().hex

def random_string(length=1000):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

def generate_data(n, value_size=1000):
    return [(generate_db_key(), random_string(value_size)) for i in range(n)]

def split(a, n):
    k, m = divmod(len(a), n)
    return list(a[i*k+min(i, m):(i+1)*k+min(i+1, m)] for i in range(n))

def setup_database(engine):
    engine_name = ''

    if (engine == 'LSM'):
        engine_name = 'RocksDB'
    elif (engine == 'BTREE'):
        engine_name = 'InnoDB'
    else:
        print(f"wrong engine name: {engine}")
        sys.exit(1)
    

    try:
        conn = mariadb.connect(**DB_CONFIG)
        cursor = conn.cursor()
        cursor.execute("DROP TABLE IF EXISTS kv")
        cursor.execute(f"""
            CREATE TABLE kv (
                k UUID PRIMARY KEY,
                v TEXT
            ) ENGINE = {engine_name};
        """)
        conn.commit()
        cursor.close()
        conn.close()
    except mariadb.Error as e:
        print(f"Błąd połączenia z MariaDB: {e}")
        sys.exit(1)

# wychodzi na odwrót
def benchmark_insert(data, multithreading = False, q = None):
    conn = mariadb.connect(**DB_CONFIG)
    cursor = conn.cursor()

    start = time.perf_counter()
    for k, v in data:
        cursor.execute(f"INSERT INTO kv (k, v) VALUES (x'{k}', '{v}')")
        conn.commit()
    end = time.perf_counter()

    cursor.close()
    conn.close()
    total_time = end - start

    if (multithreading):
        q.put(total_time)
    else:
        return total_time, total_time / len(data)

# niekonkluzywne
def benchmark_select(data, multithreading = False, q = None):
    conn = mariadb.connect(**DB_CONFIG)
    cursor = conn.cursor()

    start = time.perf_counter()
    for k, _ in data:
        cursor.execute("SELECT v FROM kv WHERE k = ?", (k,))
        conn.commit()
    end = time.perf_counter()

    cursor.close()
    conn.close()
    total_time = end - start

    if (multithreading):
        q.put(total_time)
    else:
        return total_time, total_time / len(data)
    
def benchmark_parallel(data, processes_num, target):
    data_parts = split(data, processes_num)

    processes = []
    q = multiprocessing.Queue()

    for i in range(processes_num):
        p = multiprocessing.Process(target=target, args=(data_parts[i], True, q,))
        processes.append(p)

    for p in processes:
        p.start()

    for p in processes:
        p.join()

    output = 0

    for i in range(processes_num):
        output += q.get()

    return output, output/len(data)

def benchmark_insert_parallel(data, processes_num):
    return benchmark_parallel(data, processes_num, benchmark_insert)


def benchmark_select_parallel(data, processes_num):
    return benchmark_parallel(data, processes_num, benchmark_select)


def benchmark_both_engines():
    print("-----B-Tree-----")
    print("Tworzenie tabeli...")
    setup_database(engine='BTREE')

    for i in range(1):
        print(f"Generowanie danych ({i})...")
        data = generate_data(N, VALUE_SIZE)

        print("Test insert")
        insert_btree, avg_insert_btree = benchmark_insert_parallel(data, 250)

        # print("Test select")
        # select_btree, avg_select_btree = benchmark_select_parallel(data, 4)
        

        print(f"""
            B-tree:
                insert time: {insert_btree}
        """)



    print("-----LSM-----")
    print("Tworzenie tabeli...")
    setup_database(engine='LSM')

    for i in range(1):
        print(f"Generowanie danych ({i})...")
        data = generate_data(N, VALUE_SIZE)
        print("Test insert")
        insert_lsm, avg_insert_lsm = benchmark_insert_parallel(data, 250)

        # print("Test select")
        # select_lsm, avg_select_lsm = benchmark_select_parallel(data, 4)


        print(f"""
            LSM:
                insert time: {insert_lsm}
        """)


# --- Główna część programu ---

if __name__ == "__main__":


    # czas_Insert.append(total)
    # czas_avg_Insert.append(avg*1e6)
    # czas_Select.append(total_s)
    # # czas_avg_Select.append(avg_s*1e6)
    # print(N)
    # #print(f"czas_Insert: {czas_Insert}")
    # print(f"czas_avg_Insert_B = {czas_avg_Insert}")
    # #print(f"czas_Select: {czas_Select}")
    # print(f"czas_avg_Select_B = {czas_avg_Select}")

    benchmark_both_engines()
