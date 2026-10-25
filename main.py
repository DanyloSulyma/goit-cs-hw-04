import os
import time
from threading import Thread, Lock
from multiprocessing import Process, Queue, cpu_count
from collections import defaultdict


# Спільна функція для пошуку слів у файлах
def search_in_files(files, keywords):
    """Шукає ключові слова у заданому списку файлів."""
    local_results = defaultdict(list)
    for filepath in files:
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                # Читаємо файл цілком або по рядках (для великих файлів краще по рядках)
                text = f.read()
                for keyword in keywords:
                    if keyword in text:
                        local_results[keyword].append(filepath)
        except (IOError, OSError) as e:
            print(f"[Помилка] Не вдалося прочитати файл {filepath}: {e}")
        except Exception as e:
            print(f"[Неочікувана помилка] {filepath}: {e}")
    return local_results


# 1. Багатопотоковий підхід (Threading)
def thread_worker(files, keywords, shared_results, lock):
    local_res = search_in_files(files, keywords)
    # Блокуємо доступ до спільного словника перед його оновленням
    with lock:
        for key, paths in local_res.items():
            shared_results[key].extend(paths)

def run_threading(files, keywords, num_threads=4):
    shared_results = defaultdict(list)
    lock = Lock()
    threads = []
    
    # Розділяємо файли на частини для кожного потоку
    chunk_size = len(files) // num_threads or 1
    chunks = [files[i:i + chunk_size] for i in range(0, len(files), chunk_size)]
    
    start_time = time.time()
    
    for chunk in chunks:
        t = Thread(target=thread_worker, args=(chunk, keywords, shared_results, lock))
        threads.append(t)
        t.start()
        
    for t in threads:
        t.join()
        
    end_time = time.time()
    print(f"Час виконання (Threading): {end_time - start_time:.4f} секунд")
    return dict(shared_results)


# 2. Багатопроцесорний підхід (Multiprocessing)
def process_worker(files, keywords, queue):
    local_res = search_in_files(files, keywords)
    queue.put(local_res)

def run_multiprocessing(files, keywords, num_processes=4):
    queue = Queue()
    processes = []
    
    # Розділяємо файли на частини для кожного процесу
    chunk_size = len(files) // num_processes or 1
    chunks = [files[i:i + chunk_size] for i in range(0, len(files), chunk_size)]
    
    start_time = time.time()
    
    for chunk in chunks:
        p = Process(target=process_worker, args=(chunk, keywords, queue))
        processes.append(p)
        p.start()
        
    # Збираємо результати з черги
    combined_results = defaultdict(list)
    for _ in range(len(chunks)):
        local_res = queue.get()
        for key, paths in local_res.items():
            combined_results[key].extend(paths)
            
    for p in processes:
        p.join()
        
    end_time = time.time()
    print(f"Час виконання (Multiprocessing): {end_time - start_time:.4f} секунд")
    return dict(combined_results)


# Допоміжна функція для створення тестових даних
def create_test_files():
    os.makedirs("test_files", exist_ok=True)
    files = []
    for i in range(10):
        filepath = f"test_files/file_{i}.txt"
        with open(filepath, "w", encoding="utf-8") as f:
            if i % 2 == 0:
                f.write("Цей текст містить слово Python та багато іншого.\n")
            else:
                f.write("Тут ми вивчаємо паралельне програмування на C++.\n")
            if i == 5:
                f.write("До речі, multiprocessing дуже корисний модуль.\n")
        files.append(filepath)
    return files


# Головний блок запуску
if __name__ == '__main__':
    # 1. Підготовка даних
    file_paths = create_test_files()
    search_words = ["Python", "програмування", "multiprocessing", "відсутнє_слово"]
    
    workers_count = cpu_count() # Використовуємо кількість логічних ядер процесора
    
    print(f"Пошук слів: {search_words}")
    print(f"Кількість файлів: {len(file_paths)}")
    print(f"Кількість потоків/процесів: {workers_count}\n")
    
    # 2. Тестування Threading
    print("--- Запуск Threading ---")
    threading_results = run_threading(file_paths, search_words, num_threads=workers_count)
    for word, paths in threading_results.items():
        print(f"'{word}': знайдено у {len(paths)} файлах")
    
    print("\n--- Запуск Multiprocessing ---")
    # 3. Тестування Multiprocessing
    multiprocessing_results = run_multiprocessing(file_paths, search_words, num_processes=workers_count)
    for word, paths in multiprocessing_results.items():
        print(f"'{word}': знайдено у {len(paths)} файлах")

    # Нормалізація результатів: сортування списків файлів для кожного ключового слова
    normalized_threading = {word: sorted(paths) for word, paths in threading_results.items()}
    normalized_multiprocessing = {word: sorted(paths) for word, paths in multiprocessing_results.items()}

    # Порівняння відсортованих даних
    assert normalized_threading == normalized_multiprocessing, "Помилка: результати двох методів не збігаються!"
    print("Перевірка пройдена: результати обох методів ідентичні.")