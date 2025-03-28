import requests

try:
    response = requests.get("https://www.finn.no", timeout=10)
    print(f"Статус код: {response.status_code}")
    if response.status_code == 200:
        print("Сайт доступен! Можно парсить без прокси")
    else:
        print(f"Ошибка доступа: {response.status_code}")
except Exception as e:
    print(f"Ошибка подключения: {str(e)}")
    print("Сайт недоступен или требуется прокси")