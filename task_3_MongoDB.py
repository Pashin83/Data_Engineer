from pymongo import MongoClient
from datetime import datetime, timedelta
import json

# Подключение к MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["my_database"]
collection = db["user_events"]
archived_collection = db["archived_users"] #Добавление коллекции для архивированных пользователей

# Текущая дата
current_date = datetime.now()
registration_over_30 = current_date - timedelta(days=30) #Регистрация более 30 дней назад
activity_over_14 = current_date - timedelta(days=14) #Отсутствие активности последние 14 дней

# Агрегация с фильтрацией и группировкой
pipeline = [
    {"$match": {"user_info.registration_date": {"$lt": registration_over_30}}},
    {"$sort": {"user_id": 1, "event_time": -1}},  # сортируем по user_id
    {"$group": {
        "_id": "$user_id",
        "last_activity": {"$first": "$event_time"},
        "user_data": {"$first": "$$ROOT"}
    }},
    {"$match": {"last_activity": {"$lt": activity_over_14}}}
]

#Запускает pipeline и превращает результат в список
users_archive = list(collection.aggregate(pipeline))

#Списки для хранения информации о заархивированных пользователях
archived_users = [] # Все данные пользователей
archived_users_id = [] # Только ID пользователей для отчета

for user in users_archive:
    # Получаем документ последнего события пользователя
    user_data = user["user_data"]

    # Добавляем пользователя в архив
    archived_collection.insert_one(user_data)

    # Удаляем все события пользователя из основной коллекции
    collection.delete_many({"user_id": user_data["user_id"]})

    # Добавление к спискам
    archived_users.append(user_data)
    archived_users_id.append(user_data["user_id"])

# Формирование отчета в формате JSON
report = {"date": current_date.strftime("%Y-%m-%d"), # Дата
          "archived_users_count": len(archived_users_id), # Количество заархивированных пользователей
          "archived_users_id": archived_users_id}  # Список ID пользователей

# Сохранение отчета в формате JSON c названием по дате
file_name=f'{current_date.strftime("%Y-%m-%d")}.json'
with open(file_name, 'w', encoding='utf-8') as f: 
    json.dump(report, f, ensure_ascii=False, indent=4, default=str)

print(f'Заархивировано пользователей {len(archived_users_id)}')
print(f'Отчет сохранен в файл: {file_name}')

client.close()



