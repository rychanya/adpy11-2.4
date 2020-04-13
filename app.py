import csv
import re
from datetime import datetime
import pymongo
from pprint import pprint


def read_data(csv_file: str, db: pymongo.database.Database):
    current_year = datetime.now().year

    def parse_date(date: str):
        day, month = tuple(map(int, date.split('.')))
        return datetime(year=current_year, month=month, day=day)

    with open(csv_file, encoding='utf8') as csvfile:
        # прочитать файл с данными и записать в коллекцию
        reader = csv.DictReader(csvfile)
        for event in reader:
            event['Дата'] = parse_date(event['Дата'])
            event['Цена'] = int(event['Цена'])
            db['event'].insert_one(event)


def find_cheapest(db: pymongo.database.Database):
    return db['event'].find().sort('Цена')


def find_by_name(name, db: pymongo.database.Database):
    regex = re.compile(f'.*{name}.*', re.IGNORECASE)
    return db['event'].find({'Исполнитель': regex}).sort('Цена')


def find_by_date(db: pymongo.database.Database):
    return db['event'].find().sort([
        ('Дата', pymongo.ASCENDING), ('Цена', pymongo.ASCENDING)])


if __name__ == '__main__':
    client = pymongo.MongoClient()
    db = client['hm24']
    read_data('artists.csv', db)
    print('Самый дешовый:')
    pprint(list(find_cheapest(db))[0])
    name = input('Введите часть имени: ')
    print('Результат')
    pprint(list(find_by_name(name, db)))
    print('Все события отсортированные по дате и цене')
    pprint(list(find_by_date(db)))
    client.close()
