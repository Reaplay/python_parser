#стандартные библы подключаем
import configparser
from numpy import dstack
import requests
import re
import json

#моя библа
import p_mysql_connect

#читаем файл-конфиг
config = configparser.ConfigParser()
config.read('config.ini')
#инициализируем класс для работы с БД и подключаемся к ней
db_conn = p_mysql_connect.Database(config['DATABASE'])

#получем список фильмов
array_films_query = db_conn.get_film_query()

print('Get list query')


for film in array_films_query:
    
    print ('_______________________')
    print('ID - '+ str(film[0]))
    
    # Проверяем какие линки прописаны. в зависимости от этого идем разными путями
    if(film[2] is None):
        print('KP is null')
        
        #ищем в базе этот линк
        id = db_conn.search_in_base(film[1],'link_imdb')

        #если не нашли, то обновляем статус записи
        if id is not None:
            print ('This film is found in DB')
            print ('UPDATE STATUS DISABLED')
        # обновляем статус
            db_conn.update_status(film[0], '1')
            continue

        print ('New film')
        
        #из строчки достаем ИД фильма
        id_film = re.findall("(tt\d{5,})", str(film[1]))[0]
        
        # хеад для запроса. Без такого не получим ответ
        headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}
        
        #генерируем УРЛ для запроса
        url = config['IMDB']['URL_BASE']+config['IMDB']['API_KEY']+"/"+str(id_film)

        # запрашиваем страницу
        result = requests.get(url, headers=headers)
        
        # если получаем ответ с другим кодом
        if result.status_code != 200:
            print ('Return not 200')
            print ('UPDATE STATUS ERROR')
            # обновляем статус
            db_conn.update_status(film[0],'2')
            #добавялем в базу что была такая-то ошибка
            #db_conn.error_status()
            continue
        
        # парсим результат
        data_json = json.loads(result.content.decode())
        
        #добавляем инфу
        db_conn.insert_film(data_json['title'], data_json['originalTitle'], data_json['plotLocal'], data_json['releaseDate'], data_json['year'],film[1])

        #обновляем статус
        db_conn.update_status(film[0], '1')


    elif(film[1] is None):
        print('IMDB is null')

        #ищем в базе этот линк
        id = db_conn.search_in_base(film[1],'link_imdb')

         #если не нашли, то обновляем статус записи
        if id is not None:
            print ('This film is found in DB')
            print ('UPDATE STATUS DISABLED')
        # обновляем статус
            db_conn.update_status(film[0], '1')
            continue

        #из строчки достаем ИД фильма
        id_film = re.findall("(\d{5,})", str(film[2]))[0]
        
        # хеад для запроса. Без такого не получим ответ
        headers = {'X-API-KEY': config['KINOPOISK']['API_KEY']}
        
        #генерируем УРЛ для запроса
        url = config['KINOPOISK']['URL_BASE']+str(id_film)

        # запрашиваем страницу
        result = requests.get(url, headers=headers)
        
        # если получаем ответ с другим кодом
        if result.status_code != 200:
            print ('Return not 200')
            print ('UPDATE STATUS ERROR')
            # обновляем статус
            db_conn.update_status(film[0],'2')
            #добавялем в базу что была такая-то ошибка
            #db_conn.error_status()
            continue
        
        # парсим результат
        data_json = json.loads(result.content.decode())
        if data_json['imdbId'] is not None:
            data_json['link_imdb'] = 'https://www.imdb.com/title/tt'+str(data_json['imdbId'])
        #добавляем инфу
        db_conn.insert_film(data_json['nameEn'], data_json['nameOriginal'], data_json['description'], None, data_json['year'],data_json['link_imdb'], film[1], data_json['nameRu'], data_json['imdbId'], data_json['kinopoiskId'])

        #обновляем статус
        db_conn.update_status(film[0], '1')

    else:
        print ('IMDB and KP found')
        print ('not work')
   
