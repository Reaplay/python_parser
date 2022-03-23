#стандартные библы подключаем
import configparser
from numpy import dstack
import requests
import re
import json

#моя библа
import p_mysql_connect
import loggers

#читаем файл-конфиг
config = configparser.ConfigParser()
config.read('config.ini')
#инициализируем класс для работы с БД и подключаемся к ней
db_conn = p_mysql_connect.Database(config['DATABASE'])
logger = loggers.get_logger('main')
#получем список фильмов
array_films_query = db_conn.get_film_query()
logger.info(f"Взяли данные из БД")
print('Get list query')


for film in array_films_query:
    
    print ('_______________________')
    print('ID - '+ str(film[0]))
    
    # Проверяем какие линки прописаны. в зависимости от этого идем разными путями
    if(film[2] is None):
        logger.info(f"Работаем по линку {film[1]}")
        
        #ищем в базе этот линк
        id = db_conn.search_in_base(film[1],'link_imdb')

        #если не нашли, то обновляем статус записи
        if id is not None:
            print ('This film is found in DB')
            print ('STATUS: Not need add')
        # обновляем статус
            db_conn.update_status_film(film[0], '1')
            continue

        print ('New film')
        
        #из строчки достаем ИД фильма
        id_film = re.findall("(tt\d{2,})", str(film[1]))[0]
        
        # хеад для запроса. Без такого не получим ответ
        headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}
        
        #генерируем УРЛ для запроса
        url = config['IMDB']['URL_BASE']+config['IMDB']['API_KEY']+"/"+str(id_film)

        # запрашиваем страницу
        result = requests.get(url, headers=headers)
        
        # если получаем ответ с другим кодом
        if result.status_code != 200:
            print ('Return not 200')
            print ('STATUS: ERROR API')
            # обновляем статус
            db_conn.update_status_film(film[0],'2')
            continue
        
        # парсим результат
        data_json = json.loads(result.content.decode())
        
        #добавляем инфу
        db_conn.insert_film(data_json['title'], data_json['originalTitle'], data_json['plotLocal'], data_json['releaseDate'], data_json['year'],film[1])
        print ('STATUS: ADDED')
        #обновляем статус
        db_conn.update_status_film(film[0], '1')


    elif(film[1] is None):
        print('IMDB is null')

        #ищем в базе этот линк
        id = db_conn.search_in_base(film[2],'link_kinopoisk')

         #если не нашли, то обновляем статус записи
        if id is not None:
            print ('This film is found in DB')
            print ('STATUS: Not need add')
        # обновляем статус
            db_conn.update_status_film(film[0], '1')
            continue

        #из строчки достаем ИД фильма
        id_film = re.findall("(\d{2,})", str(film[2]))[0]
        
        # хеад для запроса. Без такого не получим ответ
        headers = {'X-API-KEY': config['KINOPOISK']['API_KEY']}
        
        #генерируем УРЛ для запроса
        url = config['KINOPOISK']['URL_BASE']+str(id_film)

        # запрашиваем страницу
        result = requests.get(url, headers=headers)
        distributions = requests.get(url+'/distributions', headers=headers)

        # если получаем ответ с другим кодом
        if result.status_code != 200 or distributions.status_code != 200:
            print ('Return not 200')
            print ('STATUS: ERROR API')
            # обновляем статус
            db_conn.update_status_film(film[0],'2')
            continue

        # парсим результат
        data_json = json.loads(result.content.decode())
        if data_json['imdbId'] is not None:
            data_json['link_imdb'] = 'https://www.imdb.com/title/'+str(data_json['imdbId'])
        else:
            data_json['link_imdb'] = None

        #получаем дату выхода в прокат
        data_json_distributions = json.loads(distributions.content.decode())
        relise_data = ''
        for array_distrib in data_json_distributions['items']:
            if(array_distrib['type'] == 'WORLD_PREMIER'):
                relise_data = array_distrib['date']
        data_json['releaseDate'] = relise_data

        #добавляем инфу
        db_conn.insert_film(data_json['nameEn'], data_json['nameOriginal'], data_json['description'], data_json['releaseDate'], data_json['year'],data_json['link_imdb'], film[2], data_json['nameRu'], data_json['imdbId'], data_json['kinopoiskId'])

        print ('STATUS: ADDED')
        #обновляем статус
        db_conn.update_status_film(film[0], '1')

    else:
        print ('IMDB and KP found')
        print ('not work')

        # ищем в базе по имдб
        id_is_imdb = db_conn.search_in_base(film[1],'link_imdb')

        # ищем в базе по кп
        id_is_kinopoisk = db_conn.search_in_base(film[2],'link_kinopoisk')

        # если разные ИД то пишем ошибку
        if (id_is_imdb is not None and id_is_kinopoisk is not None) and (id_is_imdb != id_is_kinopoisk):
            print ('IMDB and KP link found, but this ID don\'t eq')
            print ('STATUS: ERROR')
            # обновляем статус
            db_conn.update_status_film(film[0],'3')
            #добавялем в базу что была такая-то ошибка
            #db_conn.error_status()
            continue
        elif (id_is_imdb is not None and id_is_kinopoisk is not None) and (id_is_imdb == id_is_kinopoisk):
            print ('IMDB and KP link found, ID is eq')
            print ('STATUS: Not need update/add')
            # обновляем статус
            db_conn.update_status_film(film[0],'1')
        
        # # если находим только часть (кп или имдб), то обновляем запись по недостающему урл
        # elif(id_is_imdb is not None and id_is_kinopoisk is None):
        #     print ('НУЖНО ПРОВЕРИТЬ УРЛы')
        #     print ('Update link. Add url KP')
        #     print ('UPDATE STATUS: OK')
        #     db_conn.update_url(id_is_imdb, 'link_kinopoisk', film[2])
        #     db_conn.update_status_film(film[0],'1')
        #     continue
        # elif(id_is_kinopoisk is not None and id_is_imdb is None):
        #     print ('НУЖНО ПРОВЕРИТЬ УРЛы')
        #     print ('Update link. Add url IMDB')
        #     print ('UPDATE STATUS: OK')
        #     db_conn.update_url(id_is_kinopoisk, 'link_imdb', film[1])
        #     db_conn.update_status_film(film[0],'1')
        #     continue
        # если не нашли
        else:
        # идем в кинопоиск, достаем данные
            #из строчки достаем ИД фильма
            id_film = re.findall("(\d{2,})", str(film[2]))[0]
            
            # хеад для запроса. Без такого не получим ответ
            headers = {'X-API-KEY': config['KINOPOISK']['API_KEY']}
            
            #генерируем УРЛ для запроса
            url = config['KINOPOISK']['URL_BASE']+str(id_film)

            # запрашиваем страницу
            result = requests.get(url, headers=headers)
            distributions = requests.get(url+'/distributions', headers=headers)
            # если получаем ответ с другим кодом
            if result.status_code != 200  or distributions.status_code != 200:
                print ('Return not 200')
                print ('STATUS: ERROR API')
                # обновляем статус
                db_conn.update_status_film(film[0],'2')
                #добавялем в базу что была такая-то ошибка
                #db_conn.error_status()
                continue
            
            # парсим результат
    
            data_json = json.loads(result.content.decode())
        # если есть ссылка на имдб, то сверяем с имеющейся
            link_imdb = re.findall("(tt\d{2,})", str(film[1]))[0]

            if(id_is_imdb is not None and id_is_kinopoisk is None):
                id_in_base = str(id_is_imdb[0])
            elif(id_is_kinopoisk is not None and id_is_imdb is None):
                id_in_base = str(id_is_kinopoisk[0])
            else:
                id_in_base = None

            if(data_json['imdbId'] is not None and data_json['imdbId'] != link_imdb):
                print ('ID imdb/kp not eq in DB')
                print ('STATUS: ERROR')
                # обновляем статус
                db_conn.update_status_film(film[0],'4')
                continue
            elif(data_json['imdbId'] is not None and data_json['imdbId'] == link_imdb):
                
                data_json['link_imdb'] = 'https://www.imdb.com/title/'+str(data_json['imdbId'])

                #получаем дату выхода в прокат
                data_json_distributions = json.loads(distributions.content.decode())
                relise_data = ''
                for array_distrib in data_json_distributions['items']:
                    if(array_distrib['type'] == 'WORLD_PREMIER'):
                        relise_data = array_distrib['date']
                data_json['releaseDate'] = relise_data

                #добавляем инфу
                if(id_in_base is None):
                    print ('STATUS: ADDED')
                    db_conn.insert_film(data_json['nameEn'], data_json['nameOriginal'], data_json['description'], data_json['releaseDate'], data_json['year'],data_json['link_imdb'], film[2], data_json['nameRu'], data_json['imdbId'], data_json['kinopoiskId'])
                else:
                    print ('STATUS: UPDATED')
                    db_conn.update_film(id_in_base, data_json['nameEn'], data_json['nameOriginal'], data_json['description'], data_json['releaseDate'], data_json['year'],data_json['link_imdb'], film[2], data_json['nameRu'], data_json['imdbId'], data_json['kinopoiskId'])
                #обновляем статус
                db_conn.update_status_film(film[0], '1')
            else:

                #из строчки достаем ИД фильма
                id_film = re.findall("(tt\d{2,})", str(film[1]))[0]

                 # хеад для запроса. Без такого не получим ответ
                headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}
                
                #генерируем УРЛ для запроса
                url = config['IMDB']['URL_BASE']+config['IMDB']['API_KEY']+"/"+str(id_film)

                # запрашиваем страницу
                result = requests.get(url, headers=headers)
                
                # если получаем ответ с другим кодом
                if result.status_code != 200:
                    print ('Return not 200')
                    print ('STATUS: ERROR API')
                    # обновляем статус
                    db_conn.update_status_film(film[0],'2')
                    #добавялем в базу что была такая-то ошибка
                    
                    continue
                
                # парсим результат
                data_json_imdb = json.loads(result.content.decode())
                
                if(data_json['nameOriginal'] == data_json_imdb['originalTitle'] and str(data_json['year']) == data_json_imdb['year']):
                    #получаем дату выхода в прокат
                    data_json_distributions = json.loads(distributions.content.decode())
                    relise_data = ''
                    for array_distrib in data_json_distributions['items']:
                        if(array_distrib['type'] == 'WORLD_PREMIER'):
                            relise_data = array_distrib['date']
                    data_json['releaseDate'] = relise_data
                    #добавляем инфу
                    if(id_in_base is None):
                        print ('STATUS: ADDED')
                        db_conn.insert_film(data_json['nameEn'], data_json['nameOriginal'], data_json['description'], data_json['releaseDate'], data_json['year'],film[1], film[2], data_json['nameRu'], data_json['imdbId'], data_json['kinopoiskId'])
                    else:
                        print ('STATUS: UPDATED')
                        db_conn.update_film(id_in_base, data_json['nameEn'], data_json['nameOriginal'], data_json['description'], data_json['releaseDate'], data_json['year'],film[1], film[2], data_json['nameRu'], data_json['imdbId'], data_json['kinopoiskId'])

                    #обновляем статус
                    db_conn.update_status_film(film[0], '1')
                else:
                    print ('ERROR: Data in API IMDB and API KP not eq')
                    db_conn.update_status_film(film[0],'4')
                continue

