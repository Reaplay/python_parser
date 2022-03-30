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
logger = loggers.get_logger('API','API')

#получем список фильмов
array_films_query = db_conn.get_film_query()
logger.info(f"Take data from DB")

for film in array_films_query:
    logger.info(f"ID Query film - {film[0]}")
    
    # Проверяем какие линки прописаны. в зависимости от этого идем разными путями
    if(film[2] is None):
        logger.info(f"URL KP in null. Search film url IMDB ({film[1]}) in DB")
        
        #ищем в базе этот линк
        id = db_conn.search_in_base(film[1],'link_imdb')

        #если нашли, то обновляем статус записи
        if id is not None:
            logger.info(f"Found film in DB. Not need added")
            # обновляем статус
            db_conn.update_status_film(film[0], '1')
            logger.info(f"Status film in query updated")
            continue
        
        logger.info(f"Film not found in DB. Start process added")

        #из строчки достаем ИД фильма
        id_film = re.findall("(tt\d{2,})", str(film[1]))[0]
        
        # хеад для запроса. Без такого не получим ответ
        headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}
        
        #генерируем УРЛ для запроса
        url = config['IMDB']['URL_BASE']+config['IMDB']['API_KEY']+"/"+str(id_film)

        # запрашиваем страницу
        logger.info(f"Get data at API IMDB")
        result = requests.get(url, headers=headers)
        
        # если получаем ответ с другим кодом
        if result.status_code != 200:
            logger.error(f"API return not 200 code")
            # обновляем статус
            db_conn.update_status_film(film[0],'2')
            logger.info(f"Status film in query updated")
            continue
        
        # парсим результат
        data_json = json.loads(result.content.decode())
        
        #добавляем инфу
        db_conn.insert_film(data_json['title'], data_json['originalTitle'], data_json['plotLocal'], data_json['releaseDate'], data_json['year'],data_json['image'],film[1])
        logger.info(f"Film add in DB")

        #обновляем статус
        db_conn.update_status_film(film[0], '1')
        logger.info(f"Status film in query updated")


    elif(film[1] is None):
        logger.info(f"URL IMDB in null. Search film url KP ({film[1]}) in DB")

        #ищем в базе этот линк
        id = db_conn.search_in_base(film[2],'link_kinopoisk')

         #если не нашли, то обновляем статус записи
        if id is not None:
            logger.info(f"Found film in DB. Not need added")
        # обновляем статус
            db_conn.update_status_film(film[0], '1')
            logger.info(f"Status film in query updated")
            continue

        #из строчки достаем ИД фильма
        id_film = re.findall("(\d{2,})", str(film[2]))[0]
        
        # хеад для запроса. Без такого не получим ответ
        headers = {'X-API-KEY': config['KINOPOISK']['API_KEY']}
        
        #генерируем УРЛ для запроса
        url = config['KINOPOISK']['URL_BASE']+str(id_film)

        # запрашиваем страницу
        logger.info(f"Get data at API KP (film and fistribution) ")
        result = requests.get(url, headers=headers)
        distributions = requests.get(url+'/distributions', headers=headers)
        
        # если получаем ответ с другим кодом
        if result.status_code != 200 or distributions.status_code != 200:
            logger.error(f"API return not 200 code")
            # обновляем статус
            db_conn.update_status_film(film[0],'2')
            logger.info(f"Status film in query updated")
            continue

        # парсим результат
        data_json = json.loads(result.content.decode())
        if data_json['imdbId'] is not None:
            logger.info(f"KP return link to IMDB")
            data_json['link_imdb'] = 'https://www.imdb.com/title/'+str(data_json['imdbId'])
        else:
            logger.info(f"KP not return link to IMDB. Is NULL")
            data_json['link_imdb'] = None

        #получаем дату выхода в прокат
        data_json_distributions = json.loads(distributions.content.decode())
        relise_data = ''
        for array_distrib in data_json_distributions['items']:
            if(array_distrib['type'] == 'WORLD_PREMIER'):
                logger.info(f"Found date World Premier")
                relise_data = array_distrib['date']

        data_json['releaseDate'] = relise_data

        #добавляем инфу
        db_conn.insert_film(data_json['nameEn'], data_json['nameOriginal'], data_json['description'], data_json['releaseDate'], data_json['year'],data_json['posterUrl'],data_json['link_imdb'], film[2], data_json['nameRu'], data_json['imdbId'], data_json['kinopoiskId'])
        logger.info(f"Film add in DB")

        #обновляем статус
        db_conn.update_status_film(film[0], '1')
        logger.info(f"Status film in query updated")

    else:
        logger.info(f"URL IMDB and KP not NULL. Search film url ({film[1]}) in DB")

        # ищем в базе по имдб
        id_is_imdb = db_conn.search_in_base(film[1],'link_imdb')

        # ищем в базе по кп
        id_is_kinopoisk = db_conn.search_in_base(film[2],'link_kinopoisk')

        # если разные ИД то пишем ошибку
        if (id_is_imdb is not None and id_is_kinopoisk is not None) and (id_is_imdb != id_is_kinopoisk):
            logger.warning(f"DB return different ID films on KP and IMDB")
            # обновляем статус
            db_conn.update_status_film(film[0],'3')
            logger.info(f"Status film in query updated")
            continue
        elif (id_is_imdb is not None and id_is_kinopoisk is not None) and (id_is_imdb == id_is_kinopoisk):
            logger.info(f"Found film in DB. Not need added")
            # обновляем статус
            db_conn.update_status_film(film[0],'1')
            logger.info(f"Status film in query updated")
        
      
        # если не нашли
        else:
            logger.info(f"Not found film in DB")
        # идем в кинопоиск, достаем данные
            #из строчки достаем ИД фильма
            id_film = re.findall("(\d{2,})", str(film[2]))[0]
            
            # хеад для запроса. Без такого не получим ответ
            headers = {'X-API-KEY': config['KINOPOISK']['API_KEY']}
            
            #генерируем УРЛ для запроса
            url = config['KINOPOISK']['URL_BASE']+str(id_film)

            # запрашиваем страницу
            logger.info(f"Get data at API KP (film and fistribution) ")
            result = requests.get(url, headers=headers)
            distributions = requests.get(url+'/distributions', headers=headers)
            
            # если получаем ответ с другим кодом
            if result.status_code != 200  or distributions.status_code != 200:
                logger.error(f"API return not 200 code")
                # обновляем статус
                db_conn.update_status_film(film[0],'2')
                logger.info(f"Status film in query updated")
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
                logger.warning(f"URL IMDB different at data in API KP")
                # обновляем статус
                db_conn.update_status_film(film[0],'4')
                logger.info(f"Status film in query updated")
                continue
            elif(data_json['imdbId'] is not None and data_json['imdbId'] == link_imdb):
                logger.info(f"URL IMDB eq data in API KP")
                data_json['link_imdb'] = 'https://www.imdb.com/title/'+str(data_json['imdbId'])

                #получаем дату выхода в прокат
                data_json_distributions = json.loads(distributions.content.decode())
                relise_data = ''
                for array_distrib in data_json_distributions['items']:
                    if(array_distrib['type'] == 'WORLD_PREMIER'):
                        logger.info(f"Found date World Premier")
                        relise_data = array_distrib['date']

                data_json['releaseDate'] = relise_data

                #добавляем инфу
                if(id_in_base is None):
                    logger.info(f"Film added in DB")
                    db_conn.insert_film(data_json['nameEn'], data_json['nameOriginal'], data_json['description'], data_json['releaseDate'], data_json['year'],data_json['posterUrl'],data_json['link_imdb'], film[2], data_json['nameRu'], data_json['imdbId'], data_json['kinopoiskId'])
                else:
                    logger.info(f"Film UPDATED in DB")
                    db_conn.update_film(id_in_base, data_json['nameEn'], data_json['nameOriginal'], data_json['description'], data_json['releaseDate'], data_json['year'],data_json['posterUrl'],data_json['link_imdb'], film[2], data_json['nameRu'], data_json['imdbId'], data_json['kinopoiskId'])
                #обновляем статус
                db_conn.update_status_film(film[0], '1')
                logger.info(f"Status film in query updated")
            else:
                logger.info(f"API KP not return URL film in IMDB")
                #из строчки достаем ИД фильма
                id_film = re.findall("(tt\d{2,})", str(film[1]))[0]

                 # хеад для запроса. Без такого не получим ответ
                headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}
                
                #генерируем УРЛ для запроса
                url = config['IMDB']['URL_BASE']+config['IMDB']['API_KEY']+"/"+str(id_film)

                # запрашиваем страницу
                logger.info(f"Get data at API IMDB")
                result = requests.get(url, headers=headers)
                
                # если получаем ответ с другим кодом
                if result.status_code != 200:
                    logger.error(f"API return not 200 code")
                    # обновляем статус
                    db_conn.update_status_film(film[0],'2')
                    logger.info(f"Status film in query updated")
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
                            logger.info(f"Found date World Premier")
                            relise_data = array_distrib['date']

                    data_json['releaseDate'] = relise_data
                    #добавляем инфу
                    if(id_in_base is None):
                        logger.info(f"Film added in DB")

                        db_conn.insert_film(data_json['nameEn'], data_json['nameOriginal'], data_json['description'], data_json['releaseDate'], data_json['year'],data_json['posterUrl'],film[1], film[2], data_json['nameRu'], data_json['imdbId'], data_json['kinopoiskId'])
                    else:
                        logger.info(f"Film UPDATED in DB")
                        db_conn.update_film(id_in_base, data_json['nameEn'], data_json['nameOriginal'], data_json['description'], data_json['releaseDate'], data_json['year'],data_json['posterUrl'],film[1], film[2], data_json['nameRu'], data_json['imdbId'], data_json['kinopoiskId'])

                    #обновляем статус
                    db_conn.update_status_film(film[0], '1')
                    logger.info(f"Status film in query updated")
                else:
                    logger.error(f"Name or year release dont EQ in KP and IMDB")
                    db_conn.update_status_film(film[0],'4')
                    logger.info(f"Status film in query updated")
                continue

