from bs4 import BeautifulSoup
import requests
import re
import datetime

import configparser

#моя библа
import p_mysql_connect
import loggers

#функция времени
def convert_datatime(data):
  data_split = data.split(" ")
  time_split = data_split[4].split(":")

  unit_data = {
      'января': '01',
      'февраля': '02',
      'марта': '03',
      'апреля': '04',
      'майя': '05',
      'июня': '06',
      'тюля': '07',
      'августа': '08',
      'сентября': '09',
      'октября': '10',
      'ноября': '11',
      'декабря': '12',
  }

  result = data_split[0]+'-'+unit_data[data_split[1]]+'-'+data_split[2]+' '+time_split[0]+':'+time_split[1]
  return(result)

def format_data(data):
    now = datetime.datetime.now()
    if data.find("сейчас") >= 0:
        return (now.strftime("%Y-%m-%d %H:%M"))
    elif data.find("сейчас") >= 0:
        time = data.split(" ")
        return (now.strftime("%Y-%m-%d")+' '+time[2])
    elif data.find("сегодня") >= 0:
        time = data.split(" ")
        return (now.strftime("%Y-%m-%d")+' '+time[2])    
    elif data.find("вчера") >= 0:
        time = data.split(" ")
        day = int(now.strftime("%d"))-1
        return (now.strftime("%Y-%m")+'-'+str(day)+' '+time[2])
    else:
        data = data.split(" ")
        return (data[0].replace('.','-')+' '+data[2])

#читаем файл-конфиг 
config = configparser.ConfigParser()
config.read('config.ini')
logger = loggers.get_logger('tracker','kinozal')
#инициализируем класс для работы с БД и подключаемся к ней
db_conn = p_mysql_connect.Database(config['DATABASE'])
session = requests.Session()

array_films = db_conn.select_film()
logger.info(f"Get data from DB")

for data_film in array_films:

    id_film = data_film[0]
    if data_film[1] is None and data_film[2] is None:
        film = data_film[3]
    elif data_film[2] is None:
        film = data_film[1]
    else:
        film = data_film[2]
    year = str(data_film[4])

    logger.info(f"Prepair film ID {data_film[0]} ({film}, {year})")

    #c=1002 - это раздел фильмы
    url = config['KINOZALTV']['URL_SEARCH']+'?s='+film+'&c=1002&d='+year
    
    try:
      rs = session.get(url)
    except Exception:
      logger.error(f"Can't connect to tracker")
      break
    
    if(rs.url.find(config['KINOZALTV']['URL_SEARCH']) == -1):
      logger.error(f"Another URL in responde ({rs.url}). CHECK IT")
      break


    text_html = BeautifulSoup(rs.text, 'lxml') # pars text
    a=str(text_html)
    result_form = text_html.find_all('table',class_='t_peer w100p')
    if(len(result_form) == 0):
      logger.warning(f"Not found table at result. Skip")
      continue
    else:
      result_form = result_form[0]


    array_tr = result_form.find_all('tr')
    i=0
    for text in array_tr:
        #пропуск первой мусорной записи
        if i == 0:
            i=i+1
            continue
        str_text = str(text)
        
        link = re.findall('[^r]details\.php\?id=\d{1,9}', str_text)[0]
        added = re.findall('<td class="s">(.*?)<\/td>', str_text)[2]
        added = format_data(added)
        
        torrent = db_conn.search_torrent(config['KINOZALTV']['URL_BASE'], link, added)
        if (torrent is not None):
            if(torrent[1] == added):
              logger.info(f"Is torrent in DB ({link}). Skip")
            
            else:
                db_conn.update_status_link(torrent[0],'0')
                logger.info(f"Torrent-file ({link}) updated. Change status")
            continue

        #заносим в базу
        db_conn.insert_result_search(id_film, config['KINOZALTV']['URL_BASE'], link)
        logger.info(f"Torrent-link ({link}) add to DB")


array_link = db_conn.select_link(config['KINOZALTV']['URL_BASE'], '0')
logger.info(f"Get torrent-link from DB")

for link in array_link:
  
  try:
    rs = session.post(link[2]+link[3])
  except Exception:
    logger.error(f"Can't get page at tracker. URL: {link[2]}{link[3]}")
    break

  text_html = BeautifulSoup(rs.text, 'lxml')

  post = str(text_html.find_all('div',class_='justify mn2 pad5x5')[0])
  array_data = text_html.find_all('div',class_='mn1_menu')[0]

  if str(array_data).find('Обновлен')>=0:
    time = str(array_data.find_all('span',class_='floatright green n')[2])
  else:
    time = str(array_data.find_all('span',class_='floatright green n')[1])

  # сразу проверяем обновился ли торрент
  registration_torrent = convert_datatime(re.findall('green n">(\d{2}\s.{1,9}\s\d{4}\s.\s\d{2}:\d{2})<\/span>', time)[0])
  if(registration_torrent == link[4]):
    logger.info(f"Data in DB actual. Skip")
    continue

  quality = re.findall('Качество:<\/b>\s?(.*?)<', post)[0]
  translate = re.findall('Перевод:<\/b>\s?(.*?)<', post)[0]
  lang_translate = re.findall('Язык:<\/b>\s?(.*?)<', post)
  if(len(lang_translate) > 0):
    lang_translate = lang_translate[0]
  else:
    lang_translate = ''
    
  subtitles = re.findall('Субтитры:<\/b>\s?(.*?)<', post)[0]
  video_codec = re.findall('Видео:<\/b>\s?(.*?)<', post)[0]
  audio = re.findall('Аудио:<\/b>\s?(.*?)<', post)[0]
 
  db_conn.update_link(str(link[0]),'1', quality, translate, lang_translate, subtitles, video_codec, audio, registration_torrent)
  logger.info(f"Update data in DB. ID link: {link[0]}")
