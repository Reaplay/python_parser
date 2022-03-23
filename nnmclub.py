from bs4 import BeautifulSoup
import requests
import re


import configparser
#моя библа
import p_mysql_connect
import loggers

#функция времени
def convert_datatime(data):
  data_split = data.split(" ")
  time_split = data_split[3].split(":")

  unit_data = {
      'Янв': '01',
      'Фев': '02',
      'Мар': '03',
      'Апр': '04',
      'Май': '05',
      'Июн': '06',
      'Июл': '07',
      'Авг': '08',
      'Сен': '09',
      'Окт': '10',
      'Ноя': '11',
      'Дек': '12',
  }

  result = data_split[0]+'-'+unit_data[data_split[1]]+'-'+data_split[2]+' '+time_split[0]+':'+time_split[1]
  return(result)

#читаем файл-конфиг 
config = configparser.ConfigParser()
config.read('config.ini')
logger = loggers.get_logger('main')
#инициализируем класс для работы с БД и подключаемся к ней
db_conn = p_mysql_connect.Database(config['DATABASE'])
session = requests.Session()


array_films = db_conn.select_film()
logger.info(f"Взяли данные из БД")
for data_film in array_films:
  
  id_film = data_film[0]
  if data_film[1] is None and data_film[2] is None:
    film = data_film[3]
  elif data_film[2] is None:
    film = data_film[1]
  else:
    film = data_film[2]
  
  year = str(data_film[4])

  logger.info(f"Идем по фильму с ID {data_film[0]} ({film}, {year})")
  

  data = {    
    'prev_sd': '0',
    'prev_a': '0',
    'prev_my': '0',
    'prev_n': '0',
    'prev_shc': '0',
    'prev_shf': '1',
    'prev_sha': '1',
    'prev_shs':'0',
    'prev_shr':'0',
    'prev_sht':'0',
    'f[]': {'270', '218', '219', '954','232'},
    'o': '1',
    's': '2',
    'tm': '-1',
    'shf': '1',
    'sha': '1',
    'ta': '1',
    'sns': '-1',
    'sds': '-1',
    'nm': film+' '+year,
    'submit': 'Поиск'
  }

  try:
    rs = session.post(config['NNMCLUB']['URL_SEARCH'],data=data)
  except Exception:
    logger.error(f"Проблема с подключением к хосту. Прерываем работу")
    break
  
  text_html = BeautifulSoup(rs.text, 'lxml') # pars text
  result_form = text_html.find_all('table',class_='forumline tablesorter')[0]
  tbody = result_form.find('tbody') #vможно было tbody прописать text_html, результат тот же. Но на всякий пока так
  array_tr = tbody.find_all('tr')

  # вращаем результат и только первую страницу

  for text in array_tr:
    str_text = str(text)
    section = re.findall(year+"\">(.*?)<\/a", str_text)[0]
    link = re.findall('topictitle" href="(.*?)"', str_text)[0]
    added = re.findall('(\d{2}-\d{2}-\d{4})<br\/>(\d{2}:\d{2})', str_text)[0]
    added = added[0]+' '+added[1]
  #  quality = re.findall(year+'\) (.*?)<\/b>', str_text)
  #  translate = re.findall('gensmall opened">(.*?)<\/span>', str_text)
    
    #проверяем, есть ли такое уже в базе
    torrent = db_conn.search_torrent(config['NNMCLUB']['URL_BASE'], link, added)
    if (torrent is not None):
      if(torrent[1] == added):
        print ('dublicate ' + link)
        logger.info(f"Дубликат. Торрент совпадает")
      else:
        db_conn.update_status_link(torrent[0],'0')
        print ('update status ' + link)
        logger.info(f"Раздача изменилась, праоставляем статус")
      continue

    #заносим в базу
    db_conn.insert_result_search(id_film, config['NNMCLUB']['URL_BASE'], link, section)
    logger.info(f"Добавляем в базу")
    print ('add to BD ' + link)


array_link = db_conn.select_link(config['NNMCLUB']['URL_BASE'], '0')
for link in array_link:
  
  rs = session.post(link[2]+link[3])
  text_html = BeautifulSoup(rs.text, 'lxml')

  search_link = re.findall('>(.*?)<',str(text_html.find_all('a',class_='nav')[2]))[0]
  if(search_link != link[5]):
    db_conn.update_status_link(link[0],'3')
    continue
  
  text_topic = text_html.find_all('table',class_='forumline')[0]
  post = str(text_topic.find_all('tr')[1])

  # сразу проверяем обновился ли торрент
  registration_torrent = convert_datatime(re.findall('<td class="genmed">[^.](.*?)<\/td>', post)[1])
  if(registration_torrent == link[4]):
    print ('actual torrent')
    continue

  quality = re.findall('Качество видео:<\/span> (.*?)<', post)[0]
  translate = re.findall('Перевод:<\/span> (.*?)<', post)[0]
  lang_translate = re.findall('Язык озвучки:<\/span> (.*?)<', post)
  if(len(lang_translate) > 0):
    lang_translate = lang_translate[0]
  else:
    lang_translate = ''
  subtitles = re.findall('Субтитры:<\/span> (.*?)<', post)[0]
  video_codec = re.findall('Видео:<\/span> (.*?)<', post)[0]
  raw_audio = re.findall('Аудио \d?:?<\/span>:? (.*?)<', post.replace('»','').replace('#',''))
  if len(raw_audio) == 0:
    audio = re.findall('Аудио:<\/span> (.*?)<', post)[0]
  else:
    audio = ''
    i = 0 
    for str_audio in raw_audio:
      i = i+1
      audio = audio+'Аудио '+str(i)+': ' + str(str_audio)+' <br> '

  db_conn.update_link(str(link[0]),'1', quality, translate, lang_translate, subtitles, video_codec, audio, registration_torrent)
  print ('update torrent')
