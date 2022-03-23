import mysql.connector
from mysql.connector import errorcode
import time
class Database():
    
    # create construct
    def __init__(self, config):
       self._conn=mysql.connector.connect(host=config['HOST'], user=config['USERNAME'], password=config['PASSWORD'], database=config['DATABASE'],charset='utf8mb4')
       self._cursor =  self._conn.cursor()
       
# var
    @property
    def connection(self):
        return self._conn

    @property
    def cursor(self):
        return self._cursor
        
#магия исполнения запроса. Не трогать, работает
    
    
    def close(self):       
        return (self.cursor.close())

    def sql_apply(self, sql, data_add):
        self.sql_execute(sql, data_add)
        self.commit()
        return (self.cursor.lastrowid)

    def sql_execute(self, query,data = None):
        try:
            a = self.cursor.execute(query,data)
        except mysql.connector.Error as e:
            s = str(e)
            print ("Error:", s)
        return (a)
    
    def commit(self):
        self.connection.commit()
    

#sql query
#API выборка очереди
    def get_film_query(self):
        self.sql_execute("SELECT * FROM `query_add_films` WHERE status = 0 AND (link_imdb IS NOT NULL OR link_kinopoisk IS NOT NULL)")
        return (self.cursor.fetchall())
#API вставка фильмов в таблицу
    def insert_film(self, title_en, originalTitle, description, releaseDate, year, link_imdb = None, link_kinopoisk = None, title_ru = None, id_imdb = None, id_kinopoisk = None):
        sql = ("INSERT INTO films (title_en, title_original,  description, date_release, year_production, link_imdb, link_kinopoisk, title_ru, id_imdb, id_kinopoisk) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)")
        data_add = (title_en, originalTitle, description, releaseDate, year, link_imdb, link_kinopoisk, title_ru, id_imdb, id_kinopoisk)        
        return (self.sql_apply(sql, data_add))
#API поиск в базе фильмов по полю
    def search_in_base(self,url, col):
        self.sql_execute("SELECT id FROM `films` WHERE "+str(col)+" = '%s'" % str(url))
        return (self.cursor.fetchone())   
#API обновление статуса очереди
    def update_status_film(self, id, status):
        sql = ("UPDATE query_add_films SET status = %s, updated_at =  CURRENT_TIMESTAMP  WHERE id = %s")
        data_add = (status, id)
        self.sql_apply(sql, data_add)
        return
#API обновление данных фильма        
    def update_film(self, id_film, title_en, originalTitle, description, releaseDate, year, link_imdb = None, link_kinopoisk = None, title_ru = None, id_imdb = None, id_kinopoisk = None):
        sql = ("UPDATE films SET title_en = %s, title_original = %s,  description = %s, date_release = %s, year_production = %s, link_imdb = %s, link_kinopoisk = %s, title_ru = %s, id_imdb = %s, id_kinopoisk = %s, updated_at =  CURRENT_TIMESTAMP WHERE id = %s")
        data_add = (title_en, originalTitle, description, releaseDate, year, link_imdb, link_kinopoisk, title_ru, id_imdb, id_kinopoisk, id_film)   
        self.sql_apply(sql, data_add)
#API обновление даты у фильма    
    def update_url(self, id_film, col, url):
        sql = ("UPDATE films SET "+str(col)+" = %s, updated_at =  CURRENT_TIMESTAMP WHERE id = %s")
        data_add = (url, id_film)
        self.sql_apply(sql, data_add)
#TORRENT выборка фильмов для поиска
    def select_film(self):
        self.sql_execute("SELECT id, title_en, title_original, title_ru, year_production, date_release  FROM `films` WHERE DATEDIFF (CURRENT_DATE(), date_release) <= 180" )
        return (self.cursor.fetchall())
#TORRENT вставка результата поиска
    def insert_result_search(self, id_film, domain, link_text, section = None):
        sql = ("INSERT INTO film_links (id_film, domain, link_text, section) VALUES (%s,%s,%s,%s)")
        data_add = (id_film, domain, link_text, section)        
        return (self.sql_apply(sql, data_add))
#TORRENT поиска на уже добавленные
    def search_torrent(self, domain, link_text, registration_torrent ):
        self.sql_execute("SELECT id, registration_torrent FROM `film_links` WHERE domain = '"+str(domain)+"' AND link_text = '"+link_text+"'" )
        return (self.cursor.fetchone())
#TORRENT выборка линков 
    def select_link(self, domain, status):
        self.sql_execute("SELECT id, id_film, domain, link_text, registration_torrent, section FROM `film_links` WHERE domain = '"+str(domain)+"' AND status = '"+status+"'" )
        return (self.cursor.fetchall())
#TORRENT обновление статуса
    def update_status_link(self, id, status):
        sql = ("UPDATE film_links SET status = %s, deleted_at =  CURRENT_TIMESTAMP 	 WHERE id = %s")
        data_add = (status, id)
        self.sql_apply(sql, data_add)
        return
#TORRENT обновление линка
    def update_link(self, id, status, quality, translate, lang_translate, subtitles, video_codec, audio, registration_torrent):
        sql = ("UPDATE film_links SET status = %s, quality = %s, translate = %s, lang_translate = %s, subtitles = %s, video_codec = %s, audio = %s, registration_torrent = %s, updated_at =  CURRENT_TIMESTAMP  WHERE id = %s")
        data_add = (status, quality, translate, lang_translate, subtitles, video_codec, audio, registration_torrent, id)
        self.sql_apply(sql, data_add)
        return
