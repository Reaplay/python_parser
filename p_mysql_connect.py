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
    def get_film_query(self):
        self.sql_execute("SELECT * FROM `query_add_films` WHERE status = 0 AND (link_imdb IS NOT NULL OR link_kinopoisk IS NOT NULL)")
        return (self.cursor.fetchall())

    def insert_film(self, title_en, originalTitle, description, releaseDate, year, link_imdb = None, link_kinopoisk = None, title_ru = None, id_imdb = None, id_kinopoisk = None):
        sql = ("INSERT INTO films (title_en, title_original,  description, date_release, year_production, link_imdb, link_kinopoisk, title_ru, id_imdb, id_kinopoisk) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)")
        data_add = (title_en, originalTitle, description, releaseDate, year, link_imdb, link_kinopoisk, title_ru, id_imdb, id_kinopoisk)        
        return (self.sql_apply(sql, data_add))

    def search_in_base(self,url, col):
        self.sql_execute("SELECT id FROM `films` WHERE "+str(col)+" = '%s'" % str(url))
        return (self.cursor.fetchone())   

    def update_status(self, id, status):
        sql = ("UPDATE query_add_films SET status = %s, updated_at =  CURRENT_TIMESTAMP 	 WHERE id = %s")
        data_add = (status, id)
        self.sql_apply(sql, data_add)
        return 
