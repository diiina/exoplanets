# -*- coding: utf-8 -*-

import csv
import psycopg2
import configparser


class ExoplanetDataset(object):
    """
    Класс для обработки набора данных с экзопланетами от НАСА:
    exoplanetarchive.ipac.caltech.edu/cgi-bin/TblView/nph-tblView?app=ExoTbls&config=cumulative
    """

    def __init__(self):
        config = configparser.ConfigParser()
        config.read('config.ini')

        self.dataset_filename = config['DEFAULT']['filename']
        self.db_params = config['DEFAULT']['connection_string']

        self.column_order = []
        self.insert_sql = "INSERT INTO exoplanets (id, kepid, kepoi_name, kepler_name, koi_disposition," \
                          "                        koi_period, koi_time, koi_duration, koi_ror, koi_prad," \
                          "                        koi_teq, koi_dor, koi_count, koi_steff, koi_srad, koi_smass)" \
                          "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"

    def _save_the_column_order(self, dict_row):
        """
        Из массива, описывающего порядок столбцов в наборе данных
        собираем массив очерёдности полей, которые нам потребуются
        """
        column_array = ['rowid', 'kepid', 'kepoi_name', 'kepler_name', 'koi_disposition',
                        'koi_period', 'koi_time0bk', 'koi_duration', 'koi_ror', 'koi_prad',
                        'koi_teq', 'koi_dor', 'koi_count', 'koi_steff', 'koi_srad', 'koi_smass']
        try:
            self.column_order = [dict_row.index(column) for column in column_array]
        except:
            print u'Неполный набор данных, некоторых полей не хватает.'
            raise

    def _get_approved_planet(self, planet):
        """
        Извлекаем из сырого массива нужные данные
        :param planet: массив, описывающий экзопланету или порядок колонок
        :return: пусто, если порядок строк или подготовленный массив данных
        """
        if not self.column_order:               # первая строка описывает колонки
            self._save_the_column_order(planet)
            return None

        result = [planet[col] for col in self.column_order]
        for key, value in enumerate(result):
            if not value:
                result[key] = None
        return result

    def _exoplanet_generator(self):
        """
        Генератор, считывающий по одной строке из набора данных.
        Строки комментариев игнорируются, строка с очерёдностью столбцов передаётся на обработку.
        :return: массив данных по одной строке, подготовленный к вставке в БД
        """
        with open(self.dataset_filename, 'rb') as f:
            reader = csv.reader(f)
            for planet in reader:
                if planet[0][0] != '#':   # комментарии пропускаем
                    prepared_planet = self._get_approved_planet(planet)
                    if prepared_planet:   # кроме первой строки
                        yield prepared_planet

    def save_to_db(self):
        """
        Сохраняет извлечённые данные в базу данных.
        """
        try:
            conn = psycopg2.connect(self.db_params)
            cur = conn.cursor()
            row_count = 0
            for planet in self._exoplanet_generator():
                cur.execute(self.insert_sql, planet)
                row_count += 1
            conn.commit()
        except psycopg2.OperationalError as err:
            print u'Невозможно присоединиться к базе данных: %s' % err.message
        except psycopg2.IntegrityError as err:
            print u'При записи в базу данных произошла ошибка нарушения целостности: %s' % err.message
        except psycopg2.DataError as err:
            print u'При записи в базу данных произошла ошибка: %s' % err.message
        except IOError as err:
            print u'Ошибка ввода вывода из набора данных: %s' % err
        else:
            print
            print u'В базу данных успешно добавлено %d записей.' % row_count
        finally:
            if conn: conn.close()


if __name__ == "__main__":
    exo = ExoplanetDataset()
    exo.save_to_db()