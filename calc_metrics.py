# -*- coding: utf-8 -*-

import os
import sys

import psycopg2
import configparser
import matplotlib.pyplot as plt


class ExoplanetMetrics(object):
    """
    Класс для расчёта некоторых метрик на основании данных об экзопланетах
    """

    def __init__(self):
        config = configparser.ConfigParser()
        config.read('config.ini')
        self.db_params = config['DEFAULT']['connection_string']

        try:
            self.conn = psycopg2.connect(self.db_params)
        except psycopg2.OperationalError as err:
            print u'Невозможно присоединиться к базе данных: %s' % err.message

    def _get_data_from_db(self, sql):
        """
        Запрос к базе данных
        :param sql: текст запроса
        :return: полученный массив строк
        """
        cur = self.conn.cursor()
        cur.execute(sql)
        return cur.fetchall()

    def _save_plt(self, plt, name):
        """
        Сохранение графика в формате *.png
        :param name: Название файла
        """
        try:
            pwd = os.getcwd()
            try:
                os.mkdir('./pictures/')
            except:
                pass
            os.chdir('./pictures/')
            plt.savefig('%s.%s' % (name, 'png'), fmt='png')
            os.chdir(pwd)
        except:
            print u'При сохранении диаграммы возникла ошибка.'
            raise
        else:
            print u'Диаграмма сохранена по адресу: "./pictures/%s.png"' % name

    def get_some_metrics(self):
        """
        Возвращает строку с информацией о самой плотной звезде, имеющей экзопланету,
        количество планет с нормальной температурой
        и название экзопланеты наиболее похожей на Землю по исследуемым параметрам
        """

        """САМАЯ ПЛОТНАЯ ЗВЕЗДА"""
        rows = self._get_data_from_db("SELECT SUBSTR(kepoi_name, 1, 6)"
                                      "  FROM exoplanets"
                                      " WHERE koi_disposition = 'CONFIRMED'"    # только подтверждённые
                                      "   AND koi_smass IS NOT NULL"
                                      "   AND koi_srad IS NOT NULL"
                                      " ORDER BY (koi_smass/koi_srad) desc"     # масса больше, радиус меньше
                                      " LIMIT 1;")                              # только одну
        densest_star = rows[0][0]

        """ПЛАНЕТЫ С НОРМАЛЬНОЙ ТЕМПЕРАТУРОЙ"""
        rows = self._get_data_from_db("SELECT COUNT(1)"
                                      "  FROM exoplanets"
                                      " WHERE koi_disposition = 'CONFIRMED'"            # только подтверждённые
                                      "   AND koi_teq - 273.15 BETWEEN -20 AND 20;")    # из Кельвинов в Цельсия
        good_weather_count = rows[0][0]

        """САМАЯ ПОХОЖАЯ НА ЗЕМЛЮ"""
        rows = self._get_data_from_db("SELECT kepler_name"
                                      "  FROM exoplanets"
                                      " WHERE koi_disposition = 'CONFIRMED'"        # только подтверждённые
                                      "   AND koi_period IS NOT NULL"
                                      "   AND koi_prad IS NOT NULL"
                                      "   AND koi_teq IS NOT NULL"
                                      " ORDER BY abs(koi_period - 365.2564) / 100"  # разница с земным периодом обращения
                                      "        + abs(koi_prad - 1) "                # разница с земным радиусом
                                      "        + abs(koi_teq - 260)"                # разница со среднеземной температурой
                                      " LIMIT 1;")                                  # только одну
        just_like_earth = rows[0][0]

        return u'Самая плотная звезда имеет название "%s". \n' \
               u'На сегодняшний день экзопланет с приятной температурой найдено %d. \n' \
               u'А больше всего на Землю похожа экзопланета под названием "%s".' \
               % (densest_star, good_weather_count, just_like_earth)

    def do_some_plot_pic(self, show_pic=True):
        """
        Строит двойную диаграмму:
        круговую диаграмму статусов экзопланет
        распределение по времени открытий подтверждённых экзопланет
        :param show_pic: True = Показать диаграмму, False = Сохранить график в виде картинки
        """

        """КРУГОВАЯ ДИАГРАММА СТАТУСОВ ЭКЗОПЛАНЕТ"""
        rows = self._get_data_from_db("SELECT COUNT(1), koi_disposition,"
                                      # для красиво откушенного куска пирога
                                      "       CASE WHEN koi_disposition = 'CONFIRMED' THEN 0.1 ELSE 0 END "
                                      "  FROM exoplanets"
                                      " GROUP BY koi_disposition;")
        plt_data = zip(*rows)
        plt.figure(1)
        plt.subplot(211)
        patches, _, labels = plt.pie(plt_data[0], explode=map(float, plt_data[2]),
                                     autopct='%1.1f%%', startangle=110)
        plt.title("Exoplanet disposition")
        plt.legend(patches, plt_data[1], loc="best", fontsize='medium')
        plt.axis('equal')

        """КОГДА МЫ ИХ НАШЛИ"""
        rows = self._get_data_from_db("SELECT COUNT(1),"        # количество открытых за месяц планет
                                      "       date_trunc('month', dt)::date"
                                      "  FROM"
                                      # превращаем интервал в дату
                                      "    (SELECT Date '2009-01-01' + round(koi_time)::integer as dt"
                                      "       FROM exoplanets) t "
                                      " GROUP BY date_trunc('month', dt) "
                                      "HAVING COUNT(1) > 5"     # не мелочимся, счёт идёт на сотни и тысячи
                                      " ORDER BY 2;")
        plt_data = zip(*rows)
        plt.subplot(212)
        plt.plot(plt_data[1], plt_data[0], label=plt_data[1], alpha=0.75)
        plt.title("Epoch of finding new exoplanets")

        if show_pic:
            plt.show()
        else:
            self._save_plt(plt, 'exoplanets_chart')


if __name__ == "__main__":
    exo = ExoplanetMetrics()

    print exo.get_some_metrics()

    if len(sys.argv) > 1:
        show_pic = sys.argv[1] != 'save'
    else:
        show_pic = True
    exo.do_some_plot_pic(show_pic)