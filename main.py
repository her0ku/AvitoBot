import os
from datetime import datetime, timezone
import time
import telebot
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
import csv
import requests
import statistics
import psycopg2
from psycopg2 import Error

bot = telebot.TeleBot(token='')
fresh_list = ['Несколько секунд назад', '1 минуту назад', '2 минуты назад', '3 минуты назад', '4 минуты назад',
              '5 минут назад', '6 минут назад', '7 минут назад', '8 минут назад', '9 минут назад', '10 минут назад',
              '11 минут назад', '12 минут назад', '13 минут назад', '14 минут назад', '15 минут назад',
              '18 минут назад', '20 минут назад', '30 минут назад', '1 час назад']

popular_req = ['iphone', 'bmw', 'audi', 'samsung', 'ear pods', 'ftp', 'toyota', 'automobili', 'imac', 'macbook',
               'xiaomi',
               'huawei', 'sony', 'lg', 'google', 'ford']

cards_list = set()
data_set = {}
state_flag = True
low_item = []
max_item = []
avg_price = []
my_conn = psycopg2.connect(user="postgres",
                           password="root",
                           host="127.0.0.1",
                           port="5432",
                           database="avito_stats_db")

def check_connect():
    try:
        connection = my_conn

        cursor = connection.cursor()
        return True
    except (Exception, Error) as error:
        print("Ошибка при работе с PostgreSQL", error)
        return False
    finally:
        if connection:
            cursor.close()
            print("Соединение с PostgreSQL закрыто")


def insert_result(popular):
    try:
        connection = my_conn

        cursor = connection.cursor()
        postgres_insert_query = """ INSERT INTO avito_statistic (POPULAR_ITEM, DATA)
                                           VALUES (%s, %s)"""
        current_date = datetime.today()
        record_to_insert = (popular, current_date)
        cursor.execute(postgres_insert_query, record_to_insert)
        print(cursor)

        connection.commit()

    except (Exception, Error) as error:
        print("Ошибка при работе с PostgreSQL", error)
    finally:
        if connection:
            cursor.close()
            print("Соединение с PostgreSQL закрыто")


def find_popular_title():
    try:
        connection = my_conn

        cursor = connection.cursor()
        postgres_answ = """ SELECT avito_statistic.popular_item FROM avito_statistic ORDER BY avito_statistic.popular_item DESC LIMIT 1"""
        cursor.execute(postgres_answ)
        record = cursor.fetchall()
        connection.commit()
        cursor.close()
        return record

    except (Exception, Error) as error:
        print("Ошибка при работе с PostgreSQL", error)
        return 'Ошибка('
    finally:
        if connection:
            cursor.close()
            print("Соединение с PostgreSQL закрыто")


def parser(url, chat_id, flag, state, site):
    low_price = 999999999
    max_price = 0
    bs = BeautifulSoup(url.text, 'html.parser')
    cards = bs.find_all('div', class_='iva-item-content-UnQQ4')
    for card in cards:
        items = card.find_all('div', class_='iva-item-body-R_Q9c')
        for item in items:
            links = item.find_all('a', class_='link-link-MbQDP link-design-default-_nSbv '
                                              'title-root-j7cja iva-item-title-_qCwt title-listRedesign-XHq38'
                                              ' title-root_maxHeight-SXHes')
            titles = item.find_all('h3', class_='title-root-j7cja iva-item-title-_qCwt title-listRedesign-XHq38 '
                                                'title-root_maxHeight-SXHes '
                                                'text-text-LurtD text-size-s-BxGpL text-bold-SinUO')
            prices = item.find_all('span', class_='price-text-E1Y7h text-text-LurtD text-size-s-BxGpL')
            dates = item.find_all('div', class_='date-text-VwmJG text-text-LurtD '
                                                'text-size-s-BxGpL text-color-noaccent-P1Rfs')
            for title in titles:
                if 'avtomobili' in site:
                    name = title.text.split(',')[0]
                    year = title.text.split(',')[1]
                else:
                    name = title.text
                    year = '0'
            for link in links:
                link = link['href']
                if link not in cards_list:
                    cards_list.add(link)
            for price in prices:
                price = price.text
            for data in dates:
                data = data.text
            if link in cards_list:
                if flag:
                    answer = data_validation(data)
                    if answer:
                        print_card(chat_id, name, year, price, data, link)
                else:
                    print_card(chat_id, name, year, price, data, link)
            sub_price = price.replace('₽', '')
            sub_price = sub_price.replace('от','')
            sub_price = ''.join(sub_price.split())
            if low_price > int(sub_price):
                low_price = int(sub_price)
                low_item.clear()
                low_item.extend((name, year, str(low_price), data, 'https://www.avito.ru' + link))
            elif max_price < int(sub_price):
                max_price = int(sub_price)
                max_item.clear()
                max_item.extend((name, year, str(max_price), data, 'https://www.avito.ru' + link))
            data_set.update({'name': name, 'year': year, 'price': price,
                             'data': data, 'link': link})
            avg_price.append(int(sub_price))
        if state:
            write_in_file(data_set)
        if not check_flag():
            break
        time.sleep(5)


def write_in_file(data_set):
    with open('myData.csv', 'a', encoding='utf-8') as csv_file:
        writer = csv.DictWriter(csv_file, data_set.keys())
        writer.writeheader()
        writer.writerow(data_set)
    csv_file.close()


def data_validation(data):
    print(data)
    if data in fresh_list:
        return True
    else:
        return False


def print_card(chat_id, name, year, price, data, link):
    bot.send_message(chat_id,
                     text='Марка --> ' + '<b>' + name + '</b>' + '\nГод --> ' + '<b>' + year + '</b>'
                          + '\nЦена --> ' + '<b>' + price + '</b>' +
                          '\nДата публикации --> ' + '<b>' + data + '</b>'
                                                                    '\nССЫЛКА НА АВИТО --> ' + 'https://www.avito.ru/' + link,
                     parse_mode='html')


@bot.message_handler(commands=['start'])
def start_message(message):
    bot.send_message(message.chat.id, '/help - узнаешь больше информации'
                                      '\n/nav - навикация по боту')
    keyboard = telebot.types.ReplyKeyboardMarkup(True)
    keyboard.row('Быстрый поиск', 'Вставить свою ссылку', 'Отсановить поиск', 'Навигация по боту')
    bot.send_message(message.chat.id, 'Можете выбрать команду из списка или нажать на кнопку!', reply_markup=keyboard)
    check_status_base = check_connect()
    if check_status_base:
        bot.send_message(message.chat.id, 'База данных работает! Можно выполнять запросы на поиск популярного '
                                          'товара для парсинга /popular!')
    else:
        bot.send_message(message.chat.id, 'База данных временно работает!')


@bot.message_handler(commands=['help'])
def help_message(message):
    bot.send_message(message.chat.id, '/start - проверяет подключение к базе для проверки популярных запросов'
                                      '\n/parse и вы перейдете в парсинг,'
                                      ' для парсинга указываете ссылку как обычную так и с фильтрами.'
                                      '\nПример ссылки --> https://www.avito.ru/moskva_i_mo/avtomobili, '
                                      'необходимо убрать ?p= при наличии.'
                                      ' После чего необходимо указать количество страниц до 100.'
                                      ' Иначе авито упадет!'
                                      ' Также можно искать прямо в самом боте! /search')


@bot.message_handler(content_types=['text'])
def text_message(message):
    get_message_user = message.text.strip().lower()
    if get_message_user == 'быстрый поиск' or message.text == '/search':
        search_text(message)
    elif get_message_user == 'вставить свою ссылку' or message.text == '/parse':
        text_send(message)
    elif get_message_user == 'отсановить поиск' or message.text == '/stop':
        stop_message(message)
    elif get_message_user == '/low':
        low_message(message)
    elif get_message_user == '/max':
        max_message(message)
    elif get_message_user == 'навигация по боту' or get_message_user == '/nav':
        nav_message(message)
    elif get_message_user == '/get':
        get_message(message)
    elif get_message_user == '/start':
        start_message(message)
    elif get_message_user == '/avg':
        avg_message(message)
    elif get_message_user == '/popular':
        popular_message(message)


@bot.message_handler(commands=['stop'])
def stop_message(message):
    bot.send_message(message.chat.id, 'Работа парсера прекращена!')
    if os.path.exists('myData.csv'):
        bot.send_document(message.chat.id, open(r'myData.csv', 'rb'))
        os.remove('myData.csv')
    global state_flag
    state_flag = False
    data_set.clear()
    cards_list.clear()


@bot.message_handler(commands=['low'])
def low_message(message):
    if not low_item:
        bot.send_message(message.chat.id, 'Товара не существует, видимо вы еще не парсили объекты!')
    else:
        bot.send_message(message.chat.id, 'Товар с минимальной ценой ->\n' + str(low_item))


@bot.message_handler(commands=['max'])
def max_message(message):
    if not max_item:
        bot.send_message(message.chat.id, 'Товара не существует, видимо вы еще не парсили объекты!')
    else:
        bot.send_message(message.chat.id, 'Товар с макисмальной ценой ->\n' + str(max_item))


@bot.message_handler(commands=['avg'])
def avg_message(message):
    if not avg_price:
        bot.send_message(message.chat.id, 'Товара не существует, видимо вы еще не парсили объекты!')
    else:
        avg = float('{:.3f}'.format(statistics.mean(avg_price)))
        bot.send_message(message.chat.id, 'Средняя цена на данный товар: ' + str(avg))


@bot.message_handler(commands=['popular'])
def popular_message(message):
    answer = find_popular_title()
    bot.send_message(message.chat.id, 'самый популярный товар для парсинга! -> ' + str(answer[0]))


@bot.message_handler(commands=['nav'])
def nav_message(message):
    bot.send_message(message.chat.id, '\n/start - запуск бота'
                                      '\n/help - посмотреть справку на использование бота'
                                      '\n/search - поиск товара внутри бота.'
                                      '\n/parse - парсить страницы по ссылке'
                                      '\n/get - получить csv документ'
                                      '\n/stop - остановить бота'
                                      '\n/low - вывести минимальную цену товара'
                                      '\n/max - вывести мксимальную цену товара'
                                      '\n/avg - средняя цена за товар'
                                      '\n/popular - вывести самый популярный товар, который парсят')


@bot.message_handler(commands=['get'])
def get_message(message):
    if os.path.exists('myData.csv'):
        bot.send_document(message.chat.id, open('myData.csv', 'rb'))
    else:
        bot.send_message(message.chat.id, 'Вы не парсили данные, файла не существует!')


@bot.message_handler(commands=['parse'])
def text_send(message):
    msg = bot.send_message(message.chat.id, 'Введите ссылку на avito, без m., если используюте мобильную версию сайта.'
                                            '\nПример ссылки --> https://www.avito.ru/moskva_i_mo/avtomobili'
                                            '\nПисать чистую ссылку не указывая номер станицы')
    bot.register_next_step_handler(msg, site_step)


@bot.message_handler(commands=['search'])
def search_text(message):
    msg = bot.send_message(message.chat.id, 'Для быстрого поиска простых товров: телефоны, нотбуки и т.д '
                                            ' (не осуществляется быстрый поиск машин).'
                                            '\nНапишите название вашего города '
                                            'на латинице с маленьких букв.'
                                            'Например: ' + '*' + ' samara' + '*', parse_mode='markdown')
    bot.register_next_step_handler(msg, fast_site_step)


def fast_site_step(message):
    city = message.text
    city_site = 'https://www.avito.ru/' + city
    msg = bot.send_message(message.chat.id, 'Теперь напишите название товара на латинице с маленьких букв. '
                                            ' Например:' + '*' + ' iphone' + '*', parse_mode='markdown')
    bot.register_next_step_handler(msg, next_step_fast_site, city_site)


def next_step_fast_site(message, city_site):
    product = message.text
    city_site += '?q=' + product
    request = requests.get(city_site)
    if request.status_code != 404:
        site = city_site
        msg = bot.send_message(message.chat.id, 'Введите количество страниц для парсинга....')
        bot.register_next_step_handler(msg, num_step, site)
    else:
        bot.send_message(message.chat.id, 'ССЫЛКА НЕ ВАЛИДНА!'
                                          ' Введите /parse, чтобы попробовать снова!')


def check_flag():
    return state_flag


def site_step(message):
    bot.send_message(message.chat.id, 'Проверяем валидность ссылки....')
    time.sleep(4)
    site = message.text
    if 'https://www.avito.ru/' in site:
        request = requests.get(site)
        if request.status_code != 404:
            bot.send_message(message.chat.id, 'ССЫЛКА ВАЛИДНА!')
            msg = bot.send_message(message.chat.id, 'Введите число для парсинга страниц, желательно до 100 стр. '
                                                    ' А то авито сломается :с')
            bot.register_next_step_handler(msg, num_step, site)
        else:
            bot.send_message(message.chat.id, 'ССЫЛКА НЕ ВАЛИДНА!'
                                              ' Введите /parse, чтобы попробовать снова!')
    else:
        bot.send_message(message.chat.id, 'Вы указали неправильную ссылку!'
                                          ' Введите /parse, чтобы попробовать снова!')


def num_step(message, site):
    num = message.text
    if not num.isdigit() or int(num) <= 0 or int(num) >= 101:
        bot.send_message(message.chat.id, 'Вы ввели не правильно число /parse - чтобы попробоовать еще раз!')
    else:
        fake_agent = UserAgent()
        header = {'User-Agent': str(fake_agent.chrome)}
        bot.send_message(message.chat.id, 'Всё супер, пока что!')
        msg = bot.send_message(message.chat.id, 'Хотите получать только свежие объявления? \nВведите символ (не emoji)'
                                                '\n➕    -> да '
                                                '\n➖    -> нет')
        bot.register_next_step_handler(msg, check_fresh_card, num, site, header)


def check_fresh_card(message, num, site, header):
    symbol = message.text
    if symbol == '+':
        flag = True
    else:
        flag = False
    msg = bot.send_message(message.chat.id, 'Хотите сохранить свои данные в csv файл? \nВведите символ (не emoji)'
                                            '\n➕    -> да '
                                            '\n➖    -> нет')
    bot.register_next_step_handler(msg, write_to_file, num, site, header, flag)


def write_to_file(message, num, site, header, flag):
    state = message.text
    global state_flag
    state_flag = True
    if state == '+':
        state = True
    else:
        state = False
    for popular in popular_req:
        if popular in site:
            insert_result(popular)
    for i in range(int(num)):
        if 'f=' in site or '?s=' in site or '?q=' in site:
            site = site + '&p=' + str(i)
        else:
            site = site + '?p=' + str(i)
        request = requests.get(site, headers=header)
        if request.status_code != 404:
            parser(request, message.chat.id, flag, state, site)
        else:
            bot.send_message(message.chat.id, 'Ссылка не действительна или не существует!'
                                              'Напишите /parse, чтобы попробовать еще раз')


bot.polling()
