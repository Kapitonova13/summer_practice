import psycopg2
import requests
import telebot
from apscheduler.schedulers.background import BackgroundScheduler
import os

DATABASE_URL = os.environ.get('DATABASE_URL')
if not DATABASE_URL:
    raise ValueError("DATABASE_URL environment variable is not set")
conn = psycopg2.connect(DATABASE_URL)

print("Успешное подключение")

cur = conn.cursor()
cur.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'vacancies');")
vacancies_exists = cur.fetchone()[0]
# print(vacancies_exists)

cur.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'cities');")
cities_exists = cur.fetchone()[0]

if not vacancies_exists:
    cur.execute("CREATE TABLE vacancies (id SERIAL PRIMARY KEY NOT NULL, position VARCHAR(100), salary_from INTEGER, experience VARCHAR(50), query VARCHAR(50), salary_to INTEGER, id_v INTEGER, city VARCHAR(50), currency VARCHAR(50));")
    conn.commit()
    print("vacancies")
else:
    print("Таблица vacancies существуют, создание не требуется.")
if not cities_exists:
    cur.execute("CREATE TABLE cities (id SERIAL PRIMARY KEY NOT NULL, city VARCHAR(50));")
    conn.commit()
    cur.execute("INSERT INTO cities (id, city) VALUES (1, 'Москва'), (2, 'Санкт-Петербург'), (3, 'Екатеринбург'), (4, 'Новосибирск'), (5, 'Казань');")
    conn.commit()  
    print("cities")
else:
    print("Таблица cities существуют, создание не требуется.")


BOT_TOKEN = "7420447400:AAH1Me8HXvH3IMAu9sejrSfVbd77LtbN7Bc"
bot = telebot.TeleBot(BOT_TOKEN)

@bot.message_handler(commands=['start'])
def main(message):
    bot.send_message(message.chat.id, 'Привет! Я бот для поиска вакансий на HeadHunter.\n\nПропишите команду /zs, чтобы узнать, как составить запрос для получения данных.\n\nПосле получения данных можно будет воспользоваться фильтрацией. Информацию по ней можно получить по команде  /filter.\n\nПросмотреть все полученные данные из базы данных можно по команде /all.', parse_mode='html')

@bot.message_handler(commands=['zs']) #создание запрос для парсинга
def handle_zapros(message):
        parts = message.text.split(" ")
        if len(parts) == 1:
            bot.send_message(message.chat.id, "<em><u>Запрос для получения данных.</u></em>\n\nВведите запрос следующим образом, соблюдая последовательность параметров:\n<b>/zs должность зарплата(от) город(России) кол-во вакансий(доп. параметр(до 100), по умолчанию - 10)</b>\n\n<em>Доступные города:</em>\nМосква\nСанкт-Петербург\nКазань\nНовосибирск\nЕкатеринбург\n\n<em>Пример:</em>\n /zs учитель 60000 Москва 30\n\nЕсли какой-то из параметров не нужен(зарплата, город), поставьте на месте этого параметра '-'. \nНО! параметр(должность) - обязательный! \nПример: /zs тестировщик - Москва\n\n<em>Ещё примеры:</em>\n/zs frontend\n/zs бэкенд 100000\n/zs продавец - - 25", parse_mode='html')
        try:
            if len(parts) > 1:
                query = parts[1] 
                url = f"https://api.hh.ru/vacancies?&only_with_salary=true&per_page=10&text={query}&search_field=name&area=113"
            if len(parts) > 2:
                salary = parts[2]
                if salary != "-":
                    url = f"https://api.hh.ru/vacancies?salary={salary}&only_with_salary=true&per_page=10&text={query}&search_field=name&area=113"
                    if len(parts) > 3:
                        city = parts[3]
                        if city != "-":
                            cur.execute("SELECT id FROM cities WHERE city=%s", (str(city),))
                            c = list(cur.fetchall())
                            if c:  # список не пустой
                                id = c[0][0]  
                                url = f"https://api.hh.ru/vacancies?salary={salary}&only_with_salary=true&per_page=10&text={query}&search_field=name&area={id}"
                            else:
                                url = ""
                            if len(parts) > 4: 
                                k = parts[4]
                                url = f"https://api.hh.ru/vacancies?salary={salary}&only_with_salary=true&per_page={k}&text={query}&search_field=name&area={id}"
                        else: #пропуск города
                            if len(parts) > 4:
                                k = parts[4]
                                url = f"https://api.hh.ru/vacancies?salary={salary}&only_with_salary=true&per_page={k}&text={query}&search_field=name&area=113"
                else:#пропуск зарплаты
                    if len(parts) > 3:
                        city = parts[3]
                        if city != "-":
                            cur.execute("SELECT id FROM cities WHERE city=%s", (str(city),))
                            c = list(cur.fetchall())
                            if c: 
                                id = c[0][0]  
                                print(id)  
                                url = f"https://api.hh.ru/vacancies?only_with_salary=true&per_page=10&text={query}&search_field=name&area={id}"
                            else:
                                url = ""
                            if len(parts) > 4: 
                                k = parts[4]
                                url = f"https://api.hh.ru/vacancies?only_with_salary=true&per_page={k}&text={query}&search_field=name&area={id}"
                        else: #пропуск города
                            if len(parts) > 4: 
                                k = parts[4]
                                url = f"https://api.hh.ru/vacancies?only_with_salary=true&per_page={k}&text={query}&search_field=name&area=113"
        
        except Exception as e:
            bot.send_message(message.chat.id, "Неверно прописан запрос!\nПопробуйте еще раз.\nВведите /zs для получения инструкции по созданию запроса.")
            print("Ошибка:", e)            
        print(url)
        if url == "":
            bot.send_message(message.chat.id, "Что-то не так!\nПроверьте правильность введённых данных и пробелы между параметрами!")

        response = requests.get(url) # отправка запроса

        if response.status_code == 200:
            data = response.json()
            vacancies = data["items"]
            print(data)
            if vacancies:
                cur.execute("SELECT * FROM vacancies WHERE query = %s", (query,))
                existing_vacancies = cur.fetchall()
                # print(existing_vacancies)

                # сравнение и обновление данных
                for vacancy in vacancies:
                    found_vacancy = None 
                    for existing_vacancy in existing_vacancies:
                        if str(existing_vacancy[6]) == str(vacancy['id']):
                            found_vacancy = existing_vacancy
                            break 

                    # print(found_vacancy)
                    if found_vacancy:                        
                        if (
                            str(found_vacancy[1]) != str(vacancy['name'])
                            or str(found_vacancy[2]) != str(vacancy['salary']['from'])
                            or str(found_vacancy[5]) != str(vacancy['salary']['to'])
                            or str(found_vacancy[3]) != str(vacancy['experience']['name'])
                            or str(found_vacancy[7]) != str(vacancy['area']['name'])
                            or str(found_vacancy[8]) != str(vacancy['salary']['currency'])
                        ): # обновление данных, если они изменились
                            cur.execute("""UPDATE vacancies SET position = %s, salary_from = %s, salary_to = %s, experience = %s, city = %s, currency = %s  WHERE id_v = %s""", (vacancy['name'], vacancy['salary']['from'], vacancy['salary']['to'], vacancy['experience']['name'], vacancy['area']['name'], vacancy['salary']['currency'], found_vacancy[6]))
                            conn.commit()
                            print(f"Вакансия {vacancy['name']} обновлена.")
                        else:
                            print("Такая запись уже существует.")

                    else:
                        try:
                            if vacancy['salary']['from'] and vacancy['salary']['to'] is not None:
                                cur.execute("INSERT INTO vacancies (position, salary_from, experience, query, salary_to, id_v, city, currency) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)", (str(vacancy['name']), int(vacancy['salary']['from']), str(vacancy['experience']['name']), str(query), int(vacancy['salary']['to']), str(vacancy['id']), str(vacancy['area']['name']), str(vacancy['salary']['currency'])))              
                                conn.commit()
                                print("Данные успешно сохранены!")
                            elif vacancy['salary']['from'] is not None:
                                cur.execute("INSERT INTO vacancies (position, salary_from, experience, query, id_v, city, currency) VALUES (%s, %s, %s, %s, %s, %s, %s)", (str(vacancy['name']), int(vacancy['salary']['from']), str(vacancy['experience']['name']), str(query), str(vacancy['id']), str(vacancy['area']['name']), str(vacancy['salary']['currency'])))             
                                conn.commit()
                                print("Данные успешно сохранены!")
                            else: 
                                cur.execute("INSERT INTO vacancies (position, experience, query, salary_to, id_v, city, currency) VALUES (%s, %s, %s, %s, %s, %s, %s)", (str(vacancy['name']), str(vacancy['experience']['name']), str(query), int(vacancy['salary']['to']), str(vacancy['id']), str(vacancy['area']['name']), str(vacancy['salary']['currency'])))             
                                conn.commit()
                                print("Данные успешно сохранены!")
                        except Exception as e:
                            print("Ошибка при сохранении данных:", e)            
                try:
                    cur.execute("SELECT position, salary_from, salary_to, experience, city, currency FROM vacancies WHERE query=%s", (str(query),)) #вывод сразу после запроса
                    d = list(cur.fetchall())
                    if d:
                        mes = ""
                        for position, salary_from, salary_to, experience, city, currency in d:
                            if currency == "RUR":
                                currency = "₽"
                            elif currency == "USD":
                                currency = "$"
                            if salary_from is None:
                                mes += f"Должность: {position}\nЗарплата: до {salary_to} {currency}\nОпыт работы: {experience}\nГород: {city}\n-------------\n"
                                bot.send_message(message.chat.id, mes)
                                mes = ""
                            elif salary_to is None:
                                mes += f"Должность: {position}\nЗарплата: от {salary_from} {currency}\nОпыт работы: {experience}\nГород: {city}\n-------------\n"
                                bot.send_message(message.chat.id, mes)
                                mes = ""
                            else:
                                mes += f"Должность: {position}\nЗарплата: {salary_from} - {salary_to}  {currency}\nОпыт работы: {experience}\nГород: {city}\n-------------\n"
                                bot.send_message(message.chat.id, mes)
                                mes = ""
                    bot.send_message(message.chat.id, "Данные получены. Введите /all, чтобы просмотреть все данные из базы данных по всем запросам.\nЛибо получите только нужные данные по фильтрам.\nУзнать больше по команде /filter")
                except Exception as e:
                        print("Ошибка:", e)
            else:
                bot.send_message(message.chat.id, "Ничего не найдено!\nПопробуйте еще раз.\nВведите /zs для получения инструкции по созданию запроса.")
  
        else:
            bot.send_message(message.chat.id, "Ошибка в запросе!\nПопробуйте еще раз.\nВведите /zs для получения инструкции по созданию запроса.")
            print(f"Ошибка запроса: {response.status_code}")

@bot.message_handler(commands=['filter']) # фильтры
def filter(message):

    text = ""
    words = message.text.split(" ") 
    try:
        if len(words) == 1:
            bot.send_message(message.chat.id, "<em><u>Фильтры по полученным данным.</u></em>\n\nДля этого необходимо прописать следующие параметры, соблюдая последовательность:\n<b>/filter должность(как при запросе) зарплата(от) зарплата(до) город(России) опыт</b>\n\nДля опыта подходят только такие параметры:\nНет опыта\nОт 1 года до 3 лет\nОт 3 до 6 лет\nБолее 6 лет\n\nДолжность необходимо вводить также, как при запросе!\n\n<em>Доступные города:</em>\nМосква\nСанкт-Петербург\nКазань\nНовосибирск\nЕкатеринбург\n\n<em>Пример:</em>\n/filter frontend 50000 100000 Москва Нет опыта\n\nЕсли какой-либо параметр из представленных вам не нужен, поставьте на его месте '-'.\n<em>Пример:</em>\n /filter - - 100000 Санкт_Петербург От 1 года до 3 лет", parse_mode='html')
        elif len(words) > 5:
            pos = words[1] if words[1] != "-" else None
            sal_fr = words[2] if words[2] != "-" else None
            sal_to = words[3] if words[3] != "-" else None
            ci = words[4] if words[4] != "-" else None
            exper = words[5] if words[5] != "-" else None
        # параметра для опыта
            if exper == "Нет":
                exper = " ".join(words[5:7]) 
            if exper == "Более":
                exper = " ".join(words[5:8]) 
            if exper == "От":
                exper = " ".join(words[5:11]) 

            sql = "SELECT position, salary_from, salary_to, experience, query, city, currency FROM vacancies WHERE"
            params = []

            if pos is not None:
                sql += " query = %s AND"
                params.append(pos) 

            if sal_fr is not None:
                sql += " salary_from >= %s AND"
                params.append(int(sal_fr)) 

            if sal_to is not None:
                sql += " salary_to <= %s AND"
                params.append(int(sal_to))

            if ci is not None:
                sql += " city = %s AND"
                params.append(ci)

            if exper is not None:
                sql += " experience = %s"
                params.append(exper)

            if sql.endswith(" AND"):
                sql = sql[:-4]

            # print(sql)
            # print(params)
            cur.execute(sql, params)
            data = list(cur.fetchall())
            if data:
                for position, salary_from, salary_to, experience, query, city, currency in data:
                    if currency == "RUR":
                        currency = "₽"
                    elif currency == "USD":
                        currency = "$"
                    if salary_from is None:
                        text += f"Должность: {position}\nЗарплата: до {salary_to} {currency}\nОпыт работы: {experience}\nГород: {city}\n-------------\n"
                        bot.send_message(message.chat.id, text)
                        text = ""
                    elif salary_to is None:
                        text += f"Должность: {position}\nЗарплата: от {salary_from} {currency}\nОпыт работы: {experience}\nГород: {city}\n-------------\n"
                        bot.send_message(message.chat.id, text)
                        text = ""
                    else:
                        text += f"Должность: {position}\nЗарплата: {salary_from} - {salary_to}  {currency}\nОпыт работы: {experience}\nГород: {city}\n-------------\n"
                        bot.send_message(message.chat.id, text)
                        text = ""
            else:
                bot.send_message(message.chat.id, "<em><u>Данных по таким фильтрам нет!</u></em>\n\nЭто происходит в 3х случаях:\n<b>1)</b> По вашим параметрам нет данных (попробуйте изменить фильтры или применить другие).\n<b>2)</b> Параметры неверно введены (проверьте правильность введенных вами параметров, а также соблюдение последовательности параметров и пробелы между ними).\n<b>3)</b> В базе данных нет записей (сделайте запрос для получения данных, узнать подробнее можно по команде /zs).\n\n<em>Попробуйте еще раз!</em>\nВведите /filter для получения инструкции по созданию фильтрации.", parse_mode='html')
                print("Ничего не нашлось")            
        else:
            bot.send_message(message.chat.id, "Пожалуйста, проставьте на местах всех параметров, которые вы не вводите '-'!\nПример:\n/filter - - 100000 - -\nЛибо проверьте правильность введения параметров и пробелы между ними!")
    except Exception as e:
        bot.send_message(message.chat.id, "Проверьте правильность введённых вами параметров и пробелы между ними!")
        print("Ошибка:", e)


@bot.message_handler(commands=['all']) # для вывода всех записей из бд 
def all(message):
    cur.execute("SELECT position, salary_from, salary_to, experience, city, currency FROM vacancies")
    d = list(cur.fetchall())
    if d: 
        mes = ""
        n = 1
        for position, salary_from, salary_to, experience, city, currency in d:
            if currency == "RUR":
                currency = "₽"
            elif currency == "USD":
                currency = "$"
            if salary_from is None:
                mes = mes + f"{n}. Должность: {position}\nЗарплата: до {salary_to} {currency}\nОпыт работы: {experience}\nГород: {city}\n-------------\n"
                bot.send_message(message.chat.id, mes)
                n+=1
                mes = ""
            elif salary_to is None:
                mes = mes + f"{n}. Должность: {position}\nЗарплата: от {salary_from} {currency}\nОпыт работы: {experience}\nГород: {city}\n-------------\n"
                bot.send_message(message.chat.id, mes)
                n+=1
                mes = ""
            else:
                mes = mes + f"{n}. Должность: {position}\nЗарплата: {salary_from} - {salary_to}  {currency}\nОпыт работы: {experience}\nГород: {city}\n-------------\n"
                bot.send_message(message.chat.id, mes)
                n+=1
                mes = ""
    else:
         bot.send_message(message.chat.id, "Данных в бд нет!\nСделайте запрос для получения данных. Узнать больше можно по команде /zs")

def clear_vacancies_table(): # отчиста базы данных
    cur.execute("DELETE FROM vacancies;")
    conn.commit()
scheduler = BackgroundScheduler()
scheduler.add_job(clear_vacancies_table, 'interval', hours=5)
scheduler.start()
   
bot.infinity_polling()
cur.close()
conn.close()

