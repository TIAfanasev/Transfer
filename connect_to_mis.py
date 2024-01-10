import psycopg2
import datetime

def mis_connect ():
    try:
        connection = psycopg2.connect(
                database="HIV",
                user="sa2",
                host="192.168.27.1",
                password="4100",
                port="5432"
            )
        cursor = connection.cursor()
    except Exception as e:
        print(e)
        print("Ошибка подключения к МИСу")
        output_file = open('output.txt', 'a')
        try:
            output_file.write(f"{datetime.datetime.now().replace(microsecond=0)}    Ошибка подключения к МИСу\n")
        finally:
            output_file.close()
        return False, False
    else:
        return connection, cursor