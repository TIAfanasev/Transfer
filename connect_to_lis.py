import pymssql
import datetime

def lis_connect ():
    try:
        connection = pymssql.connect(server='labserv', 
                            user='sa2', 
                            password='4100', 
                            database='host_acl',
                            autocommit = True)
        cursor = connection.cursor()

        # cursor.execute ("SELECT TOP 5 * FROM acl_out_sample",)
        # connection.commit()
        # print(cursor.fetchone())
    except Exception as e:
        print(e)
        print("Ошибка подключения к ЛИСу")
        output_file = open('output.txt', 'a')
        try:
            output_file.write(f"{datetime.datetime.now().replace(microsecond=0)}    Ошибка подключения к ЛИСу\n")
        finally:
            output_file.close()
        return False, False
    else:
        return connection, cursor