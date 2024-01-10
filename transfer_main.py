import connect_to_mis
import connect_to_lis
import datetime
import time

while True:
    # время таймаута (1 час)
    sleep_time = 3600

    # задание временных рамок
    now = datetime.datetime.now()
    time_9am = now.replace(hour=9, minute=0, second=0, microsecond=0)
    time_9pm = now.replace(hour=21, minute=0, second=0, microsecond=0)

    # открытие файла записи логов
    output_file = open('output.txt', 'a')

    # проверка временных рамок
    if time_9am <= now <= time_9pm:

        print(f"Программа запущена. Не останавливайте работу программы!")
        output_file.write(f"{datetime.datetime.now().replace(microsecond=0)}   Программа запущена.\n")
        
        # подключение к ЛИСу (см. connect_to_lis.py)
        lis_connection, lis_cursor = connect_to_lis.lis_connect()

        if lis_connection:
            print("Подключение к ЛИСу выполнено успешно!")
            records = []

            # получение record_id невыгруженных заказов
            try:
                lis_cursor.execute("SELECT record_id, COUNT(*) \
                                FROM dbo.acl_out_sample INNER JOIN \
                                dbo.acl_out_results_sample_type0 on dbo.acl_out_sample.record_id = dbo.acl_out_results_sample_type0.sample_out_record_id \
                                WHERE fread IS NULL AND len(patient_id)<=6 \
                                GROUP BY record_id")
                lis_connection.commit()

            except Exception as e:
                # изменение таймаута на 10 мин
                sleep_time = 600

                print(e)
                print("Ошибка получения списка не перекаченных заказов")
                try:
                    output_file.write(f"{datetime.datetime.now().replace(microsecond=0)}   Ошибка получения списка не перекаченных заказов.\n")
                except Exception:
                    print("Ошибка записи логов")

            else:
                records = lis_cursor.fetchall()
                print("Список невыгруженных заказов получен успешно!")

                # подключение к МИСу (см. connect_to_mis.py)
                mis_connection, mis_cursor = connect_to_mis.mis_connect()

                if mis_connection:
                    print("Подключение к МИСу выполнено успешно!")

                    # счетчик записей за сессию
                    total_sum = 0
                    # начало сессии перекачки
                    start_time = datetime.datetime.now()

                    # перебор заказов
                    for indicator in records:
                        checksum = indicator[1]
                        current_id = indicator[0]

                        #получение невыгруженных анализов для каждого заказа
                        try:
                            lis_cursor.execute(f"SELECT \
                                                patient_id,\
                                                test_code,\
                                                sample_date,\
                                                mcn_code,\
                                                result,\
                                                units,\
                                                refmin,\
                                                refmax,\
                                                specimen \
                                                FROM acl_out_sample INNER JOIN acl_out_results_sample_type0 on acl_out_sample.record_id = acl_out_results_sample_type0.sample_out_record_id\
                                                WHERE record_id = '{current_id}'")
                            lis_connection.commit()

                        except Exception as e:
                            # изменение таймаута на 10 мин
                            sleep_time = 600

                            print(e)
                            print("Ошибка получения списка не перекаченных анализов")
                            try:
                                output_file.write(f"{datetime.datetime.now().replace(microsecond=0)}   Ошибка получения списка не перекаченных анализов.\n")
                            except Exception:
                                print("Ошибка записи логов")

                        else:
                            analyzes = lis_cursor.fetchall()

                            # счетчик обработанных записей
                            counter = 0

                            # перебор каждого анализа и проверка его присутствия в МИСе
                            for one_analyze in analyzes:
                                try:
                                    mis_cursor.execute(f"SELECT * FROM \"tblPatientAclResult\" \
                                                        WHERE patient_id = '{one_analyze[0]}' AND acl_test_code = '{one_analyze[1]}' AND acl_sample_date = '{one_analyze[2]}'")
                                    mis_connection.commit()

                                except Exception as e:
                                    # изменение таймаута на 10 мин
                                    sleep_time = 600
                                    
                                    print(e)
                                    print("Ошибка проверки первичного ключа")
                                    try:
                                        output_file.write(f"{datetime.datetime.now().replace(microsecond=0)}   Ошибка проверки первичного ключа.\n")
                                    except Exception:
                                        print("Ошибка записи логов")

                                else:
                                    mis_analyze = mis_cursor.fetchone()

                                    # если первичный ключ (ПК) найден, то обновляем эту запись по ПК
                                    if mis_analyze:
                                        try:
                                            mis_cursor.execute(f"UPDATE \"tblPatientAclResult\" \
                                                                SET acl_mcn_code = '{one_analyze[3]}', acl_result = '{one_analyze[4]}', acl_units = '{one_analyze[5]}', \
                                                                acl_refmin = '{one_analyze[6]}', acl_refmax = '{one_analyze[7]}', acl_specimen = '{one_analyze[8]}', \
                                                                user1 = 'dbo', datetime1 = '{datetime.date.today()}' \
                                                                WHERE patient_id = '{one_analyze[0]}' AND acl_test_code = '{one_analyze[1]}' AND acl_sample_date = '{one_analyze[2]}'")
                                            mis_connection.commit()

                                        except Exception as e:
                                            # изменение таймаута на 10 мин
                                            sleep_time = 600

                                            print(e)
                                            print("Ошибка обновления записи")
                                            try:
                                                output_file.write(f"{datetime.datetime.now().replace(microsecond=0)}   Ошибка обновления записи.\n")
                                            except Exception:
                                                print("Ошибка записи логов")
                                        
                                        else:
                                            counter += 1

                                    # если первичный ключ (ПК) не найден, то добавляем запись по ПК
                                    else:
                                        try:
                                            mis_cursor.execute(f"INSERT INTO \"tblPatientAclResult\" \
                                                                VALUES ('{one_analyze[0]}', '{one_analyze[1]}', '{one_analyze[2]}', '{one_analyze[3]}', \
                                                                '{one_analyze[4]}', '{one_analyze[5]}', '{one_analyze[6]}', '{one_analyze[7]}', '{one_analyze[8]}', \
                                                                'dbo', '{datetime.date.today()}')")
                                            mis_connection.commit()

                                        except Exception as e:
                                            # изменение таймаута на 10 мин
                                            sleep_time = 600

                                            print(e)
                                            print("Ошибка добавления записи")
                                            try:
                                                output_file.write(f"{datetime.datetime.now().replace(microsecond=0)}   Ошибка добавления записи.\n")
                                            except Exception:
                                                print("Ошибка записи логов")
                                        
                                        else:
                                            counter += 1

                        # проверяем количество анализов по ID записи после добавления
                        try:
                            lis_cursor.execute(f"SELECT COUNT(*) \
                                                FROM dbo.acl_out_sample INNER JOIN \
                                                dbo.acl_out_results_sample_type0 on dbo.acl_out_sample.record_id = dbo.acl_out_results_sample_type0.sample_out_record_id \
                                                WHERE record_id = '{current_id}'")
                            lis_connection.commit()

                        except Exception as e:
                            # изменение таймаута на 10 мин
                            sleep_time = 600

                            print(e)
                            print("Ошибка вычисления кол-ва анализов в заказе")
                            try:
                                output_file.write(f"{datetime.datetime.now().replace(microsecond=0)}   Ошибка вычисления кол-ва анализов в заказе.\n")
                            except Exception:
                                print("Ошибка записи логов")
                        
                        else:
                            res_sum = lis_cursor.fetchone()

                            # если изначальное кол-во анализов, счетчик обработанных записей и кол-во анализов после добавления сходится, то ставим флаг передачи записи fread
                            if counter == checksum == res_sum[0]:
                                try:
                                    lis_cursor.execute(f"UPDATE acl_out_sample \
                                                        SET fread = '1'\
                                                        WHERE record_id = '{current_id}'")
                                    lis_connection.commit()

                                except Exception as e:
                                    # изменение таймаута на 10 мин
                                    sleep_time = 600

                                    print(e)
                                    print("Ошибка обновления флага fread в ЛИСе")
                                    try:
                                        output_file.write(f"{datetime.datetime.now().replace(microsecond=0)}   Ошибка обновления флага fread в ЛИСе.\n")
                                    except Exception:
                                        print("Ошибка записи логов")

                                else:
                                    print(f"Передано {counter} записей")
                                    total_sum += counter
                    
                    # время окончания перекачки
                    end_time = datetime.datetime.now()

            # закрываем соеинение с базами данных
                    mis_connection.close()            
            lis_connection.close()
            
            print(f"Передача выполнена! Всего передано {total_sum} записей за {str(end_time-start_time)[:-3]}")
            try:
                output_file.write(f"{datetime.datetime.now().replace(microsecond=0)}   Всего передано {total_sum} записей за {str(end_time-start_time)[:-3]}\n")
            except Exception:
                print("Ошибка записи логов")
        print(f"Программа в спящем режиме. Можно останавливать нажатием Ctrl+C.")
        output_file.write(f"{datetime.datetime.now().replace(microsecond=0)}   Программа в спящем режиме.\n \n")
    else:
        print("Ночью мы спим!")
    
    output_file.close()
    
    # переход в спящий режим
    time.sleep(sleep_time)