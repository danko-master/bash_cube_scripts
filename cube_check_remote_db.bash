#!/bin/bash

path_to_sqlite="/home/gashnikovon/cubes/pcub_sac/dcopy.sqlite"
mysql_user='sac'
mysql_pass='qir29sir'
mysql_host='192.168.129.134'
mysql_db='sac_dev'



sqlite3 $path_to_sqlite  "insert into log (date_time, info) values(datetime('now'), 'Запуск скрипта выборки данных из БД СЦ')"

last_created_at=$(sqlite3 $path_to_sqlite  "select created_at from data order by created_at desc limit(1)")
echo "$last_created_at"

sqlite3 $path_to_sqlite  "insert into log (date_time, info) values(datetime('now'), 'Поиск данных из БД СЦ с датой свыше $last_created_at')"


# check mysql
mysql_message=$(mysql --user $mysql_user -p$mysql_pass -h$mysql_host $mysql_db -Bse "SELECT parameter_id, subject_id, val_numeric, created_at FROM param_vals where created_at > '$last_created_at' limit 1" 2>&1 )
if [ ${PIPESTATUS[0]} -ne 0 ]
then
  message="Ошибка скрипта: ${mysql_message}" 
  sqlite3 $path_to_sqlite  "insert into log (date_time, info) values(datetime('now'), 'Ошибка работы с MySQL. Данные не внесены!')"
  echo $message
else
  i=0
  sqlite_cmd=""
  va=500
  va_cnt=0
  while read fieldA fieldB fieldC fieldD fieldE fieldF
  do
    echo "Record $(( i++ )): fieldA: $fieldA, fieldB: $fieldB, fieldC: $fieldC, fieldD: $fieldD, fieldE: $fieldE, fieldF: $fieldF::${fieldF:0:4}_${fieldF:5:2}"
    # $(( i++ ))

    echo $[$i/$va]
    if [ $[$i/$va] -gt $va_cnt ] 
    then
      sqlite_cmd+="insert into data (param_id, subject_id, value, created_at, year, mounth) values(\"$fieldA\", \"$fieldB\", \"$fieldC\", \"$fieldD\", \"${fieldF:0:4}\", \"${fieldF:5:2}\");"
      sqlite3 $path_to_sqlite "BEGIN TRANSACTION; $sqlite_cmd COMMIT TRANSACTION;"
      echo "Вставка записей в БД"
      echo "Пока Вставлено записей: $i и процесс продолжается"
      sqlite3 $path_to_sqlite  "insert into log (date_time, info) values(datetime('now'), 'Пока Вставлено записей: $i и процесс продолжается')"
      # echo $sqlite_cmd
      echo "кратность: $(( va_cnt++ ))"
      # обнуляем вставку
      sqlite_cmd=""
    else  
      sqlite_cmd+="insert into data (param_id, subject_id, value, created_at, year, mounth) values(\"$fieldA\", \"$fieldB\", \"$fieldC\", \"$fieldD\", \"${fieldF:0:4}\", \"${fieldF:5:2}\");"
    fi
    # sqlite3 $path_to_sqlite  "insert into data (param_id, subject_id, value, created_at, year, mounth) values(\"$fieldA\", \"$fieldB\", \"$fieldC\", \"$fieldD\", \"${fieldF:0:4}\", \"${fieldF:5:2}\")"
  done < <(mysql --user $mysql_user -p$mysql_pass -h$mysql_host $mysql_db -Bse "SELECT parameter_id, subject_id, val_numeric, created_at, date_time FROM param_vals where created_at > '$last_created_at'")
  
  # вставим остатки
  sqlite3 $path_to_sqlite "BEGIN TRANSACTION; $sqlite_cmd COMMIT TRANSACTION;"
  echo "Вставка остатков записей в БД"
  echo "Пока Вставлено записей: $i и процесс завершается"
  sqlite3 $path_to_sqlite  "insert into log (date_time, info) values(datetime('now'), 'Пока Вставлено записей: $i и процесс завершается')"
  
  
  # sqlite3 $path_to_sqlite  "insert into data (param_id, subject_id, value, created_at, year, mounth) values
  echo "Вcего Вставлено записей: $i"
  sqlite3 $path_to_sqlite  "insert into log (date_time, info) values(datetime('now'), 'Вставлено записей: $i')"
fi



sqlite3 $path_to_sqlite  "insert into log (date_time, info) values(datetime('now'), 'Остановка скрипта выборки данных из БД СЦ')"


echo "OK"
