Описание работ по проекту

Шаг 1. Обновить структуру Data Lake
	В таблицу событий добавились два поля — широта и долгота исходящих сообщений. Обновлённая таблица уже находится в HDFS по этому пути: /user/master/data/geo/events.
	Добавить координаты городов Австралии, которые аналитики собрали в одну таблицу — geo.csv. Для этого необходимо загрузить таблицу в Jupyter, воспользовавшись командой copyFromLocal в терминале.
		-создадим директорию командой hdfs dfs -mkdir -p /user/malina692/data/geo
		-скопируем файл командой hdfs dfs -put /lessons/geo.csv /user/malina692/data/geo/geo.csv
		-прочитаем данные (код в файле read_geo_csv.py)	
Шаг 2. Создать витрину в разрезе пользователей	
Шаг 3. Создать витрину в разрезе зон
Шаг 4. Построить витрину для рекомендации друзей
Итоговый файл geo_analysis.py по шагам 2-4
Шаг 5. Автоматизировать обновление витрин (dag geo_analysis_dag.py)