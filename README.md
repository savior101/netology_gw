
## Дипломный проект по профессии Data Engineer
#### Нетология, курс "Дата-инженер с нуля до middle"
#### Селезенев Антон, группа DEG-17

Задание на выполнение работ в файле [TECHNICAL_TASK](https://github.com/savior101/netology_gw/blob/main/documents/TECHNICAL_TASK.md).

**Инструменты:** Kubernetes-cluster K3S (Debian bullseye), PostgreSQL 14.5, DBeaver Community 22.2.5, [SqlDBM](https://sqldbm.com), Python 3.7, Apache Airflow 2.6.2, Tableau Dasktop, Ubuntu 22.04.2 LTS. 

**Этапы реализации:**
1. Анализ исходных данных;
2. Проектирование DWH;
3. Разработка DWH;
4. Разработка ETL-процессов;
5. Тестирование, коррекция;
6. Построение отчетности в Tableau;
7. Написание документации.

**Результат работы:**
+ Проектные ER-диаграммы DWH;
+ SQL-скрипт для развертывания DWH;
+ Итоговые ER-диаграммы DWH;
+ Python-скрипты ETL-процессов;
+ DAG-файл Airflow;
+ Tableau workbook с двумя аналитическими дашбордами.

Система производит выгрузку данных из файла-источника, трансформирует, дополняет, агрегирует и записывает в хранилище данных, основанное на СУБД PostgreSQL, для дальнейшей работы с ним с помощью BI инструментов.

Описание входного датасета (источник данных) - в файле [DATASET_DESCRIPTION](http://github.com/savior101/netology_gw/blob/main/DATASET_DESCRIPTION.md).

#### → Проектирование DWH
За основу принята архитектура Инмона: данные из источника в неизменном виде загружаются в stage, затем нормализуются и загружаются в nds, после чего обогощаются данными и загружаются в dds. Разработка логической и физической схем выполнена с использованием сервиса SqlDBM.

Схема **stage**.
<p align="center">
  <img width="230" height="300" src="https://github.com/savior101/netology_gw/blob/main/pictures/db_model/stage.png">
</p>

Схема **nds** (normalized data store).
<p align="center">
  <img width="650" height="650" src="https://github.com/savior101/netology_gw/blob/main/pictures/db_model/nds.png">
</p>
Таблицы в nds обладают историчностью (SCD2).

Схема **dds** (dimension data store).
<p align="center">
  <img width="670" height="450" src="https://github.com/savior101/netology_gw/blob/main/pictures/db_model/dds.png">
</p>

#### → Разработка DWH
Хранилище данных реализовано на базе PostgreSQL 14 (инстанс поднят в k3s-cluster). Скрипт развертывания БД содержит DDL создания схем, отношений, индексов.

**ER-диаграммы:**

Схема **stage**.
<p align="center">
  <img width="150" height="300" src="https://github.com/savior101/netology_gw/blob/main/pictures/ER_diagrams/stage.png">
</p>

Схема **nds**.
<p align="center">
  <img width="730" height="450" src="https://github.com/savior101/netology_gw/blob/main/pictures/ER_diagrams/nds.png">
</p>

Схема **dds**.
<p align="center">
  <img width="470" height="300" src="https://github.com/savior101/netology_gw/blob/main/pictures/ER_diagrams/dds.png">
</p>

#### → Разработка ETL-процесса
В качестве инструмента выбран Python, оркестрация осуществляется с помощью Apache Airflow (инстанс поднят в k3s-cluster).

ETL-процесс разбит на 3 этапа:
1. Извлечение из файла-источника, запись в неизменном виде в таблицу stage;
2. Извлечение из stage, нормализация данных, запись в схему nds;
3. Извлечение из nds, преобразование, обогащение данных, запись в схему dds.

Код описанных выше этапов, а также DAG-файл расположены в директории etl репозитория (дополнительно приложен txt-файл переменной airflow).

#### → Построение отчетности в Tableau
Разработаны два дашборда:
1. "Общие сведения о продажах", который включает в себя индикацию таких метрик, как общее количество продаж, средний чек, выручка, себестоимость продаж, прибыль. Также на дашборде отображены графики динамики данных метрик.\
  Все показатели можно рассматривать в разрезе дат, годов, месяцев, дней недели, праздничных / не праздничных дней, а также предусмотрена возможность отфильтровать по филиалу.
2. "Анализ продаж". Дашборд содержит 6 графиков:
+ Распределение выручки по половому признаку;
+ Распределение выручки в разрезе категории продуктов и пола клиента;
+ Средний чек по типу клиента ("Member", "Normal");
+ Средний чек по рейтингам (анализ удовлетворенности покупателей);
+ Продажи по категориям продуктов (количество продаж и выручка по сегментам);
+ Средний чек по филиалам.\
  Все показатели можно рассматривать в разрезе дат, годов, месяцев, дней недели, праздничных / не праздничных дней, а также предусмотрена возможность отфильтровать по филиалу.

Дашборд **Общие сведения о продажах**.
<p align="center">
  <img width="800" height="500" src="https://github.com/savior101/netology_gw/blob/main/pictures/dashboard_screens/Общие сведения о продажах.jpg">
</p>

Дашборд **Анализ продаж**.
<p align="center">
  <img width="800" height="500" src="https://github.com/savior101/netology_gw/blob/main/pictures/dashboard_screens/Анализ продаж.jpg">
</p>


Описание физической схемы данных - в [файле](https://).
