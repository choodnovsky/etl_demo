## Описание проекта
Целью настоящего проекта является демонстрация возможностей опенсорсного ПО в обработке данных.
Бизнес логика следующая:
* Заказчик кладет в s3-совместимый бакет csv файл с выгруженными данными о продажах супермаркета  
* Название файла меняться НЕ должно. Наличие суффикса в виде даты и тп обсуждается дополнительно
* Состав и название колонок меняться НЕ должны. Предполагается что выгрузка осуществляется из единой системы заказчика.
* Периодичность выгрузки и пересечение данных значения НЕ имеет

## Описание инфраструктуры
Предполагается что инфраструктура будет располагаться в облаке, но ввиду отсутствия доступа к облакам и в целях демонстрации 
развернем инфраструктуру в докер-контейнерах на локальном компьютере c mac osx m1  
____
Схема инфраструктуры ![blocks](images/blocks.png)  


#### Airflow
1. Скачиваем в директорию с проектом файл __docker-compose.yaml__ `curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.3.3/docker-compose.yaml'`
2. Создаем директории `mkdir -p ./dags ./logs ./plugins ./data`. Data понадобится когда будем скачивать сырые файлы
3. Для работы на локальном компе можно выставить `AIRFLOW__CORE__EXECUTOR: LocalExecutor` и удалить из __docker-compose.yaml__ все что связано с celery и redis, но профиту от этого мало
4. Инициализируем базу __AIRFLOW__ командой `docker compose up airflow-init`. Сборка завершена с кодом 0 - т.е. успешно
5. Запускаем остальные контейнеры с севисами __AIRFLOW__ `docker-compose up -d`
6. Если надо добавить библиотеки создаем __requirements.txt__, пересобираем образ командой `docker build . --tag extending_airflow:latest`. При этом прописываем название после тега в копии __docker-compose.yaml__
7. Переназываем новый __docker-compose-ext.yaml__
8. Закрываем все контейнеры командой `docker compose down`
9. Заново инициализируем базу __AIRFLOW__ командой `docker compose up airflow-init` на основе первого __docker-compose.yaml__ 
10. Запускаем остальные контейнеры на основе расширенного __docker-compose-ext.yaml__ `docker-compose -f docker-compose-ext.yaml up -d --no-deps --build airflow-webserver airflow-scheduler airflow-worker airflow-triggerer`

#### DWH
11. Поднимаем контейнер с __DWH на posrgres__. Переходим в папку `postgres_docker` и находясь там запускаем `docker-compose up -d`

#### Minio (S3)
12. Поднимаем контейнер с __minio бакет сомвестисый с s3__. Переходим в папку `minio_docker` и находясь там запускаем `docker-compose up -d`

#### BI
13. Поднимаем контейнер с __SUPERSET__. Переходим в папку `superset_docker` и находясь там запускаем `docker-compose -f docker-compose-non-dev.yml`
____
Проверяем работоспособность всех контейнеров ![Docker_ps](images/docker_ps.png)
____
Пробуем загрузить файл в бакете ![minio](images/minio_screen.png)


## Описание схемы хранилища
1. Хранилище будет располагаться на базе __postgres__
2. Хранилище будет состоять из двух слоев:
   * __nds__, куда будем заливать нормализованные до 3NF таблицы фактов и измерений, но уникальные, но без ключей
   * __dds__, куда будем перегружать данные из таблиц фактов и измерений с преобразованными форматами, ключами. При этом таблицы в dds будут обновляться только свежими данными, т.е. если значение измерения или номер заказа в фактах уже есть эта строка проигнорируется
3. Формирование витрин будет осуществляться в BI системе на базе __apache superset__ по запросу заказчика. В качестве демонстрации будет создана денормализованная таблица с расширенными признаками времени и даты

____
Схема слоя NDS ![stage](images/nds.png)  
____
Схема слоя DDS ![nds](images/dds.png)



## Описание ETL процесса
1. Заказчик кладет выгрузку в формате csv в бакет совместимый с S3
2. Даг airflow активируется при срабатывании сенсора, когда в определенный бакет попадает определенный файл
3. Следующий таск сохраняет файл в файловой системе airflow (предполагается что заказчик присылает выгрузку не за 10 лет. несколько сотен тысяч строк допустимо)
4. При сохранении файла airflow дает ему техническое название и сохраняет без расширения. Перенаименовываем сохраненный файл
5. Параллельно проверяем есть ли нужные сущности в слое nds. Если нет - создаем их. По идее создание таблиц осуществится в первый раз. В последующие разы тас будет проходить мимо
6. Сразу же заливаем измерения Время и Дата, так как они не зависят от получаемых данных
7. Из полученного файла получаем уникальные названия измерений:  
   * Направление продаж
   * Гендер
   * Товарная группа
   * Способ оплаты
   * Город
   * Тип покупателя  
   Во избежание конфликтов postgres заливаем эти данные как есть в слой stage. Не заботимся ни о ключах ни о формате
8. После сохранения файла в файловой системе удаляем его из s3
9. После сохранения таблиц с измерениями в stage сравниваем названия с уже существующими уникальными значениями в аналогичных таблицах в слое nds
10. Т.к. таблицы в слое nds обладают инкрементальными ключами добавление новых значений будет увеличивать номера ключей. Прежние ключи останутся без изменений
11. В следующем таске получаем обновленные таблицы с измерениями и ключами и преобразуем их в питоновские словари меняя местами ключ-значение
12. Снова возвращаемся к скаченному файлу и меняем все измерения с текста на номера ключей. Преобразуем форматы дат и времени
13. Заливаем полученную таблицу в слой nds
14. Сравниваем номера заказов с номерами ранее загруженных строк с заказами в слое nds и добавляем новые уникальные строки с заказами
15. Таким образом мы будем получать новые данные не затирая предыдущие и исключаем возможное дублирование  
16. После сохранения свежих фактов в слое nds удаляем файл csv из файловой системы airflow
_____
Схема ETL процесса (DAG) ![DAG](images/dag.png)  
_____
## 👉[ТУТ !!! Python скрипт с DAG-ом !!! ТУТ](airflow_docker/dags/dag_etl_taskflow.py) 👈
_____
<html>
<body bgcolor="#2b2b2b">
<table CELLSPACING=0 CELLPADDING=5 COLS=1 WIDTH="100%" BGCOLOR="#606060" >
<tr><td><center>
<font face="Arial, Helvetica" color="#000000">
dag_etl_taskflow.py</font>
</center></td></tr></table>
<pre><a name="l1"><span class="ln">1    </span></a><span class="s0">import </span><span class="s1">os</span>
<a name="l2"><span class="ln">2    </span></a><span class="s0">import </span><span class="s1">sqlalchemy</span>
<a name="l3"><span class="ln">3    </span></a><span class="s0">from </span><span class="s1">datetime </span><span class="s0">import </span><span class="s1">datetime</span><span class="s0">, </span><span class="s1">timedelta</span>
<a name="l4"><span class="ln">4    </span></a><span class="s0">import </span><span class="s1">numpy </span><span class="s0">as </span><span class="s1">np</span>
<a name="l5"><span class="ln">5    </span></a><span class="s0">import </span><span class="s1">pandas </span><span class="s0">as </span><span class="s1">pd</span>
<a name="l6"><span class="ln">6    </span></a><span class="s0">from </span><span class="s1">airflow </span><span class="s0">import </span><span class="s1">DAG</span>
<a name="l7"><span class="ln">7    </span></a><span class="s0">from </span><span class="s1">airflow.decorators </span><span class="s0">import </span><span class="s1">task</span>
<a name="l8"><span class="ln">8    </span></a><span class="s0">from </span><span class="s1">airflow.operators.bash </span><span class="s0">import </span><span class="s1">BashOperator</span>
<a name="l9"><span class="ln">9    </span></a><span class="s0">from </span><span class="s1">airflow.providers.amazon.aws.hooks.s3 </span><span class="s0">import </span><span class="s1">S3Hook</span>
<a name="l10"><span class="ln">10   </span></a><span class="s0">from </span><span class="s1">airflow.providers.amazon.aws.sensors.s3 </span><span class="s0">import </span><span class="s1">S3KeySensor</span>
<a name="l11"><span class="ln">11   </span></a><span class="s0">from </span><span class="s1">airflow.providers.postgres.hooks.postgres </span><span class="s0">import </span><span class="s1">PostgresHook</span>
<a name="l12"><span class="ln">12   </span></a><span class="s0">from </span><span class="s1">airflow.providers.postgres.operators.postgres </span><span class="s0">import </span><span class="s1">PostgresOperator</span>
<a name="l13"><span class="ln">13   </span></a><span class="s0">from </span><span class="s1">airflow.providers.amazon.aws.operators.s3_delete_objects </span><span class="s0">import </span><span class="s1">S3DeleteObjectsOperator</span>
<a name="l14"><span class="ln">14   </span></a>
<a name="l15"><span class="ln">15   </span></a><span class="s1">raw_key = </span><span class="s2">'supermarket_sales.csv'</span>
<a name="l16"><span class="ln">16   </span></a><span class="s1">raw_bucket = </span><span class="s2">'raw'</span>
<a name="l17"><span class="ln">17   </span></a><span class="s1">raw_local_path = </span><span class="s2">'data'</span>
<a name="l18"><span class="ln">18   </span></a><span class="s1">file_new_name = </span><span class="s2">'downloaded_from_minio.csv'</span>
<a name="l19"><span class="ln">19   </span></a><span class="s1">nds_layer = </span><span class="s2">'nds'</span>
<a name="l20"><span class="ln">20   </span></a><span class="s1">dds_layer = </span><span class="s2">'dds'</span>
<a name="l21"><span class="ln">21   </span></a><span class="s1">default_args = {</span>
<a name="l22"><span class="ln">22   </span></a>    <span class="s2">'owner'</span><span class="s1">: </span><span class="s2">'Victor'</span><span class="s0">,</span>
<a name="l23"><span class="ln">23   </span></a>    <span class="s2">'retries'</span><span class="s1">: </span><span class="s3">5</span><span class="s0">,</span>
<a name="l24"><span class="ln">24   </span></a>    <span class="s2">'retry_delay'</span><span class="s1">: timedelta(minutes=</span><span class="s3">10</span><span class="s1">)</span>
<a name="l25"><span class="ln">25   </span></a><span class="s1">}</span>
<a name="l26"><span class="ln">26   </span></a>
<a name="l27"><span class="ln">27   </span></a><span class="s0">with </span><span class="s1">DAG(</span>
<a name="l28"><span class="ln">28   </span></a>        <span class="s1">dag_id=</span><span class="s2">'dag_etl'</span><span class="s0">,</span>
<a name="l29"><span class="ln">29   </span></a>        <span class="s1">description=</span><span class="s2">'стартует, когда в бакет попадает csv файл'</span><span class="s0">,</span>
<a name="l30"><span class="ln">30   </span></a>        <span class="s1">start_date=datetime(</span><span class="s3">2023</span><span class="s0">, </span><span class="s3">4</span><span class="s0">, </span><span class="s3">27</span><span class="s0">, </span><span class="s3">0</span><span class="s1">)</span><span class="s0">,</span>
<a name="l31"><span class="ln">31   </span></a>        <span class="s1">schedule_interval=</span><span class="s2">'@daily'</span><span class="s0">,</span>
<a name="l32"><span class="ln">32   </span></a>        <span class="s1">default_args=default_args</span>
<a name="l33"><span class="ln">33   </span></a><span class="s1">) </span><span class="s0">as </span><span class="s1">dag:</span>
<a name="l34"><span class="ln">34   </span></a>    <span class="s1">task_s3_sensor = S3KeySensor(</span>
<a name="l35"><span class="ln">35   </span></a>        <span class="s1">task_id=</span><span class="s2">'sensor_s3_obj'</span><span class="s0">,</span>
<a name="l36"><span class="ln">36   </span></a>        <span class="s1">bucket_name=raw_bucket</span><span class="s0">,</span>
<a name="l37"><span class="ln">37   </span></a>        <span class="s1">bucket_key=raw_key</span><span class="s0">,</span>
<a name="l38"><span class="ln">38   </span></a>        <span class="s1">aws_conn_id=</span><span class="s2">'minio_conn'</span><span class="s0">,</span>
<a name="l39"><span class="ln">39   </span></a>        <span class="s1">mode=</span><span class="s2">'poke'</span><span class="s0">,</span>
<a name="l40"><span class="ln">40   </span></a>        <span class="s1">poke_interval=</span><span class="s3">5</span><span class="s0">,</span>
<a name="l41"><span class="ln">41   </span></a>        <span class="s1">timeout=</span><span class="s3">30 </span><span class="s4"># Тут надо выставить 24*60*60 - т.е. все сутки, НО комп сильно устает</span>
<a name="l42"><span class="ln">42   </span></a>    <span class="s1">)</span>
<a name="l43"><span class="ln">43   </span></a>    <span class="s1">task_create_tables = PostgresOperator(</span>
<a name="l44"><span class="ln">44   </span></a>        <span class="s1">task_id=</span><span class="s2">'create_nds_tables_if_not_exists'</span><span class="s0">,</span>
<a name="l45"><span class="ln">45   </span></a>        <span class="s1">postgres_conn_id=</span><span class="s2">'postgres_conn'</span><span class="s0">,</span>
<a name="l46"><span class="ln">46   </span></a>        <span class="s1">sql=</span><span class="s2">f&quot;&quot;&quot;</span>
<a name="l47"><span class="ln">47   </span></a>            <span class="s2">CREATE SCHEMA IF NOT EXISTS </span><span class="s0">{</span><span class="s1">dds_layer</span><span class="s0">}</span><span class="s2">;</span>
<a name="l48"><span class="ln">48   </span></a>            <span class="s2">CREATE SCHEMA IF NOT EXISTS </span><span class="s0">{</span><span class="s1">nds_layer</span><span class="s0">}</span><span class="s2">;</span>
<a name="l49"><span class="ln">49   </span></a>    
<a name="l50"><span class="ln">50   </span></a>            <span class="s2">SET search_path TO </span><span class="s0">{</span><span class="s1">dds_layer</span><span class="s0">}</span><span class="s2">;        </span>
<a name="l51"><span class="ln">51   </span></a>            
<a name="l52"><span class="ln">52   </span></a>            <span class="s2">--// создаем таблицу с ветками //--</span>
<a name="l53"><span class="ln">53   </span></a>            <span class="s2">CREATE TABLE IF NOT EXISTS dim_branch(</span>
<a name="l54"><span class="ln">54   </span></a>            <span class="s2">id SERIAL PRIMARY KEY,</span>
<a name="l55"><span class="ln">55   </span></a>            <span class="s2">branch VARCHAR(100) NOT NULL);</span>
<a name="l56"><span class="ln">56   </span></a>            
<a name="l57"><span class="ln">57   </span></a>            <span class="s2">--// создаем таблицу с городами //--</span>
<a name="l58"><span class="ln">58   </span></a>            <span class="s2">CREATE TABLE IF NOT EXISTS dim_city(</span>
<a name="l59"><span class="ln">59   </span></a>            <span class="s2">id SERIAL PRIMARY KEY,</span>
<a name="l60"><span class="ln">60   </span></a>            <span class="s2">city VARCHAR(100) NOT NULL);</span>
<a name="l61"><span class="ln">61   </span></a>            
<a name="l62"><span class="ln">62   </span></a>            <span class="s2">--// создаем таблицу с типами клиентов //--</span>
<a name="l63"><span class="ln">63   </span></a>            <span class="s2">CREATE TABLE IF NOT EXISTS dim_customer_type(</span>
<a name="l64"><span class="ln">64   </span></a>            <span class="s2">id SERIAL PRIMARY KEY,</span>
<a name="l65"><span class="ln">65   </span></a>            <span class="s2">customer_type VARCHAR(200) NOT NULL);</span>
<a name="l66"><span class="ln">66   </span></a>            
<a name="l67"><span class="ln">67   </span></a>            <span class="s2">--// создаем таблицу с гендерами //--</span>
<a name="l68"><span class="ln">68   </span></a>            <span class="s2">CREATE TABLE IF NOT EXISTS dim_gender(</span>
<a name="l69"><span class="ln">69   </span></a>            <span class="s2">id SERIAL PRIMARY KEY,</span>
<a name="l70"><span class="ln">70   </span></a>            <span class="s2">gender VARCHAR(200) NOT NULL);</span>
<a name="l71"><span class="ln">71   </span></a>            
<a name="l72"><span class="ln">72   </span></a>            <span class="s2">--// создаем таблицу с продуктовыми линейками //--</span>
<a name="l73"><span class="ln">73   </span></a>            <span class="s2">CREATE TABLE IF NOT EXISTS dim_product_line(</span>
<a name="l74"><span class="ln">74   </span></a>            <span class="s2">id SERIAL PRIMARY KEY,</span>
<a name="l75"><span class="ln">75   </span></a>            <span class="s2">product_line VARCHAR(200) NOT NULL);</span>
<a name="l76"><span class="ln">76   </span></a>            
<a name="l77"><span class="ln">77   </span></a>            <span class="s2">--// создаем таблицу с видами оплат //--</span>
<a name="l78"><span class="ln">78   </span></a>            <span class="s2">CREATE TABLE IF NOT EXISTS dim_payment(</span>
<a name="l79"><span class="ln">79   </span></a>            <span class="s2">id SERIAL PRIMARY KEY,</span>
<a name="l80"><span class="ln">80   </span></a>            <span class="s2">payment VARCHAR(100) NOT NULL);</span>
<a name="l81"><span class="ln">81   </span></a>            
<a name="l82"><span class="ln">82   </span></a>            <span class="s2">--// создаем таблицу с фактами //--       </span>
<a name="l83"><span class="ln">83   </span></a>            <span class="s2">CREATE TABLE IF NOT EXISTS fact_sales(</span>
<a name="l84"><span class="ln">84   </span></a>            <span class="s2">invoice_id VARCHAR(15) PRIMARY KEY,</span>
<a name="l85"><span class="ln">85   </span></a>            <span class="s2">branch INT NOT NULL REFERENCES dim_branch(id),</span>
<a name="l86"><span class="ln">86   </span></a>            <span class="s2">city INT NOT NULL REFERENCES dim_city(id),</span>
<a name="l87"><span class="ln">87   </span></a>            <span class="s2">customer_type INT NOT NULL REFERENCES dim_customer_type(id),</span>
<a name="l88"><span class="ln">88   </span></a>            <span class="s2">gender INT NOT NULL REFERENCES dim_gender(id),</span>
<a name="l89"><span class="ln">89   </span></a>            <span class="s2">product_line INT NOT NULL REFERENCES dim_product_line(id),</span>
<a name="l90"><span class="ln">90   </span></a>            <span class="s2">unit_price DOUBLE PRECISION,</span>
<a name="l91"><span class="ln">91   </span></a>            <span class="s2">quantity DOUBLE PRECISION,</span>
<a name="l92"><span class="ln">92   </span></a>            <span class="s2">&quot;tax_5%&quot; DOUBLE PRECISION,</span>
<a name="l93"><span class="ln">93   </span></a>            <span class="s2">total DOUBLE PRECISION,</span>
<a name="l94"><span class="ln">94   </span></a>            <span class="s2">date DATE NOT NULL,</span>
<a name="l95"><span class="ln">95   </span></a>            <span class="s2">time TIME NOT NULL,</span>
<a name="l96"><span class="ln">96   </span></a>            <span class="s2">payment INT NOT NULL REFERENCES dim_payment(id),</span>
<a name="l97"><span class="ln">97   </span></a>            <span class="s2">cogs DOUBLE PRECISION,</span>
<a name="l98"><span class="ln">98   </span></a>            <span class="s2">gross_margin_percentage DOUBLE PRECISION,</span>
<a name="l99"><span class="ln">99   </span></a>            <span class="s2">gross_income DOUBLE PRECISION,</span>
<a name="l100"><span class="ln">100  </span></a>            <span class="s2">rating DOUBLE PRECISION);        </span>
<a name="l101"><span class="ln">101  </span></a>        <span class="s2">&quot;&quot;&quot;</span>
<a name="l102"><span class="ln">102  </span></a>    <span class="s1">)</span>
<a name="l103"><span class="ln">103  </span></a>    <span class="s1">task_update_dims = PostgresOperator(</span>
<a name="l104"><span class="ln">104  </span></a>        <span class="s1">task_id=</span><span class="s2">'update_dim_tables'</span><span class="s0">,</span>
<a name="l105"><span class="ln">105  </span></a>        <span class="s1">postgres_conn_id=</span><span class="s2">'postgres_conn'</span><span class="s0">,</span>
<a name="l106"><span class="ln">106  </span></a>        <span class="s1">sql=</span><span class="s2">f&quot;&quot;&quot;</span>
<a name="l107"><span class="ln">107  </span></a>            <span class="s2">SET search_path TO </span><span class="s0">{</span><span class="s1">dds_layer</span><span class="s0">}</span><span class="s2">;    </span>
<a name="l108"><span class="ln">108  </span></a>            
<a name="l109"><span class="ln">109  </span></a>            <span class="s2">--// Обновляем таблицы в dds сырыми таблицами из nds //--</span>
<a name="l110"><span class="ln">110  </span></a>            <span class="s2">INSERT INTO dim_branch (branch)</span>
<a name="l111"><span class="ln">111  </span></a>            <span class="s2">(SELECT branch FROM </span><span class="s0">{</span><span class="s1">nds_layer</span><span class="s0">}</span><span class="s2">.dim_branch WHERE branch NOT IN (SELECT branch FROM dim_branch));</span>
<a name="l112"><span class="ln">112  </span></a>            <span class="s2">INSERT INTO dim_city (city)</span>
<a name="l113"><span class="ln">113  </span></a>            <span class="s2">(SELECT city FROM </span><span class="s0">{</span><span class="s1">nds_layer</span><span class="s0">}</span><span class="s2">.dim_city WHERE city NOT IN (SELECT city FROM dim_city));</span>
<a name="l114"><span class="ln">114  </span></a>            <span class="s2">INSERT INTO dim_customer_type (customer_type)</span>
<a name="l115"><span class="ln">115  </span></a>            <span class="s2">(SELECT customer_type FROM </span><span class="s0">{</span><span class="s1">nds_layer</span><span class="s0">}</span><span class="s2">.dim_customer_type WHERE customer_type NOT IN (SELECT customer_type FROM dim_customer_type));</span>
<a name="l116"><span class="ln">116  </span></a>            <span class="s2">INSERT INTO dim_gender (gender)</span>
<a name="l117"><span class="ln">117  </span></a>            <span class="s2">(SELECT gender FROM </span><span class="s0">{</span><span class="s1">nds_layer</span><span class="s0">}</span><span class="s2">.dim_gender WHERE gender NOT IN (SELECT gender FROM dim_gender));</span>
<a name="l118"><span class="ln">118  </span></a>            <span class="s2">INSERT INTO dim_product_line (product_line)</span>
<a name="l119"><span class="ln">119  </span></a>            <span class="s2">(SELECT product_line FROM </span><span class="s0">{</span><span class="s1">nds_layer</span><span class="s0">}</span><span class="s2">.dim_product_line WHERE product_line NOT IN (SELECT product_line FROM dim_product_line));</span>
<a name="l120"><span class="ln">120  </span></a>            <span class="s2">INSERT INTO dim_payment (payment)</span>
<a name="l121"><span class="ln">121  </span></a>            <span class="s2">(SELECT payment FROM </span><span class="s0">{</span><span class="s1">nds_layer</span><span class="s0">}</span><span class="s2">.dim_payment WHERE payment NOT IN (SELECT payment FROM dim_payment));</span>
<a name="l122"><span class="ln">122  </span></a>             
<a name="l123"><span class="ln">123  </span></a>            <span class="s2">&quot;&quot;&quot;</span>
<a name="l124"><span class="ln">124  </span></a>    <span class="s1">)</span>
<a name="l125"><span class="ln">125  </span></a>    <span class="s1">task_update_fact = PostgresOperator(</span>
<a name="l126"><span class="ln">126  </span></a>        <span class="s1">task_id=</span><span class="s2">'update_fact_table'</span><span class="s0">,</span>
<a name="l127"><span class="ln">127  </span></a>        <span class="s1">postgres_conn_id=</span><span class="s2">'postgres_conn'</span><span class="s0">,</span>
<a name="l128"><span class="ln">128  </span></a>        <span class="s1">sql=</span><span class="s2">f&quot;&quot;&quot;</span>
<a name="l129"><span class="ln">129  </span></a>            <span class="s2">SET search_path TO </span><span class="s0">{</span><span class="s1">dds_layer</span><span class="s0">}</span><span class="s2">;</span>
<a name="l130"><span class="ln">130  </span></a>            
<a name="l131"><span class="ln">131  </span></a>            <span class="s2">--// Обновляем таблицу с фактом свежей таблицей с фактами из stage //--</span>
<a name="l132"><span class="ln">132  </span></a>            <span class="s2">INSERT INTO fact_sales (invoice_id, branch, city, customer_type, gender,</span>
<a name="l133"><span class="ln">133  </span></a>                                    <span class="s2">product_line, unit_price, quantity, &quot;tax_5%&quot;, total, date,</span>
<a name="l134"><span class="ln">134  </span></a>                                    <span class="s2">time, payment, cogs, gross_margin_percentage, gross_income, rating)</span>
<a name="l135"><span class="ln">135  </span></a>            <span class="s2">(SELECT </span>
<a name="l136"><span class="ln">136  </span></a>                    <span class="s2">invoice_id, branch, city, customer_type, gender, </span>
<a name="l137"><span class="ln">137  </span></a>                    <span class="s2">product_line, unit_price, quantity, &quot;tax_5%&quot;, total, date::date,</span>
<a name="l138"><span class="ln">138  </span></a>                    <span class="s2">time, payment, cogs, gross_margin_percentage, gross_income, rating </span>
<a name="l139"><span class="ln">139  </span></a>            <span class="s2">FROM </span><span class="s0">{</span><span class="s1">nds_layer</span><span class="s0">}</span><span class="s2">.fact_sales WHERE invoice_id NOT IN (SELECT invoice_id FROM fact_sales));</span>
<a name="l140"><span class="ln">140  </span></a>            <span class="s2">&quot;&quot;&quot;</span>
<a name="l141"><span class="ln">141  </span></a>    <span class="s1">)</span>
<a name="l142"><span class="ln">142  </span></a>    <span class="s1">task_delete_s3_obj = S3DeleteObjectsOperator(</span>
<a name="l143"><span class="ln">143  </span></a>        <span class="s1">task_id=</span><span class="s2">'delete_s3_obj'</span><span class="s0">,</span>
<a name="l144"><span class="ln">144  </span></a>        <span class="s1">bucket=raw_bucket</span><span class="s0">,</span>
<a name="l145"><span class="ln">145  </span></a>        <span class="s1">keys=raw_key</span><span class="s0">,</span>
<a name="l146"><span class="ln">146  </span></a>        <span class="s1">aws_conn_id=</span><span class="s2">'minio_conn'</span><span class="s0">,</span>
<a name="l147"><span class="ln">147  </span></a>        <span class="s1">trigger_rule=</span><span class="s2">'none_failed_min_one_success'</span>
<a name="l148"><span class="ln">148  </span></a>    <span class="s1">)</span>
<a name="l149"><span class="ln">149  </span></a>    <span class="s1">task_clear_data_directory = BashOperator(</span>
<a name="l150"><span class="ln">150  </span></a>        <span class="s1">task_id=</span><span class="s2">'clear_data_directory'</span><span class="s0">,</span>
<a name="l151"><span class="ln">151  </span></a>        <span class="s1">bash_command=</span><span class="s2">'rm -rf ${pwd}data/* | echo &quot;приехали&quot;'</span>
<a name="l152"><span class="ln">152  </span></a>    <span class="s1">)</span>
<a name="l153"><span class="ln">153  </span></a>
<a name="l154"><span class="ln">154  </span></a>
<a name="l155"><span class="ln">155  </span></a>    <span class="s1">@task</span>
<a name="l156"><span class="ln">156  </span></a>    <span class="s0">def </span><span class="s1">extract_from_s3(bucket_key</span><span class="s0">, </span><span class="s1">bucket_name</span><span class="s0">, </span><span class="s1">local_path):</span>
<a name="l157"><span class="ln">157  </span></a>        <span class="s1">hook = S3Hook(</span><span class="s2">'minio_conn'</span><span class="s1">)</span>
<a name="l158"><span class="ln">158  </span></a>        <span class="s1">file_name = hook.download_file(bucket_key</span><span class="s0">, </span><span class="s1">bucket_name</span><span class="s0">, </span><span class="s1">local_path)</span>
<a name="l159"><span class="ln">159  </span></a>        <span class="s0">return </span><span class="s1">file_name</span>
<a name="l160"><span class="ln">160  </span></a>
<a name="l161"><span class="ln">161  </span></a>
<a name="l162"><span class="ln">162  </span></a>    <span class="s1">@task</span>
<a name="l163"><span class="ln">163  </span></a>    <span class="s0">def </span><span class="s1">rename_extracted_file(file_name</span><span class="s0">, </span><span class="s1">file_new_name):</span>
<a name="l164"><span class="ln">164  </span></a>        <span class="s1">downloaded_file_path = </span><span class="s2">'/'</span><span class="s1">.join(file_name.split(</span><span class="s2">'/'</span><span class="s1">)[:-</span><span class="s3">1</span><span class="s1">])</span>
<a name="l165"><span class="ln">165  </span></a>        <span class="s1">os.rename(src=file_name</span><span class="s0">, </span><span class="s1">dst=</span><span class="s2">f&quot;</span><span class="s0">{</span><span class="s1">downloaded_file_path</span><span class="s0">}</span><span class="s2">/</span><span class="s0">{</span><span class="s1">file_new_name</span><span class="s0">}</span><span class="s2">&quot;</span><span class="s1">)</span>
<a name="l166"><span class="ln">166  </span></a>        <span class="s0">return </span><span class="s1">downloaded_file_path</span>
<a name="l167"><span class="ln">167  </span></a>
<a name="l168"><span class="ln">168  </span></a>
<a name="l169"><span class="ln">169  </span></a>    <span class="s1">@task</span>
<a name="l170"><span class="ln">170  </span></a>    <span class="s0">def </span><span class="s1">dim_branch(downloaded_file_path</span><span class="s0">, </span><span class="s1">file_new_name):</span>
<a name="l171"><span class="ln">171  </span></a>        <span class="s1">df = pd.read_csv(</span><span class="s2">f&quot;</span><span class="s0">{</span><span class="s1">downloaded_file_path</span><span class="s0">}</span><span class="s2">/</span><span class="s0">{</span><span class="s1">file_new_name</span><span class="s0">}</span><span class="s2">&quot;</span><span class="s1">)</span>
<a name="l172"><span class="ln">172  </span></a>        <span class="s1">df.columns = [column_title.lower().replace(</span><span class="s2">' '</span><span class="s0">, </span><span class="s2">'_'</span><span class="s1">) </span><span class="s0">for </span><span class="s1">column_title </span><span class="s0">in </span><span class="s1">df.columns]</span>
<a name="l173"><span class="ln">173  </span></a>        <span class="s1">branch = pd.Series(df[</span><span class="s2">'branch'</span><span class="s1">].unique()</span><span class="s0">, </span><span class="s1">name=</span><span class="s2">'branch'</span><span class="s1">)</span>
<a name="l174"><span class="ln">174  </span></a>        <span class="s1">branch_df = pd.DataFrame(branch)</span>
<a name="l175"><span class="ln">175  </span></a>        <span class="s1">hook = PostgresHook(postgres_conn_id=</span><span class="s2">'postgres_conn'</span><span class="s1">)</span>
<a name="l176"><span class="ln">176  </span></a>        <span class="s1">branch_df.to_sql(</span><span class="s2">'dim_branch'</span><span class="s0">, </span><span class="s1">hook.get_sqlalchemy_engine()</span><span class="s0">, </span><span class="s1">schema=nds_layer</span><span class="s0">, </span><span class="s1">if_exists=</span><span class="s2">'replace'</span><span class="s1">)</span>
<a name="l177"><span class="ln">177  </span></a>
<a name="l178"><span class="ln">178  </span></a>
<a name="l179"><span class="ln">179  </span></a>    <span class="s1">@task</span>
<a name="l180"><span class="ln">180  </span></a>    <span class="s0">def </span><span class="s1">dim_city(downloaded_file_path</span><span class="s0">, </span><span class="s1">file_new_name):</span>
<a name="l181"><span class="ln">181  </span></a>        <span class="s1">df = pd.read_csv(</span><span class="s2">f&quot;</span><span class="s0">{</span><span class="s1">downloaded_file_path</span><span class="s0">}</span><span class="s2">/</span><span class="s0">{</span><span class="s1">file_new_name</span><span class="s0">}</span><span class="s2">&quot;</span><span class="s1">)</span>
<a name="l182"><span class="ln">182  </span></a>        <span class="s1">df.columns = [column_title.lower().replace(</span><span class="s2">' '</span><span class="s0">, </span><span class="s2">'_'</span><span class="s1">) </span><span class="s0">for </span><span class="s1">column_title </span><span class="s0">in </span><span class="s1">df.columns]</span>
<a name="l183"><span class="ln">183  </span></a>        <span class="s1">city = pd.Series(df[</span><span class="s2">'city'</span><span class="s1">].unique()</span><span class="s0">, </span><span class="s1">name=</span><span class="s2">'city'</span><span class="s1">)</span>
<a name="l184"><span class="ln">184  </span></a>        <span class="s1">city_df = pd.DataFrame(city)</span>
<a name="l185"><span class="ln">185  </span></a>        <span class="s1">hook = PostgresHook(postgres_conn_id=</span><span class="s2">'postgres_conn'</span><span class="s1">)</span>
<a name="l186"><span class="ln">186  </span></a>        <span class="s1">city_df.to_sql(</span><span class="s2">'dim_city'</span><span class="s0">, </span><span class="s1">hook.get_sqlalchemy_engine()</span><span class="s0">, </span><span class="s1">schema=nds_layer</span><span class="s0">, </span><span class="s1">if_exists=</span><span class="s2">'replace'</span><span class="s1">)</span>
<a name="l187"><span class="ln">187  </span></a>
<a name="l188"><span class="ln">188  </span></a>
<a name="l189"><span class="ln">189  </span></a>    <span class="s1">@task</span>
<a name="l190"><span class="ln">190  </span></a>    <span class="s0">def </span><span class="s1">dim_customer_type(downloaded_file_path</span><span class="s0">, </span><span class="s1">file_new_name):</span>
<a name="l191"><span class="ln">191  </span></a>        <span class="s1">df = pd.read_csv(</span><span class="s2">f&quot;</span><span class="s0">{</span><span class="s1">downloaded_file_path</span><span class="s0">}</span><span class="s2">/</span><span class="s0">{</span><span class="s1">file_new_name</span><span class="s0">}</span><span class="s2">&quot;</span><span class="s1">)</span>
<a name="l192"><span class="ln">192  </span></a>        <span class="s1">df.columns = [column_title.lower().replace(</span><span class="s2">' '</span><span class="s0">, </span><span class="s2">'_'</span><span class="s1">) </span><span class="s0">for </span><span class="s1">column_title </span><span class="s0">in </span><span class="s1">df.columns]</span>
<a name="l193"><span class="ln">193  </span></a>        <span class="s1">customer_type = pd.Series(df[</span><span class="s2">'customer_type'</span><span class="s1">].unique()</span><span class="s0">, </span><span class="s1">name=</span><span class="s2">'customer_type'</span><span class="s1">)</span>
<a name="l194"><span class="ln">194  </span></a>        <span class="s1">customer_type_df = pd.DataFrame(customer_type)</span>
<a name="l195"><span class="ln">195  </span></a>        <span class="s1">hook = PostgresHook(postgres_conn_id=</span><span class="s2">'postgres_conn'</span><span class="s1">)</span>
<a name="l196"><span class="ln">196  </span></a>        <span class="s1">customer_type_df.to_sql(</span><span class="s2">'dim_customer_type'</span><span class="s0">, </span><span class="s1">hook.get_sqlalchemy_engine()</span><span class="s0">, </span><span class="s1">schema=nds_layer</span><span class="s0">, </span><span class="s1">if_exists=</span><span class="s2">'replace'</span><span class="s1">)</span>
<a name="l197"><span class="ln">197  </span></a>
<a name="l198"><span class="ln">198  </span></a>
<a name="l199"><span class="ln">199  </span></a>    <span class="s1">@task</span>
<a name="l200"><span class="ln">200  </span></a>    <span class="s0">def </span><span class="s1">dim_gender(downloaded_file_path</span><span class="s0">, </span><span class="s1">file_new_name):</span>
<a name="l201"><span class="ln">201  </span></a>        <span class="s1">df = pd.read_csv(</span><span class="s2">f&quot;</span><span class="s0">{</span><span class="s1">downloaded_file_path</span><span class="s0">}</span><span class="s2">/</span><span class="s0">{</span><span class="s1">file_new_name</span><span class="s0">}</span><span class="s2">&quot;</span><span class="s1">)</span>
<a name="l202"><span class="ln">202  </span></a>        <span class="s1">df.columns = [column_title.lower().replace(</span><span class="s2">' '</span><span class="s0">, </span><span class="s2">'_'</span><span class="s1">) </span><span class="s0">for </span><span class="s1">column_title </span><span class="s0">in </span><span class="s1">df.columns]</span>
<a name="l203"><span class="ln">203  </span></a>        <span class="s1">gender = pd.Series(df[</span><span class="s2">'gender'</span><span class="s1">].unique()</span><span class="s0">, </span><span class="s1">name=</span><span class="s2">'gender'</span><span class="s1">)</span>
<a name="l204"><span class="ln">204  </span></a>        <span class="s1">gender_df = pd.DataFrame(gender)</span>
<a name="l205"><span class="ln">205  </span></a>        <span class="s1">hook = PostgresHook(postgres_conn_id=</span><span class="s2">'postgres_conn'</span><span class="s1">)</span>
<a name="l206"><span class="ln">206  </span></a>        <span class="s1">gender_df.to_sql(</span><span class="s2">'dim_gender'</span><span class="s0">, </span><span class="s1">hook.get_sqlalchemy_engine()</span><span class="s0">, </span><span class="s1">schema=nds_layer</span><span class="s0">, </span><span class="s1">if_exists=</span><span class="s2">'replace'</span><span class="s1">)</span>
<a name="l207"><span class="ln">207  </span></a>
<a name="l208"><span class="ln">208  </span></a>
<a name="l209"><span class="ln">209  </span></a>    <span class="s1">@task</span>
<a name="l210"><span class="ln">210  </span></a>    <span class="s0">def </span><span class="s1">dim_product_line(downloaded_file_path</span><span class="s0">, </span><span class="s1">file_new_name):</span>
<a name="l211"><span class="ln">211  </span></a>        <span class="s1">df = pd.read_csv(</span><span class="s2">f&quot;</span><span class="s0">{</span><span class="s1">downloaded_file_path</span><span class="s0">}</span><span class="s2">/</span><span class="s0">{</span><span class="s1">file_new_name</span><span class="s0">}</span><span class="s2">&quot;</span><span class="s1">)</span>
<a name="l212"><span class="ln">212  </span></a>        <span class="s1">df.columns = [column_title.lower().replace(</span><span class="s2">' '</span><span class="s0">, </span><span class="s2">'_'</span><span class="s1">) </span><span class="s0">for </span><span class="s1">column_title </span><span class="s0">in </span><span class="s1">df.columns]</span>
<a name="l213"><span class="ln">213  </span></a>        <span class="s1">product_line = pd.Series(df[</span><span class="s2">'product_line'</span><span class="s1">].unique()</span><span class="s0">, </span><span class="s1">name=</span><span class="s2">'product_line'</span><span class="s1">)</span>
<a name="l214"><span class="ln">214  </span></a>        <span class="s1">product_line_df = pd.DataFrame(product_line)</span>
<a name="l215"><span class="ln">215  </span></a>        <span class="s1">hook = PostgresHook(postgres_conn_id=</span><span class="s2">'postgres_conn'</span><span class="s1">)</span>
<a name="l216"><span class="ln">216  </span></a>        <span class="s1">product_line_df.to_sql(</span><span class="s2">'dim_product_line'</span><span class="s0">, </span><span class="s1">hook.get_sqlalchemy_engine()</span><span class="s0">, </span><span class="s1">schema=nds_layer</span><span class="s0">, </span><span class="s1">if_exists=</span><span class="s2">'replace'</span><span class="s1">)</span>
<a name="l217"><span class="ln">217  </span></a>
<a name="l218"><span class="ln">218  </span></a>
<a name="l219"><span class="ln">219  </span></a>    <span class="s1">@task</span>
<a name="l220"><span class="ln">220  </span></a>    <span class="s0">def </span><span class="s1">dim_payment(downloaded_file_path</span><span class="s0">, </span><span class="s1">file_new_name):</span>
<a name="l221"><span class="ln">221  </span></a>        <span class="s1">df = pd.read_csv(</span><span class="s2">f&quot;</span><span class="s0">{</span><span class="s1">downloaded_file_path</span><span class="s0">}</span><span class="s2">/</span><span class="s0">{</span><span class="s1">file_new_name</span><span class="s0">}</span><span class="s2">&quot;</span><span class="s1">)</span>
<a name="l222"><span class="ln">222  </span></a>        <span class="s1">df.columns = [column_title.lower().replace(</span><span class="s2">' '</span><span class="s0">, </span><span class="s2">'_'</span><span class="s1">) </span><span class="s0">for </span><span class="s1">column_title </span><span class="s0">in </span><span class="s1">df.columns]</span>
<a name="l223"><span class="ln">223  </span></a>        <span class="s1">payment = pd.Series(df[</span><span class="s2">'payment'</span><span class="s1">].unique()</span><span class="s0">, </span><span class="s1">name=</span><span class="s2">'payment'</span><span class="s1">)</span>
<a name="l224"><span class="ln">224  </span></a>        <span class="s1">payment_df = pd.DataFrame(payment)</span>
<a name="l225"><span class="ln">225  </span></a>        <span class="s1">hook = PostgresHook(postgres_conn_id=</span><span class="s2">'postgres_conn'</span><span class="s1">)</span>
<a name="l226"><span class="ln">226  </span></a>        <span class="s1">payment_df.to_sql(</span><span class="s2">'dim_payment'</span><span class="s0">, </span><span class="s1">hook.get_sqlalchemy_engine()</span><span class="s0">, </span><span class="s1">schema=nds_layer</span><span class="s0">, </span><span class="s1">if_exists=</span><span class="s2">'replace'</span><span class="s1">)</span>
<a name="l227"><span class="ln">227  </span></a>
<a name="l228"><span class="ln">228  </span></a>
<a name="l229"><span class="ln">229  </span></a>    <span class="s1">@task</span>
<a name="l230"><span class="ln">230  </span></a>    <span class="s0">def </span><span class="s1">dim_time():</span>
<a name="l231"><span class="ln">231  </span></a>        <span class="s5">&quot;&quot;&quot; 
<a name="l232"><span class="ln">232  </span></a>        Создаем таблицу с временем и признаками времени 
<a name="l233"><span class="ln">233  </span></a>        и заливаем сразу в nds 
<a name="l234"><span class="ln">234  </span></a>        &quot;&quot;&quot;</span>
<a name="l235"><span class="ln">235  </span></a>        <span class="s1">time_range = pd.date_range(start=</span><span class="s2">&quot;00:00&quot;</span><span class="s0">, </span><span class="s1">end=</span><span class="s2">&quot;23:59&quot;</span><span class="s0">, </span><span class="s1">freq=</span><span class="s2">&quot;1min&quot;</span><span class="s1">)</span>
<a name="l236"><span class="ln">236  </span></a>        <span class="s1">df = pd.DataFrame(pd.Series(time_range.strftime(</span><span class="s2">&quot;%H:%M:%S&quot;</span><span class="s1">)</span><span class="s0">, </span><span class="s1">name=</span><span class="s2">'time'</span><span class="s1">))</span>
<a name="l237"><span class="ln">237  </span></a>        <span class="s1">day_part = </span><span class="s2">'day_part'</span>
<a name="l238"><span class="ln">238  </span></a>        <span class="s1">df.loc[(df[</span><span class="s2">'time'</span><span class="s1">] &gt;= </span><span class="s2">'00:00:00'</span><span class="s1">) &amp; (df[</span><span class="s2">'time'</span><span class="s1">] &lt; </span><span class="s2">'06:00:00'</span><span class="s1">)</span><span class="s0">, </span><span class="s1">day_part] = </span><span class="s2">'night'</span>
<a name="l239"><span class="ln">239  </span></a>        <span class="s1">df.loc[(df[</span><span class="s2">'time'</span><span class="s1">] &gt;= </span><span class="s2">'06:00:00'</span><span class="s1">) &amp; (df[</span><span class="s2">'time'</span><span class="s1">] &lt; </span><span class="s2">'11:00:00'</span><span class="s1">)</span><span class="s0">, </span><span class="s1">day_part] = </span><span class="s2">'morning'</span>
<a name="l240"><span class="ln">240  </span></a>        <span class="s1">df.loc[(df[</span><span class="s2">'time'</span><span class="s1">] &gt;= </span><span class="s2">'11:00:00'</span><span class="s1">) &amp; (df[</span><span class="s2">'time'</span><span class="s1">] &lt; </span><span class="s2">'17:00:00'</span><span class="s1">)</span><span class="s0">, </span><span class="s1">day_part] = </span><span class="s2">'noon'</span>
<a name="l241"><span class="ln">241  </span></a>        <span class="s1">df.loc[(df[</span><span class="s2">'time'</span><span class="s1">] &gt;= </span><span class="s2">'17:00:00'</span><span class="s1">) &amp; (df[</span><span class="s2">'time'</span><span class="s1">] &lt; </span><span class="s2">'22:00:00'</span><span class="s1">)</span><span class="s0">, </span><span class="s1">day_part] = </span><span class="s2">'evening'</span>
<a name="l242"><span class="ln">242  </span></a>        <span class="s1">df.loc[(df[</span><span class="s2">'time'</span><span class="s1">] &gt;= </span><span class="s2">'22:00:00'</span><span class="s1">) &amp; (df[</span><span class="s2">'time'</span><span class="s1">] &lt; </span><span class="s2">'24:00:00'</span><span class="s1">)</span><span class="s0">, </span><span class="s1">day_part] = </span><span class="s2">'night'</span>
<a name="l243"><span class="ln">243  </span></a>        <span class="s1">hook = PostgresHook(postgres_conn_id=</span><span class="s2">'postgres_conn'</span><span class="s1">)</span>
<a name="l244"><span class="ln">244  </span></a>        <span class="s1">df.to_sql(</span><span class="s2">'dim_time'</span><span class="s0">,</span>
<a name="l245"><span class="ln">245  </span></a>                  <span class="s1">hook.get_sqlalchemy_engine()</span><span class="s0">,</span>
<a name="l246"><span class="ln">246  </span></a>                  <span class="s1">schema=dds_layer</span><span class="s0">,</span>
<a name="l247"><span class="ln">247  </span></a>                  <span class="s1">if_exists=</span><span class="s2">'replace'</span><span class="s0">,</span>
<a name="l248"><span class="ln">248  </span></a>                  <span class="s1">index=</span><span class="s0">False,</span>
<a name="l249"><span class="ln">249  </span></a>                  <span class="s1">dtype={</span><span class="s2">'time'</span><span class="s1">: sqlalchemy.types.TIME()})</span>
<a name="l250"><span class="ln">250  </span></a>
<a name="l251"><span class="ln">251  </span></a>
<a name="l252"><span class="ln">252  </span></a>    <span class="s1">@task</span>
<a name="l253"><span class="ln">253  </span></a>    <span class="s0">def </span><span class="s1">dim_date():</span>
<a name="l254"><span class="ln">254  </span></a>        <span class="s5">&quot;&quot;&quot; 
<a name="l255"><span class="ln">255  </span></a>        Создаем таблицу с датами и признаками дат 
<a name="l256"><span class="ln">256  </span></a>        и заливаем сразу в nds 
<a name="l257"><span class="ln">257  </span></a>        &quot;&quot;&quot;</span>
<a name="l258"><span class="ln">258  </span></a>        <span class="s1">df = pd.DataFrame(pd.date_range(start=</span><span class="s2">&quot;2019-01-01&quot;</span><span class="s0">, </span><span class="s1">end=</span><span class="s2">&quot;2099-12-31&quot;</span><span class="s1">)</span><span class="s0">, </span><span class="s1">columns=[</span><span class="s2">'date'</span><span class="s1">])</span>
<a name="l259"><span class="ln">259  </span></a>        <span class="s1">df[</span><span class="s2">'week_of_year'</span><span class="s1">] = df[</span><span class="s2">'date'</span><span class="s1">].dt.isocalendar().week</span>
<a name="l260"><span class="ln">260  </span></a>        <span class="s1">df[</span><span class="s2">'week_start'</span><span class="s1">] = df[</span><span class="s2">'date'</span><span class="s1">].dt.to_period(</span><span class="s2">'W-SUN'</span><span class="s1">).dt.start_time</span>
<a name="l261"><span class="ln">261  </span></a>        <span class="s1">df[</span><span class="s2">'day_of_week'</span><span class="s1">] = df[</span><span class="s2">'date'</span><span class="s1">].dt.dayofweek + </span><span class="s3">1</span>
<a name="l262"><span class="ln">262  </span></a>        <span class="s1">df[</span><span class="s2">'month_number'</span><span class="s1">] = df[</span><span class="s2">'date'</span><span class="s1">].dt.month</span>
<a name="l263"><span class="ln">263  </span></a>        <span class="s1">df[</span><span class="s2">'month_name'</span><span class="s1">] = pd.to_datetime(df[</span><span class="s2">'date'</span><span class="s1">]</span><span class="s0">, </span><span class="s1">format=</span><span class="s2">'%m'</span><span class="s1">).dt.month_name()</span>
<a name="l264"><span class="ln">264  </span></a>        <span class="s1">df[</span><span class="s2">'quarter'</span><span class="s1">] = df[</span><span class="s2">'date'</span><span class="s1">].dt.quarter</span>
<a name="l265"><span class="ln">265  </span></a>        <span class="s1">df[</span><span class="s2">'year'</span><span class="s1">] = df[</span><span class="s2">'date'</span><span class="s1">].dt.year</span>
<a name="l266"><span class="ln">266  </span></a>        <span class="s1">df[</span><span class="s2">'season'</span><span class="s1">] = np.where(df[</span><span class="s2">'month_number'</span><span class="s1">].isin([</span><span class="s3">12</span><span class="s0">, </span><span class="s3">1</span><span class="s0">, </span><span class="s3">2</span><span class="s1">])</span><span class="s0">, </span><span class="s2">'winter'</span><span class="s0">, </span><span class="s2">'spring'</span><span class="s1">)</span>
<a name="l267"><span class="ln">267  </span></a>        <span class="s1">df[</span><span class="s2">'season'</span><span class="s1">] = np.where(df[</span><span class="s2">'month_number'</span><span class="s1">].isin([</span><span class="s3">6</span><span class="s0">, </span><span class="s3">7</span><span class="s0">, </span><span class="s3">8</span><span class="s1">])</span><span class="s0">, </span><span class="s2">'summer'</span><span class="s0">, </span><span class="s1">df[</span><span class="s2">'season'</span><span class="s1">])</span>
<a name="l268"><span class="ln">268  </span></a>        <span class="s1">df[</span><span class="s2">'season'</span><span class="s1">] = np.where(df[</span><span class="s2">'month_number'</span><span class="s1">].isin([</span><span class="s3">9</span><span class="s0">, </span><span class="s3">10</span><span class="s0">, </span><span class="s3">11</span><span class="s1">])</span><span class="s0">, </span><span class="s2">'fall'</span><span class="s0">, </span><span class="s1">df[</span><span class="s2">'season'</span><span class="s1">])</span>
<a name="l269"><span class="ln">269  </span></a>        <span class="s1">df[</span><span class="s2">'date'</span><span class="s1">] = df[</span><span class="s2">'date'</span><span class="s1">].dt.strftime(</span><span class="s2">'%Y-%m-%d'</span><span class="s1">)</span>
<a name="l270"><span class="ln">270  </span></a>        <span class="s1">df[</span><span class="s2">'week_start'</span><span class="s1">] = df[</span><span class="s2">'week_start'</span><span class="s1">].dt.strftime(</span><span class="s2">'%Y-%m-%d'</span><span class="s1">)</span>
<a name="l271"><span class="ln">271  </span></a>        <span class="s1">hook = PostgresHook(postgres_conn_id=</span><span class="s2">'postgres_conn'</span><span class="s1">)</span>
<a name="l272"><span class="ln">272  </span></a>        <span class="s1">df.to_sql(</span><span class="s2">'dim_date'</span><span class="s0">,</span>
<a name="l273"><span class="ln">273  </span></a>                  <span class="s1">hook.get_sqlalchemy_engine()</span><span class="s0">,</span>
<a name="l274"><span class="ln">274  </span></a>                  <span class="s1">schema=dds_layer</span><span class="s0">,</span>
<a name="l275"><span class="ln">275  </span></a>                  <span class="s1">if_exists=</span><span class="s2">'replace'</span><span class="s0">,</span>
<a name="l276"><span class="ln">276  </span></a>                  <span class="s1">index=</span><span class="s0">False,</span>
<a name="l277"><span class="ln">277  </span></a>                  <span class="s1">dtype={</span><span class="s2">'date'</span><span class="s1">: sqlalchemy.types.DATE()</span><span class="s0">,</span>
<a name="l278"><span class="ln">278  </span></a>                         <span class="s2">'week_start'</span><span class="s1">: sqlalchemy.types.DATE()})</span>
<a name="l279"><span class="ln">279  </span></a>
<a name="l280"><span class="ln">280  </span></a>
<a name="l281"><span class="ln">281  </span></a>    <span class="s1">@task</span>
<a name="l282"><span class="ln">282  </span></a>    <span class="s0">def </span><span class="s1">fact_nds(downloaded_file_path</span><span class="s0">, </span><span class="s1">file_new_name):</span>
<a name="l283"><span class="ln">283  </span></a>        <span class="s5">&quot;&quot;&quot; 
<a name="l284"><span class="ln">284  </span></a>        Забираем из базы обновленные измерения и их ключи. 
<a name="l285"><span class="ln">285  </span></a>        Ключи прежних загрузок остаются неизменными. 
<a name="l286"><span class="ln">286  </span></a>        Преобразуем эти пары в словари и меняем в таблице фактов значения на ключи. 
<a name="l287"><span class="ln">287  </span></a>        Заливаем пока в stage 
<a name="l288"><span class="ln">288  </span></a>        &quot;&quot;&quot;</span>
<a name="l289"><span class="ln">289  </span></a>        <span class="s1">hook = PostgresHook(postgres_conn_id=</span><span class="s2">'postgres_conn'</span><span class="s1">)</span>
<a name="l290"><span class="ln">290  </span></a>        <span class="s1">conn = hook.get_conn()</span>
<a name="l291"><span class="ln">291  </span></a>        <span class="s1">cursor = conn.cursor()</span>
<a name="l292"><span class="ln">292  </span></a>        <span class="s1">cursor.execute(</span><span class="s2">f&quot;&quot;&quot;SET search_path TO </span><span class="s0">{</span><span class="s1">dds_layer</span><span class="s0">}</span><span class="s2">; SELECT * FROM dim_branch;&quot;&quot;&quot;</span><span class="s1">)</span>
<a name="l293"><span class="ln">293  </span></a>        <span class="s1">branch = dict(cursor.fetchall())</span>
<a name="l294"><span class="ln">294  </span></a>        <span class="s1">cursor.execute(</span><span class="s2">f&quot;&quot;&quot;SET search_path TO </span><span class="s0">{</span><span class="s1">dds_layer</span><span class="s0">}</span><span class="s2">; SELECT * FROM dim_city;&quot;&quot;&quot;</span><span class="s1">)</span>
<a name="l295"><span class="ln">295  </span></a>        <span class="s1">city = dict(cursor.fetchall())</span>
<a name="l296"><span class="ln">296  </span></a>        <span class="s1">cursor.execute(</span><span class="s2">f&quot;&quot;&quot;SET search_path TO </span><span class="s0">{</span><span class="s1">dds_layer</span><span class="s0">}</span><span class="s2">; SELECT * FROM dim_customer_type;&quot;&quot;&quot;</span><span class="s1">)</span>
<a name="l297"><span class="ln">297  </span></a>        <span class="s1">customer_type = dict(cursor.fetchall())</span>
<a name="l298"><span class="ln">298  </span></a>        <span class="s1">cursor.execute(</span><span class="s2">f&quot;&quot;&quot;SET search_path TO </span><span class="s0">{</span><span class="s1">dds_layer</span><span class="s0">}</span><span class="s2">; SELECT * FROM dim_gender;&quot;&quot;&quot;</span><span class="s1">)</span>
<a name="l299"><span class="ln">299  </span></a>        <span class="s1">gender = dict(cursor.fetchall())</span>
<a name="l300"><span class="ln">300  </span></a>        <span class="s1">cursor.execute(</span><span class="s2">f&quot;&quot;&quot;SET search_path TO </span><span class="s0">{</span><span class="s1">dds_layer</span><span class="s0">}</span><span class="s2">; SELECT * FROM dim_product_line;&quot;&quot;&quot;</span><span class="s1">)</span>
<a name="l301"><span class="ln">301  </span></a>        <span class="s1">product_line = dict(cursor.fetchall())</span>
<a name="l302"><span class="ln">302  </span></a>        <span class="s1">cursor.execute(</span><span class="s2">f&quot;&quot;&quot;SET search_path TO </span><span class="s0">{</span><span class="s1">dds_layer</span><span class="s0">}</span><span class="s2">; SELECT * FROM dim_payment;&quot;&quot;&quot;</span><span class="s1">)</span>
<a name="l303"><span class="ln">303  </span></a>        <span class="s1">payment = dict(cursor.fetchall())</span>
<a name="l304"><span class="ln">304  </span></a>        <span class="s1">cursor.close()</span>
<a name="l305"><span class="ln">305  </span></a>        <span class="s1">conn.close()</span>
<a name="l306"><span class="ln">306  </span></a>        <span class="s1">df = pd.read_csv(</span><span class="s2">f&quot;</span><span class="s0">{</span><span class="s1">downloaded_file_path</span><span class="s0">}</span><span class="s2">/</span><span class="s0">{</span><span class="s1">file_new_name</span><span class="s0">}</span><span class="s2">&quot;</span><span class="s1">)</span>
<a name="l307"><span class="ln">307  </span></a>        <span class="s1">df.columns = [column_title.lower().replace(</span><span class="s2">' '</span><span class="s0">, </span><span class="s2">'_'</span><span class="s1">) </span><span class="s0">for </span><span class="s1">column_title </span><span class="s0">in </span><span class="s1">df.columns]</span>
<a name="l308"><span class="ln">308  </span></a>        <span class="s1">df[</span><span class="s2">'date'</span><span class="s1">] = pd.to_datetime(df[</span><span class="s2">'date'</span><span class="s1">]</span><span class="s0">, </span><span class="s1">format=</span><span class="s2">&quot;%m/%d/%Y&quot;</span><span class="s1">)</span>
<a name="l309"><span class="ln">309  </span></a>        <span class="s1">df[</span><span class="s2">'time'</span><span class="s1">] = pd.to_datetime(df[</span><span class="s2">'time'</span><span class="s1">]</span><span class="s0">, </span><span class="s1">format=</span><span class="s2">&quot;%H:%M&quot;</span><span class="s1">).dt.time</span>
<a name="l310"><span class="ln">310  </span></a>        <span class="s1">df[</span><span class="s2">'branch'</span><span class="s1">] = df[</span><span class="s2">'branch'</span><span class="s1">].map({v: k </span><span class="s0">for </span><span class="s1">k</span><span class="s0">, </span><span class="s1">v </span><span class="s0">in </span><span class="s1">branch.items()})</span>
<a name="l311"><span class="ln">311  </span></a>        <span class="s1">df[</span><span class="s2">'city'</span><span class="s1">] = df[</span><span class="s2">'city'</span><span class="s1">].map({v: k </span><span class="s0">for </span><span class="s1">k</span><span class="s0">, </span><span class="s1">v </span><span class="s0">in </span><span class="s1">city.items()})</span>
<a name="l312"><span class="ln">312  </span></a>        <span class="s1">df[</span><span class="s2">'customer_type'</span><span class="s1">] = df[</span><span class="s2">'customer_type'</span><span class="s1">].map({v: k </span><span class="s0">for </span><span class="s1">k</span><span class="s0">, </span><span class="s1">v </span><span class="s0">in </span><span class="s1">customer_type.items()})</span>
<a name="l313"><span class="ln">313  </span></a>        <span class="s1">df[</span><span class="s2">'gender'</span><span class="s1">] = df[</span><span class="s2">'gender'</span><span class="s1">].map({v: k </span><span class="s0">for </span><span class="s1">k</span><span class="s0">, </span><span class="s1">v </span><span class="s0">in </span><span class="s1">gender.items()})</span>
<a name="l314"><span class="ln">314  </span></a>        <span class="s1">df[</span><span class="s2">'product_line'</span><span class="s1">] = df[</span><span class="s2">'product_line'</span><span class="s1">].map({v: k </span><span class="s0">for </span><span class="s1">k</span><span class="s0">, </span><span class="s1">v </span><span class="s0">in </span><span class="s1">product_line.items()})</span>
<a name="l315"><span class="ln">315  </span></a>        <span class="s1">df[</span><span class="s2">'payment'</span><span class="s1">] = df[</span><span class="s2">'payment'</span><span class="s1">].map({v: k </span><span class="s0">for </span><span class="s1">k</span><span class="s0">, </span><span class="s1">v </span><span class="s0">in </span><span class="s1">payment.items()})</span>
<a name="l316"><span class="ln">316  </span></a>        <span class="s1">df.to_sql(</span><span class="s2">'fact_sales'</span><span class="s0">, </span><span class="s1">hook.get_sqlalchemy_engine()</span><span class="s0">, </span><span class="s1">schema=nds_layer</span><span class="s0">, </span><span class="s1">if_exists=</span><span class="s2">'replace'</span><span class="s0">, </span><span class="s1">index=</span><span class="s0">False</span><span class="s1">)</span>
<a name="l317"><span class="ln">317  </span></a>
<a name="l318"><span class="ln">318  </span></a>
<a name="l319"><span class="ln">319  </span></a>    <span class="s1">file_name = extract_from_s3(raw_key</span><span class="s0">, </span><span class="s1">raw_bucket</span><span class="s0">, </span><span class="s1">raw_local_path)</span>
<a name="l320"><span class="ln">320  </span></a>    <span class="s1">downloaded_file_path = rename_extracted_file(file_name</span><span class="s0">, </span><span class="s1">file_new_name)</span>
<a name="l321"><span class="ln">321  </span></a>    <span class="s1">branch = dim_branch(downloaded_file_path</span><span class="s0">, </span><span class="s1">file_new_name)</span>
<a name="l322"><span class="ln">322  </span></a>    <span class="s1">city = dim_city(downloaded_file_path</span><span class="s0">, </span><span class="s1">file_new_name)</span>
<a name="l323"><span class="ln">323  </span></a>    <span class="s1">customer_type = dim_customer_type(downloaded_file_path</span><span class="s0">, </span><span class="s1">file_new_name)</span>
<a name="l324"><span class="ln">324  </span></a>    <span class="s1">gender = dim_gender(downloaded_file_path</span><span class="s0">, </span><span class="s1">file_new_name)</span>
<a name="l325"><span class="ln">325  </span></a>    <span class="s1">product_line = dim_product_line(downloaded_file_path</span><span class="s0">, </span><span class="s1">file_new_name)</span>
<a name="l326"><span class="ln">326  </span></a>    <span class="s1">payment = dim_payment(downloaded_file_path</span><span class="s0">, </span><span class="s1">file_new_name)</span>
<a name="l327"><span class="ln">327  </span></a>    <span class="s1">time_ = dim_time()</span>
<a name="l328"><span class="ln">328  </span></a>    <span class="s1">date_ = dim_date()</span>
<a name="l329"><span class="ln">329  </span></a>    <span class="s1">fact_sales = fact_nds(downloaded_file_path</span><span class="s0">, </span><span class="s1">file_new_name)</span>
<a name="l330"><span class="ln">330  </span></a>
<a name="l331"><span class="ln">331  </span></a>    <span class="s1">task_s3_sensor &gt;&gt; file_name &gt;&gt; downloaded_file_path &gt;&gt; [</span>
<a name="l332"><span class="ln">332  </span></a>        <span class="s1">branch</span><span class="s0">, </span><span class="s1">city</span><span class="s0">, </span><span class="s1">customer_type</span><span class="s0">, </span><span class="s1">gender</span><span class="s0">, </span><span class="s1">product_line</span><span class="s0">, </span><span class="s1">payment</span>
<a name="l333"><span class="ln">333  </span></a>    <span class="s1">] &gt;&gt; task_update_dims</span>
<a name="l334"><span class="ln">334  </span></a>    <span class="s1">task_update_dims &gt;&gt; fact_sales &gt;&gt; task_update_fact &gt;&gt; task_clear_data_directory</span>
<a name="l335"><span class="ln">335  </span></a>    <span class="s1">task_s3_sensor &gt;&gt; task_create_tables &gt;&gt; [time_</span><span class="s0">, </span><span class="s1">date_]</span>
<a name="l336"><span class="ln">336  </span></a>    <span class="s1">downloaded_file_path &gt;&gt; task_delete_s3_obj</span></pre>
</body>
</html>


## BI superset
Поскольку на территории РФ использование Tableau невозможно, а MS Power BI выпускается только для OS Windows будем использовать superset
1. Подключаемся по дефорлтным логину и паролю ![superset_connection](images/superset_connection.png)
2. Создаем подключение к хранилищу на postgres ![postgres connection](images/postgres_connection.png)
3. В качестве витрины создаем датасет sql запросом. В нашем случае получается денормализованная таблица с расширенными измерениями даты и времени ![dns_mart](images/dns_mart.png)
## 👉 [Денормализованная таблица - витрина](sqllab_dns_sales_20230430T130150.csv) 👈
4. Создаем чарты и собираем их в дашборд ![dashboard](images/dashboard.jpg)
5. В UI superset без труда можно настроить автоматическое обновление и рассылку ![refresh](images/refresh.png)