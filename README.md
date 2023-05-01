### Airflow
1. Скачиваем в директорию с проектом файл докер композ `curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.3.3/docker-compose.yaml'`
2. Создаем директории `mkdir -p ./dags ./logs ./plugins`
3. Инициализируем базу __AIRFLOW__ командой `docker compose up airflow-init`. Сборка завершена с кодом 0 - т.е. успешно
4. Запускаем остальные контейнеры с севисами __AIRFLOW__ `docker-compose up -d`
### DWH
5. Поднимаем контейнер с __DWH на posrgres__. Переходим в папку `postgres_docker` и находясь там запускаем `docker-compose up -d`
### BI
6. Поднимаем контейнер с __SUPERSET__. Переходим в папку `superset_docker` и находясь там запускаем `docker-compose -f docker-compose-non-dev.yml`