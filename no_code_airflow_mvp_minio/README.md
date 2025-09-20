# No‑Code Airflow DAG Generator (MVP)

Минимальный прототип, который берет YAML‑описание пайплайна и генерирует Python‑файл с DAG для Airflow.

## Что уже умеет (MVP)
- Источник: локальный CSV (`local_csv`)
- Трансформации: `select`, `cast` (`int`, `float`, `timestamp`, `string`), `filter` (выражение pandas `DataFrame.query`)
- Качество данных: `not_null`, `unique`, `row_count_min`
- Назначение: Postgres через Airflow Connection (`conn_id`) с режимами `append`/`replace`

> Секреты не хранятся в YAML — используйте Airflow Connections.

## Установка
```bash
# (опционально) создать виртуальное окружение
pip install -r requirements.txt
```

## Генерация DAG
Вариант A — из терминала:
```bash
python generator/generate.py examples/pipeline_localcsv_to_pg.yaml --out dags/pipeline_localcsv_to_pg.py
```

Вариант B — из Jupyter:
```python
from generator.generate import generate_dag
generate_dag("examples/pipeline_localcsv_to_pg.yaml", out_path="dags/pipeline_localcsv_to_pg.py")
```

## Деплой в Airflow
- Скопируйте сгенерированный файл из `dags/` в папку DAGs вашего Airflow (или настройте git‑sync).
- В Airflow создайте Connection `pg_dw` (Conn Type: Postgres) с вашими параметрами.
- Установите зависимости рантайма в образ Airflow: `pandas`, `sqlalchemy`, `psycopg2-binary`.
  Пример:
  ```bash
  pip install pandas sqlalchemy psycopg2-binary
  ```

## YAML DSL (минимум)
- `source.type`: `local_csv`
- `transforms`: `select`/`cast`/`filter`
- `target.type`: `postgres` с `conn_id` и `table`
- `quality_checks`: `not_null`/`unique`/`row_count_min`

## Ограничения MVP
- Только локальный CSV как источник.
- `target.mode`: `append`/`replace` (без upsert).
- Простые трансформации; выражения фильтра — синтаксис pandas `query`.
- Без сенсоров/датасетов/линнейджа (добавим на следующем шаге).


## Подключение MinIO (через S3 совместимость)
В Airflow создайте Connection `minio_s3`:
- Conn Type: **Amazon Web Services** (или **S3** в старых провайдерах)
- Login: ваш **Access Key**
- Password: ваш **Secret Key**
- Extras (JSON):
  ```json
  {
    "endpoint_url": "http://<VM-IP>:9000",
    "region_name": "us-east-1"
  }
  ```

В рантайме Airflow установите зависимости:
```bash
pip install s3fs apache-airflow-providers-amazon
```

Сгенерируйте DAG из примера MinIO:
```bash
python generator/generate.py examples/pipeline_minio_to_pg.yaml --out dags/pipeline_minio_to_pg.py
```
