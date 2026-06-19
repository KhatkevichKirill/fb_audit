# fb_audit Pipeline Guide (RU / EN)

## RU — Подробное описание

### 1) Назначение проекта

`fb_audit` — это ETL-пайплайн для Meta Ads API. Он регулярно:
- забирает журнал изменений (`actions`);
- загружает атрибуты сущностей (`account/campaign/adset/ad/creative`);
- загружает performance-метрики (`insights`, `insights_update`, `intraday_insights.py`);
- пишет все в PostgreSQL-таблицы для аналитики и автоматизаций.

### 2) Основные файлы и их роль

- `actions.py`  
  Загружает историю изменений объектов из Meta (кто/что/когда изменил), пишет в `actions` и `actions_log`.

- `account_atribute.py`  
  Загружает атрибуты рекламных аккаунтов в `property_accounts`.

- `campaign_atribute.py`  
  Загружает атрибуты кампаний в `property_campaigns`.

- `adset_atribute.py`  
  Загружает атрибуты adset в `property_adsets`.

- `ad_atribute.py`  
  Загружает атрибуты ads в `property_ads` (upsert по `id`).

- `creative_atribute.py`  
  Загружает атрибуты креативов в `property_creatives`.

- `insights.py`  
  Историческая загрузка дневных инсайтов (date-range backfill).

- `insights_update.py`  
  Инкрементальная дневная догрузка инсайтов + атомарный refetch последних 7 дней.

- `insights_breakdowns_update.py`  
  Performance по age×gender и по placement → `insights_breakdowns_demographic`, `insights_breakdowns_placement`.

- `intraday_insights.py`  
  Внутридневной `today`-срез по ad-level: на каждом запуске удаляет и пересобирает данные по каждому аккаунту в `intraday_insights`.

- `utils.py`  
  Общие функции (подключение к БД, reconnection, фильтрация колонок по схеме, нормализация payload, account filtering, универсальные DB-хелперы).

### 3) Поток данных (высокоуровнево)

1. Meta API -> `actions`  
   Сначала загружается журнал изменений, чтобы понять, какие объекты реально поменялись.

2. `actions` + `insights` -> `property_*`  
   Атрибутные скрипты выбирают кандидатов:
   - объекты с новыми событиями после `recording_date`;
   - или объекты, которые есть в `insights`, но еще отсутствуют в `property_*`.

3. Meta API -> `insights`  
   Загружаются performance-метрики по `ad` level.

4. Meta API ошибки 100/33 и 100/1487221 -> `deleted_objects`  
   Удаленные/недоступные сущности tombstone-ятся, чтобы не дергать их бесконечно.

### 4) Логика инкрементальности

- `actions_log` хранит, какие `(account_id, date)` уже были обработаны в `actions`.
- `insights_log` хранит, какие `(account_id, date)` уже были загружены в `insights`.
- `property_*` обновляются не полным сканом, а только по candidate query.

Это снижает нагрузку на API и ускоряет ежедневный прогон.

### 5) Актуальные изменения API/метрик

В `insights` и `insights_update`:
- API-version для throttle-проверки берется из `FB_GRAPH_API_VERSION` (по умолчанию `v23.0`);
- attribution windows: `1d_view`, `1d_click`, `7d_click`, `28d_click`;
- добавлены поля `results` и `cost_per_result` (в т.ч. важны для app installs/trials);
- в схему добавлены `results JSONB` и `cost_per_result JSONB` + `ALTER TABLE ... ADD COLUMN IF NOT EXISTS`.

### 6) Унификация через `utils.py`

Вынесены и используются общие функции:
- `connection_to_database(...)`
- `reconnection_to_database(...)`
- `get_table_columns(...)`
- `modify_object_data(...)`
- `modify_action_data(...)`
- `get_allowed_account_ids(...)`
- `store_object(...)`
- `delete_object(...)`
- `store_deleted_object(...)`

Это уменьшает дублирование, упрощает поддержку и снижает риск расхождения логики между ноутбуками.

### 7) Обработка ошибок

- `FacebookRequestError code=190` -> считается критичным (проблема токена).
- `code=100` + `error_subcode in (33, 1487221)` -> объект пишется в `deleted_objects`.
- Остальные ошибки логируются и скрипт продолжает обработку следующих сущностей.

### 8) Переменные окружения

Минимально нужны:
- `FB_ACCESS_TOKEN`
- `DB_HOST`
- `DB_PORT`
- `DB_NAME`
- `DB_USER`
- `DB_PASSWORD`

Дополнительно:
- `ACCOUNT_IDS` — ограничение списка аккаунтов через whitelist.
- `ACTIONS_START_DATE`, `ACTIONS_END_DATE` — backfill-окно для `actions`.
- `INSIGHTS_START_DATE`, `INSIGHTS_END_DATE` — backfill-окно для `insights`/`insights_update`.

### 9) Почему важен `ACCOUNT_IDS`

`ACCOUNT_IDS` позволяет:
- изолировать только рабочие аккаунты;
- безопасно запускать частичные бэкфиллы;
- не тратить лимиты API на нецелевые аккаунты.

Поддерживаются значения как `12345`, так и `act_12345`.

### 10) Практический порядок запуска

Рекомендуемый порядок:
1. `actions.py`
2. `account_atribute.py`
3. `campaign_atribute.py`
4. `adset_atribute.py`
5. `ad_atribute.py`
6. `creative_atribute.py`
7. `insights_update.py` (или `insights.py` для backfill)
8. `insights_breakdowns_update.py`
9. `intraday_insights.py` (по расписанию в течение дня)

Так гарантируется, что атрибуты обновляются на базе свежих изменений и свежих performance-данных.

### 11) Аналитический слой (опционально)

Для NL→SQL анализа поверх той же БД см.
[data_analyst-fb_audit](https://github.com/KhatkevichKirill/data_analyst-fb_audit):
`data-analyst init-view` (создаёт `v_insights_daily`) → `data-analyst serve`.

---

## EN — Detailed Description

### 1) Project Purpose

`fb_audit` is an ETL pipeline for Meta Ads API. It regularly:
- pulls change history (`actions`);
- updates entity attributes (`account/campaign/adset/ad/creative`);
- fetches performance metrics (`insights`, `insights_update`, `intraday_insights.py`);
- stores data in PostgreSQL for reporting and automations.

### 2) Main Files and Responsibilities

- `actions.py`  
  Pulls Meta activity history and writes to `actions` + `actions_log`.

- `account_atribute.py`  
  Loads account attributes into `property_accounts`.

- `campaign_atribute.py`  
  Loads campaign attributes into `property_campaigns`.

- `adset_atribute.py`  
  Loads ad set attributes into `property_adsets`.

- `ad_atribute.py`  
  Loads ad attributes into `property_ads` (upsert by `id`).

- `creative_atribute.py`  
  Loads creative attributes into `property_creatives`.

- `insights.py`  
  Historical daily insights loader (date-range backfill).

- `insights_update.py`  
  Incremental daily insights updater with atomic last-7-day refetch.

- `insights_breakdowns_update.py`  
  Age×gender and placement breakdowns.

- `intraday_insights.py`  
  Intraday `today` ad-level snapshot. On each run, it fully refreshes account data in `intraday_insights` (delete + insert).

- `utils.py`  
  Shared helpers for DB connectivity, reconnection, schema-safe inserts, payload normalization, account filtering, and common DB write operations.

### 3) Data Flow (High-Level)

1. Meta API -> `actions`  
   Change log is collected first to identify what actually changed.

2. `actions` + `insights` -> `property_*`  
   Attribute scripts build candidate sets from:
   - entities with events newer than `recording_date`;
   - entities present in insights but missing in `property_*`.

3. Meta API -> `insights`  
   Daily performance metrics are collected at `ad` level.

4. Meta API delete/not-accessible errors -> `deleted_objects`  
   Deleted/inaccessible objects are tombstoned and skipped in future runs.

### 4) Incremental Strategy

- `actions_log` tracks processed `(account_id, date)` pairs for actions.
- `insights_log` tracks processed `(account_id, date)` pairs for insights.
- `property_*` updates are candidate-driven, not full table rescans.

This approach reduces API load and runtime.

### 5) API and Metrics Modernization

In `insights` / `insights_update`:
- throttle check API version now comes from `FB_GRAPH_API_VERSION` (default `v23.0`);
- attribution windows now: `1d_view`, `1d_click`, `7d_click`, `28d_click`;
- added `results` and `cost_per_result` fields (important for app installs/trials);
- schema includes `results JSONB` and `cost_per_result JSONB` with safe `ALTER TABLE ... ADD COLUMN IF NOT EXISTS`.

### 6) Shared Logic via `utils.py`

The following helpers are centralized:
- `connection_to_database(...)`
- `reconnection_to_database(...)`
- `get_table_columns(...)`
- `modify_object_data(...)`
- `modify_action_data(...)`
- `get_allowed_account_ids(...)`
- `store_object(...)`
- `delete_object(...)`
- `store_deleted_object(...)`

This reduces duplication and keeps behavior consistent across notebooks.

### 7) Error Handling

- `FacebookRequestError code=190` -> treated as critical (token issue).
- `code=100` + `error_subcode in (33, 1487221)` -> object is inserted into `deleted_objects`.
- Other errors are logged and processing continues for remaining entities.

### 8) Environment Variables

Required:
- `FB_ACCESS_TOKEN`
- `DB_HOST`
- `DB_PORT`
- `DB_NAME`
- `DB_USER`
- `DB_PASSWORD`

Optional:
- `ACCOUNT_IDS` for account whitelist filtering.
- `ACTIONS_START_DATE`, `ACTIONS_END_DATE` for actions backfill windows.
- `INSIGHTS_START_DATE`, `INSIGHTS_END_DATE` for insights backfill windows.

### 9) Why `ACCOUNT_IDS` Matters

`ACCOUNT_IDS` helps to:
- isolate only active/target accounts;
- run safe partial backfills;
- avoid wasting API limits on unrelated accounts.

Both `12345` and `act_12345` formats are supported.

### 10) Recommended Run Order

1. `actions.py`
2. `account_atribute.py`
3. `campaign_atribute.py`
4. `adset_atribute.py`
5. `ad_atribute.py`
6. `creative_atribute.py`
7. `insights_update.py` (or `insights.py` for historical backfill)
8. `insights_breakdowns_update.py`
9. `intraday_insights.py` (scheduled throughout the day)

This sequence keeps entity attributes aligned with latest object changes and performance updates.

### 11) Analysis layer (optional)

For natural-language SQL over the same warehouse, see
[data_analyst-fb_audit](https://github.com/KhatkevichKirill/data_analyst-fb_audit):
run `data-analyst init-view` once, then `data-analyst serve`.
