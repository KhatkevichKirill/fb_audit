

В проекте следующие файлы
- actions — Данные по действиям пользователей - когда кто и как произвёл определённые действия. Например когда поменялась ставка или бюджет, когда было остановлено или запущено объявление/адсет/кампания и пр.
- account_atribute — Технические данные по аккаунту, такие как, название, выбранная валюта, настройки и пр.
- ad_atribute — Технические данные по креативу, такие как, название, id, статус, хэш картинки, данные по таргетингу(тянутся из настроек адсета)
- adset_atribute — Технические данные по адсету - название, id, таргетинги, гео, ставка, бюджет и пр.
- campaign_atribute — Технические данные по кампании
- creative_atribute — тех данные по креативу - текст объявления, CTA, utm метки, привязанная страница, дата создания и пр.
- insights — инсайты
- insights_update — обновление инсайтов(проверяем что уже есть и забираем только то, чего не хватает)

## Актуализация (2026-04)

- В `insights` и `insights_update` обновлён API version для проверки rate limit: `v19.0`.
- Удалены депрекейтнутые attribution windows: `7d_view`, `28d_view`.
- В запрос инсайтов добавлены поля `results` и `cost_per_result` (для app installs/trials).
- В схеме таблицы `insights` добавлены колонки `results JSONB` и `cost_per_result JSONB`, плюс `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` для уже существующих БД.
- В `actions` добавлены:
  - нормализация `date_time_in_timezone`/`event_time` в ISO-формат,
  - JSON-сериализация `extra_data`,
  - параметризованная запись в `actions_log`,
  - override периода через `ACTIONS_START_DATE` / `ACTIONS_END_DATE`.
- В `account_atribute`, `campaign_atribute`, `adset_atribute`, `ad_atribute`, `creative_atribute`:
  - унифицирована обработка ошибок (включая `code=190` и `error_subcode`),
  - включена безопасная запись только существующих колонок таблицы (`information_schema.columns`),
  - расширены `exclude_list` по полям, которые в новых версиях API чаще ломают совместимость.
- В `ad_atribute` включён upsert через `ON CONFLICT (id) DO UPDATE`.
- В `creative_atribute` убран устаревший `REFRESH MATERIALIZED VIEW mview_audit_by_offers` (view отсутствует в текущей схеме).
- Общие функции вынесены в `utils.py` и подключены в ноутбуках:
  - подключение/reconnect к БД,
  - безопасная фильтрация колонок по схеме таблицы,
  - нормализация payload для `actions` и `*_atribute`,
  - фильтрация аккаунтов через `ACCOUNT_IDS`.

## Документация / Documentation

- Подробный двуязычный (RU/EN) гайд по архитектуре и пайплайну:
  - `PIPELINE_GUIDE_RU_EN.md`
