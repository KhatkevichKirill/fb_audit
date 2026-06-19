-- -------------------------------------------------------
-- insights_breakdowns_demographic — per-ad daily metrics broken down by age x gender
-- (insights_breakdowns_update.py also creates these IF NOT EXISTS at runtime;
--  this file is the explicit DDL for review / migration.)
-- -------------------------------------------------------
CREATE TABLE IF NOT EXISTS insights_breakdowns_demographic (
  account_id                    TEXT,
  campaign_id                   TEXT,
  adset_id                      TEXT,
  ad_id                         TEXT,
  date_start                    DATE,
  date_stop                     DATE,
  age                           TEXT,
  gender                        TEXT,
  impressions                   TEXT,
  reach                         TEXT,
  clicks                        TEXT,
  spend                         TEXT,
  unique_inline_link_clicks     TEXT,
  inline_link_clicks            TEXT,
  inline_post_engagement        TEXT,
  estimated_ad_recallers        TEXT,
  estimated_ad_recall_rate      TEXT,
  objective                     TEXT,
  unique_clicks                 TEXT,
  actions                       JSONB,
  action_values                 JSONB,
  outbound_clicks               JSONB,
  unique_actions                JSONB,
  unique_outbound_clicks        JSONB,
  video_p25_watched_actions     JSONB,
  video_p50_watched_actions     JSONB,
  video_p75_watched_actions     JSONB,
  video_p95_watched_actions     JSONB,
  results                       JSONB,
  cost_per_result               JSONB
);
CREATE INDEX IF NOT EXISTS idx_ibd_ad_id      ON insights_breakdowns_demographic(ad_id);
CREATE INDEX IF NOT EXISTS idx_ibd_date       ON insights_breakdowns_demographic(date_start);
CREATE INDEX IF NOT EXISTS idx_ibd_account    ON insights_breakdowns_demographic(account_id, date_start);
CREATE INDEX IF NOT EXISTS idx_ibd_age_gender ON insights_breakdowns_demographic(age, gender);

CREATE TABLE IF NOT EXISTS insights_breakdowns_demographic_log (
  account_id     TEXT,
  date           DATE,
  with_data      BOOLEAN,
  recording_date TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_ibd_log_account ON insights_breakdowns_demographic_log(account_id, date);

-- -------------------------------------------------------
-- insights_breakdowns_placement — per-ad daily metrics broken down by
-- publisher_platform x platform_position x impression_device
-- -------------------------------------------------------
CREATE TABLE IF NOT EXISTS insights_breakdowns_placement (
  account_id                    TEXT,
  campaign_id                   TEXT,
  adset_id                      TEXT,
  ad_id                         TEXT,
  date_start                    DATE,
  date_stop                     DATE,
  publisher_platform            TEXT,
  platform_position             TEXT,
  impression_device             TEXT,
  impressions                   TEXT,
  reach                         TEXT,
  clicks                        TEXT,
  spend                         TEXT,
  unique_inline_link_clicks     TEXT,
  inline_link_clicks            TEXT,
  inline_post_engagement        TEXT,
  estimated_ad_recallers        TEXT,
  estimated_ad_recall_rate      TEXT,
  objective                     TEXT,
  unique_clicks                 TEXT,
  actions                       JSONB,
  action_values                 JSONB,
  outbound_clicks               JSONB,
  unique_actions                JSONB,
  unique_outbound_clicks        JSONB,
  video_p25_watched_actions     JSONB,
  video_p50_watched_actions     JSONB,
  video_p75_watched_actions     JSONB,
  video_p95_watched_actions     JSONB,
  results                       JSONB,
  cost_per_result               JSONB
);
CREATE INDEX IF NOT EXISTS idx_ibp_ad_id    ON insights_breakdowns_placement(ad_id);
CREATE INDEX IF NOT EXISTS idx_ibp_date     ON insights_breakdowns_placement(date_start);
CREATE INDEX IF NOT EXISTS idx_ibp_account  ON insights_breakdowns_placement(account_id, date_start);
CREATE INDEX IF NOT EXISTS idx_ibp_platform ON insights_breakdowns_placement(publisher_platform, platform_position);

CREATE TABLE IF NOT EXISTS insights_breakdowns_placement_log (
  account_id     TEXT,
  date           DATE,
  with_data      BOOLEAN,
  recording_date TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_ibp_log_account ON insights_breakdowns_placement_log(account_id, date);
