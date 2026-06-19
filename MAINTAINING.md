# Maintaining this repo

This repository is the **generic core** of a larger private Meta Ads project. It
is kept in sync from that private superset: project-specific logic (automation
rules, creative pipelines, dashboards, internal columns) is deliberately left out,
and only the reusable ETL is published here.

Because of that, changes flow **one way** (private → public) through a small
sync tool, with an automated secret/leak gate so nothing sensitive is ever
published. Pull requests and issues are still welcome — fixes are folded back into
the private source and re-published.

If a file here looks like it was rewritten rather than copied, that's expected:
the public versions are cleaned (config from `.env`, shared helpers, no hardcoded
paths or credentials).
