# KOI Configuration

`personal.env.example` is the node's settings template. Copy it to `personal.env` and fill
in your values; do not commit the copy. It lists each variable the node reads that you are
likely to set, with a comment, including the access-control and service-token settings
described in the quickstart's
[Access control](../docs/operations/quickstart-personal-koi.md#7-access-control) section.

### Personal config: `*.example` template pattern

The sensors and the batch wrapper read their choices from files that follow the same
pattern: the `*.example.*` file is a generic template; copy it to the name without
`.example` and edit the copy.

- `substack_publications.example.yaml` → `substack_publications.yaml`: Substacks for
  `scripts/substack_sensor.py`
- `rss_feeds.example.yaml` → `rss_feeds.yaml`: feeds for `scripts/rss_sensor.py`
- `extract-batch.env.example` → `extract-batch.env`: extraction transport override for
  one-off batches through `scripts/run_batch_extract.sh`

## Local Env Loading

Use this pattern when running processes that spawn child processes:

```bash
set -a; source config/personal.env; set +a
```

This safely exports all variables (including quoted values) to child processes.
