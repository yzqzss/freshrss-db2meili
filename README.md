# freshrss-db2meili

Import articles from FreshRSS database (Mysql/MariaDB) into MeiliSearch

## Installation

```bash
uv sync
```

## Configuration

```bash
cp config.py.example config.py
```

Edit `config.py` and set the correct configuration.

## Usage

```bash
python freshrss_db2meili.py --init # create the index in MeiliSearch
python freshrss_db2meili.py # Incrementally import entry table from Mysql/MariaDB to MeiliSearch
python freshrss_db2meili.py --delete # Delete the index in MeiliSearch
```
