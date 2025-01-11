# freshrss-db2meili

Import articles from FreshRSS database (Mysql/MariaDB) into MeiliSearch

## Installation

```bash
pip install -e .
apt intall pandoc -y # for markdown conversion
```

Verify that pandoc can convert from html to markdown:

```bash
$ pandoc --list-input-formats | grep html
html
$ pandoc --list-output-formats | grep markdown
markdown
markdown_github
markdown_mmd
markdown_phpextra
markdown_strict
$ echo '<h1>hahaha</h1><p>eeeee</p>' | pandoc --from html --to markdown
hahaha
======

eeeee
```

markdown
markdown_github
markdown_mmd
markdown_phpextra
markdown_strict
commonmark
commonmark_x

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
