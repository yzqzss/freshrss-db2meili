import argparse
import asyncio
import os
from typing import List, Sequence, TypedDict
import zlib

from meilisearch_python_sdk import AsyncClient as MeiliAsyncClient
from mysql.connector.cursor import MySQLCursor
from tqdm import tqdm
import meilisearch_python_sdk.errors
import mysql.connector
# from markdownify import MarkdownConverter
from html_to_markdown import convert


from config import (
    CHUNK_SIZE,
    WORKERS,
    FRESHRSS_USERNAME,
    MEILI_KEY,
    MEILI_URL,
    MYSQL_CONTENT_COMPRESSED,
    MYSQL_DATABASE,
    MYSQL_HOST,
    MYSQL_PASSWORD,
    MYSQL_PREFIX,
    MYSQL_USER,
)

die = False

async def markdowify_by_pandoc(content: bytes) -> str:
    proc = await asyncio.create_subprocess_exec(
        "pandoc", "-f", "html", "-t", "markdown",
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )
    stdout, stderr = await proc.communicate(input=content)
    await proc.wait()
    return stdout.decode("utf-8")    

def get_todo_entries_size(cursor: MySQLCursor, meili_max_id: int) -> int:
    # 有多少行
    sql = f"""
SELECT
    COUNT(*)
FROM
    {MYSQL_PREFIX}{FRESHRSS_USERNAME}_entry
WHERE
    id > {meili_max_id};
"""
    print(sql)
    cursor.execute(sql)
    result = cursor.fetchone()[0]
    assert isinstance(result, int)

    return result

class Entry(TypedDict):
    id: int
    id_feed: int
    title: str
    author: list[str] # generated list
    link: str
    date: int
    lastSeen: int
    is_favorite: int
    tags: list[str]   # generated list
    content: str

    content_length: int

def get_entry_data(cursor: MySQLCursor, meili_max_id: int):
    sql = f"""
SELECT 
    id, 
    id_feed, 
    title, 
    author, 
    link, 
    date, 
    lastSeen, 
    is_favorite, 
    tags, 
    {"content_bin AS content" if MYSQL_CONTENT_COMPRESSED else "content"}
FROM
    {MYSQL_PREFIX}{FRESHRSS_USERNAME}_entry
WHERE
    id > {meili_max_id}
ORDER BY id ASC;
"""
    print(sql)
    cursor.execute(sql)
    while entry_chunk := cursor.fetchmany(CHUNK_SIZE):
        columns = [column[0] for column in cursor.description]
        entry_chunk = [dict(zip(columns, row)) for row in entry_chunk]
        entry_chunk: List[Entry]
        # convert bytesarray to string
        for row in entry_chunk:
            if MYSQL_CONTENT_COMPRESSED:
                # https://dev.mysql.com/doc/refman/8.0/en/encryption-functions.html#function_compress
                if len(row["content"]) > 4: # type: bytes
                    row["content"] = zlib.decompress(row["content"][4:])
                else: # empty content
                    row["content"] = b""
            else:
                # TODO:
                # row["content"] = row["content"] ?
                # or row["content"] = row["content"].decode("utf-8") ?
                ...

            assert isinstance(row["content"], bytes)
        yield entry_chunk



async def get_meili_max_id(ml_client: MeiliAsyncClient) -> int:
    stats = await ml_client.index("entry").get_stats()
    if stats.number_of_documents == 0:
        return 0

    r = await ml_client.index("entry").search(
        query="",
        limit=1,
        attributes_to_retrieve=['id'],
        sort=["id:desc"]
    )
    max_id = r.hits[0]['id']
    return max_id


async def main():
    parser = argparse.ArgumentParser()

    # arguments
    parser.add_argument("--init", help="init index", action="store_true")
    parser.add_argument("--delete", help="delete index", action="store_true")

    # parse the args
    args = parser.parse_args()

    # connect to meilisearch
    ml_client = MeiliAsyncClient(MEILI_URL, MEILI_KEY)
    if args.init:
        await ml_client.index('entry').update_searchable_attributes(["title", "content", "author", "link", "tags"])
        await ml_client.index('entry').update_sortable_attributes(["id", "date"])
        await ml_client.index('entry').update_filterable_attributes(["id", "id_feed","content", "author", "tags", "date", "title", "link", "content_length"])
        print("index initialized")
        return
    if args.delete:
        await ml_client.index('entry').delete()
        print("index deleted")
        return
    meili_max_id = await get_meili_max_id(ml_client)

    # connect to mysql
    my_conn = mysql.connector.connect(
        host=MYSQL_HOST,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DATABASE
    )

    my_cursor = my_conn.cursor()
    my_cursor.execute("SHOW TABLES")
    [print(x) for x in my_cursor]

    
    todo_entries_size = get_todo_entries_size(my_cursor, meili_max_id)

    chunked_entries_queue: asyncio.Queue[Sequence[Entry]] = asyncio.Queue(maxsize=WORKERS)

    async def worker(worker_id:int = 0):
        while True:
            entry_chunk = await chunked_entries_queue.get()
            for row in tqdm(entry_chunk, desc=f"worker{worker_id} ...", unit="markdown", unit_scale=1, total=len(entry_chunk)):
                if row["content"]:
                    row["content"] = convert(row["content"].decode("utf-8"))
                else:
                    row["content"] = ""

                row["content_length"] = len(row["content"])-(sum(row["content"].count(x) for x in ["\n", " ", "\t", "\r", "\0"]))

                # author = '' if self.authors is None else ';' + '; '.join(self.authors)
                if row["author"]:
                    row["author"] = row["author"][1:].split("; ")
                else:
                    row["author"] = []
                assert isinstance(row["author"], list)

                if row["tags"]:
                    row["tags"] = row["tags"][1:].split(" #")
                else:
                    row["tags"] = []
                assert isinstance(row["tags"], list)
            

            try:
                task = await ml_client.index("entry").add_documents(
                    documents=entry_chunk,
                    primary_key="id",
                    compress=True
                )
                while True:
                    task_info = await ml_client.get_task(task.task_uid)
                    if task_info.status in ["succeeded", "failed"]:
                        break
                    await asyncio.sleep(0.5)
            except (meilisearch_python_sdk.errors.MeilisearchApiError, Exception) as e:
                print(e)
                print(entry_chunk)
                global die
                die = True

            
            chunked_entries_queue.task_done()



    # start workers
    workers = [worker(i) for i in range(WORKERS)]
    tasks = [asyncio.create_task(worker) for worker in workers]

    for entry_chunk in tqdm(get_entry_data(my_cursor, meili_max_id), total=todo_entries_size//CHUNK_SIZE, desc="importing entry", unit="chunk", unit_scale=CHUNK_SIZE):
        await chunked_entries_queue.put(entry_chunk)
        if die:
            print("die...")
            break
        if os.path.exists("stop"):
            print("stop...")
            break
    
    await chunked_entries_queue.join()
    print('closing workers')
    for task in tasks:
        task.cancel()

    print("done")


if __name__ == "__main__":
    asyncio.run(main())