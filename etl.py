import logging
import json
import sqlite3
from elasticsearch import Elasticsearch

conn = sqlite3.connect('db.sqlite')


def fix_database():
    conn.execute("delete from writers where name = 'N/A'")
    conn.execute("delete from actors where name = 'N/A'")
    conn.execute("update movies set director = null where director = 'N/A'")
    conn.execute("update movies set plot = null where plot = 'N/A'")
    conn.execute("update movies set imdb_rating = null where imdb_rating = 'N/A'")
    movie_actor_dupes = conn.execute("select movie_id, actor_id, count(*) as cnt from movie_actors group by movie_id, actor_id having count (*)>1").fetchall()
    for movie_actor in movie_actor_dupes:
        query = f"-- delete from movie_actors where movie_id = '{movie_actor[0]}' and actor_id = '{movie_actor[1]}'"
        conn.execute(query)

    conn.commit()


def create_new_object(index, id, body):
    try:
        es.create(index, id, body, doc_type=None, params=None, headers=None)
    except Exception as e:
        logging.info(e)


def get_writer_data(writer, writers):
    writer_ids = get_writer_ids(writer, writers)
    writers_sql_info = []
    for writer_id in set(writer_ids):
        query = f"""select id, name from writers where writers.id = '{writer_id}'"""
        writers_sql_info.extend(conn.execute(query).fetchall())
    writer_names = [','.join([actor[1] for actor in writers_sql_info])]
    writers_list = []
    for writer in writers_sql_info:
        writers_list.append(dict(id=writer[0], name=writer[1]))
    return writers_list, writer_names


def get_writer_ids(writer, writers):
    if writers:
        writer_ids = [writer.get('id') for writer in json.loads(writers)]
    else:
        writer_ids = [writer]
    return writer_ids


def get_actor_data(movie_id):
    query = f"select actors.id, actors.name from movie_actors  inner join actors on actor_id=actors.id  where movie_id = '{movie_id}'"
    actors = conn.execute(query).fetchall()
    actor_list = []
    actor_names = [','.join([actor[1] for actor in actors])]
    for actor in actors:
        actor_list.append(dict(id=actor[0], name=actor[1]))
    return actor_list, actor_names


def import_data():
    query = "select id, imdb_rating, genre, title, plot, director, writer, writers from movies"
    movies = conn.execute(query).fetchall()
    for movie in movies:
        writer = movie[6]
        writers = movie[7]
        writer_data, writers_names = get_writer_data(writer, writers)
        movie_id = movie[0]
        actor_data, actors_names = get_actor_data(movie_id)
        rating = float(movie[1]) if movie[1] and isinstance(movie[1], str) else None
        movie_body = dict(id=movie_id, imdb_rating=rating, genre=movie[2], title=movie[3], description=movie[4],
                          director=movie[5], actors_names=actors_names, writers_names=writers_names, actors=actor_data,
                          writers=writer_data)
        create_new_object(index="movies", id=movie_id, body=movie_body)


if __name__ == '__main__':
    fix_database()
    es = Elasticsearch()
    import_data()
