import json
import requests

from flask import Flask, request, jsonify

app = Flask('movies_service')


@app.route('/api/movies/<movie_id>', methods=['GET'])
def movie_details(movie_id: str) -> str:
    """
    :param movie_id: идентификатор фильма
    :return: данные из ES о фильме
    """
    url = f'http://127.0.0.1:9200/movies/_doc/{movie_id}'
    response = requests.get(url)
    json_data = json.loads(response.text).get('_source')
    if json_data is not None:
        # Необходимые преобразования для прохождения теста в Postman
        json_data['genre'] = json_data.get('genre', ',').split(', ')
        json_data['director'] = json_data.get('director', ', ').split(', ')
        # Необходимая сортировка для прохождения теста в Postman
        json_data['writers'] = sorted(json_data['writers'], key=lambda i: i['name'].split(' ')[1])
        data = dict(id=json_data.get('id'),
                    title=json_data.get('title'),
                    description=json_data.get('description'),
                    imdb_rating=json_data.get('imdb_rating'),
                    writers=json_data.get('writers'),
                    actors=json_data.get('actors'),
                    genre=json_data.get('genre'),
                    director=json_data.get('director'))
        return jsonify(data)
    else:
        if response.status_code == 404:
            return "Link is not found", 404
        else:
            return "Error", response.status_code


@app.route('/api/movies', methods=['GET'], strict_slashes=False)
def movies_list() -> str:
    """
    :return: данные из ES об отфильтрованном по request.args списке фильмов
    """
    try:
        search: str = request.args.get('search', '')
        limit: int = int(request.args.get('limit', 50))
        page: int = int(request.args.get('page', 1))
        sort: str = request.args.get('sort', 'id')
        sort_order: str = request.args.get('sort_order', 'asc')
    except Exception:
        return "Unprocessable Entity", 422
    if limit <= 0 or page <= 0 or sort_order not in ['asc', 'desc'] or sort not in ['title', 'imdb_rating', 'id']:
        return "Unprocessable Entity", 422
    url = f'http://127.0.0.1:9200/movies/_search'
    from_value = page * limit - limit

    if search:
        query = {'query': dict(multi_match=dict(query=search,
                                                fields=["title^5", "description^4",
                                                        "genre^2",
                                                        "actors_names^3",
                                                        "writers_names",
                                                        "director"
                                                        ])),
                 'size': limit,
                 'from': from_value,
                 "sort": [{sort: {"order": sort_order}}]}
    else:
        query = {
            "size": limit,
            "from": from_value,
            "sort": [{sort: {"order": sort_order}}],
            "query":
                {
                    "match_all": {}
                },
        }

    response = requests.post(
        url,
        data=json.dumps(query),
        headers={'Content-Type': 'application/json'}
    )
    if response.status_code == 200:
        results = json.loads(response.text).get('hits', {}).get('hits', {})
        data = []
        for result in results:
            source_data = result.get('_source')
            data.append(dict(id=source_data['id'], title=source_data['title'], imdb_rating=source_data['imdb_rating']))
        return jsonify(data)
    elif response.status_code == 400:
        return "Invalid request body format", 400
    elif response.status_code == 422:
        return "Invalid request body", 400
    else:
        return "Link not found", 404


if __name__ == '__main__':
    app.run(port=8000)
