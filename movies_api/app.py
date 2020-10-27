import json
import requests

from flask import Flask, request

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
    return json_data


@app.route('/api/movies', methods=['GET'], strict_slashes=False)
def movies_list() -> str:
    """
    :return: данные из ES об отфильтрованном по request.args списке фильмов
    """
    search: str = request.args.get('search', '')
    limit: int = int(request.args.get('limit', 50))
    page: int = int(request.args.get('page', 1))
    sort: str = request.args.get('sort', 'id')
    sort_order: str = request.args.get('sort_order', 'asc')
    url = f'http://127.0.0.1:9200/movies/_search'
    query = {'query': dict(multi_match=dict(query=search,
                                            fields=["title^5", "description^4",
                                                    "genre^2",
                                                    "actors_names^3",
                                                    "writers_names",
                                                    "director"
                                                    ],
                                            fuzziness="auto")),
             'size': limit,
             'from': page * limit - 50,
             "sort": [{sort: {"order": sort_order}}]}
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
        return str(data)
    elif response.status_code == 400:
        return "Неправильный формат тела запроса", 400
    elif response.status_code == 422:
        return "Неправильное тело запроса", 400


if __name__ == '__main__':
    app.run(port=8000)
