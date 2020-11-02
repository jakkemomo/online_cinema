# Online Cinema  


## Yandex Praktikum    
- [x] First stage: ETL from Sqlite to Elasticsearch  
- [x] Second stage: Flask API to communicate with Elasticsearch   
- [ ] Third stage: Django admin panel for content control




## Startup sequence
1. sudo docker run -p 9200:9200 -e "discovery.type=single-node" docker.elastic.co/elasticsearch/elasticsearch:7.9.2
2. Import ES scheme: https://code.s3.yandex.net/middle-python/learning-materials/es_schema.txt
3. Run etl.py
4. Run app.py
