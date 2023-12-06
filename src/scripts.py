import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import from_json, explode, expr, concat_ws, collect_list, col, split, count, row_number, \
    regexp_replace, broadcast
from pyspark.sql.types import ArrayType, StructType, StructField, IntegerType, StringType, FloatType
from pyspark.sql.utils import AnalysisException

load_dotenv(".env")
movies_metadata_csv_path = os.getenv("MOVIES_METADATA_CSV_PATH")
ratings_csv_path = os.getenv("RATINGS_CSV_PATH")
links_csv_path = os.getenv("LINKS_CSV_PATH")
keywords_csv_path = os.getenv("KEYWORDS_CSV_PATH")


def dataset_extract(spark):
    try:

        movies = spark.read.csv(movies_metadata_csv_path, sep=',', header=True)
        ratings = spark.read.csv(ratings_csv_path, sep=',', header=True)
        links = spark.read.csv(links_csv_path, sep=',', header=True)
        keywords = spark.read.csv(keywords_csv_path, sep=',', header=True)

        return movies, keywords, links, ratings

    except Exception as e:
        print(f'Ошибка при чтении данных: {str(e)}')
        return None



def dataset_transform(keywords, ratings, links, spark):
    try:
        # Вызов функции для извлечения данных
        datasets = dataset_extract(spark)

        # Проверка наличия данных
        if datasets:
            movies, keywords, links, ratings = datasets
        else:
            # Обработка ошибки, если что-то пошло не так при чтении данных
            print('Произошла ошибка при чтении данных. Пожалуйста, проверьте пути к файлам и их формат.')

        ########################################################################################################################################################
        # Преобразуем JSON-подобную строку типа string в массив для корректного считывания данных, а затем выведем ее в виде строки из слов, разделенных запятой

        # Определение схемы для парсинга строки JSON
        json_schema = ArrayType(StructType([
            StructField('id', IntegerType()),
            StructField('name', StringType())
        ]))
        # Преобразование строки в массив JSON-объектов
        keywords = keywords.withColumn('keywords_array', from_json('Keywords', json_schema))

        # Развернуть массив JSON-объектов в отдельные строки
        keywords_exp = keywords.select('id',
                                       explode(expr("transform(keywords_array, x -> x[\"name\"])")).alias('keyword'))
        keywords_exp = keywords_exp.withColumn('id', keywords_exp['id'].cast(IntegerType()))

        # Агрегировать ключевые слова для каждого фильма
        keywords_agg = keywords_exp.groupBy('id').agg(concat_ws(',', collect_list('keyword')).alias('keywords_str'))
        keywords_agg = keywords_agg.drop('keywords_array')
        keywords_agg = keywords_agg.sort('id')

        ###########################################################################################
        # для каждого фильма находим другой похожий фильм, где будет минимум 3 таких же ключвых слова

        # Выберем только нужные колонки
        keywords_selected = keywords_agg.select('id', explode(split(col('keywords_str'), ',')).alias('keyword'))

        # Создаем кросс-джойн таблицы самой с собой
        cross_joined = keywords_selected.crossJoin(
            keywords_selected.withColumnRenamed('id', 'recommend_id').withColumnRenamed('keyword', 'keyword_2'))

        # Фильтруем комбинации с условиями по ключевым словам и id
        filtered_combinations = cross_joined.filter(
            (col('id') != col('recommend_id')) &  # исключаем одинаковые id
            (col('keyword') == col('keyword_2'))  # условие на одинаковые ключевые слова
        )
        common_keywords_count = filtered_combinations.groupBy('id',
                                                              'recommend_id').count()  # добавим столбец count для подсчета слов по каждой группе
        filtered_common_keywords = common_keywords_count.filter(
            col('count') >= 3)  # добавим фильтр на наличие минимум 3х слов

        # Выберем только нужные колонки и отсортируем по id
        filtered_common_keywords = filtered_common_keywords.select('id', 'recommend_id')
        sorted_common_keywords = filtered_common_keywords.orderBy('id')

        ############################################################
        # фильмы которые понравились пользователю(оценка не ниже 3.0)
        ratings = ratings.withColumn('rating', ratings['rating'].cast(FloatType()))
        links = links.withColumnRenamed('imdbId', 'imdb_id')

        user_ratings = ratings.alias('t1') \
            .join(broadcast(links.alias('t2')), col('t1.movieId') == col('t2.movieId'), 'left') \
            .filter(col('t1.rating') > 3.0) \
            .select('t1.userId', 't1.movieId', 't1.rating', 't2.imdb_id')

        #################################################################################################
        # Соединим фильмы, которые понравились пользователю с подборкой похожих фильмов по ключевым словам
        # Присоединение DataFrame
        recommended_movies = user_ratings.join(sorted_common_keywords,
                                               user_ratings['movieId'] == sorted_common_keywords['id'], 'inner')

        # Группировка по 'id' и подсчет количества совпадений ключевых слов
        common_keywords_count = recommended_movies.groupBy('id', 'recommend_id').agg(count('*').alias('count'))

        # Определение окна, сгруппированного по 'id' и упорядоченного по убыванию 'count'
        window_spec = Window.partitionBy('id').orderBy(col('count').desc())

        # Добавление столбца с порядковым номером для каждой группы 'id'
        ranked_filtered_common_keywords = common_keywords_count.withColumn('rank', row_number().over(window_spec))

        # Ограничение результата только теми записями, где 'rank' меньше или равен 3
        filtered_common_keywords_limited = ranked_filtered_common_keywords.filter(col('rank') <= 3)

        # Вывод результата
        filtered_common_keywords_limited = filtered_common_keywords_limited.select('id', 'recommend_id').orderBy('id')

        #################################################################################################
        # Отфильтруем список рекомендаций по высокой оценке остальных пользователей (оценка не ниже 6.5/10)
        cleaned_movies = movies.withColumn('imdb_id', regexp_replace('imdb_id', '[^0-9]', ''))
        cleaned_movies1 = cleaned_movies.select(['id', 'original_title', 'vote_average'])
        cleaned_movies2 = cleaned_movies1.withColumn('vote_average', cleaned_movies1['vote_average'].cast('float'))
        cleaned_movies3 = cleaned_movies2.withColumn('vote_average',
                                                     regexp_replace(col('vote_average'), '[^0-9,.]', ''))
        # Итоговое соединение и фильтрация
        movies_rec = filtered_common_keywords_limited.join(broadcast(cleaned_movies3),
                                                           filtered_common_keywords_limited.recommend_id == cleaned_movies3.id,
                                                           'left')
        subfinal = movies_rec.select(filtered_common_keywords_limited['id'], 'recommend_id', 'original_title').filter(
            col('vote_average').cast('float') >= 6.5)
        final = subfinal.filter(col('vote_average').isNotNull())

        return sorted_common_keywords, user_ratings, final
    except AnalysisException as e:
        print(f'Ошибка при трансформации данных: {str(e)}')
        return None


######################################################################################
# Этот пункт на случай, если человек впервые пользуется сервисом и он не смотрел фильмы
def popular_films(spark,quan=10):  # входное значение, для определения сколько фильмом человек хочет получить в рекомендации
    try:
        datasets = dataset_extract(spark)

        # Проверка наличия данных
        if datasets:
            movies, keywords, links, ratings = datasets
        else:
            # Обработка ошибки, если что-то пошло не так при чтении данных
            print('Произошла ошибка при чтении данных. Пожалуйста, проверьте пути к файлам и их формат.')

        cleaned_movies1 = movies.select(['original_title', 'popularity', 'vote_average'])
        cleaned_movies2 = cleaned_movies1.withColumn('popularity', cleaned_movies1['popularity'].cast(
            'float'))  # преобразование к типу float для корректных манипуляций

        cleaned_movies3 = cleaned_movies2.withColumn('popularity',
                                                     regexp_replace(col('popularity'), '[^0-9,.]', '')).filter(
            col('popularity').isNotNull())  # убираем посторонние символы и null
        cleaned_movies4 = cleaned_movies3.withColumn('vote_average',
                                                     regexp_replace(col('vote_average'), '[^0-9.]', '')).filter(
            col('vote_average').isNotNull())
        cleaned_movies5 = cleaned_movies4.filter(
            (col('popularity') >= 10.0) & (col('vote_average').cast('float') <= 10.0)).sort(
            col('popularity').desc())  # считаем фильм неактуальным при его популярности ниже 10 и
        # убираем ошибки, где средняя оценка больше 10

        cleaned_movies5.show(quan)
        return cleaned_movies5

    except AnalysisException as e:
        print(f'Ошибка при выполнении функции: {str(e)}')
        return None


def recommendation_of_movies(user_id,spark):
    try:
        # Загрузка данных
        datasets = dataset_extract(spark)

        if datasets:
            movies, keywords,   links, ratings = datasets

            # Трансформация данных
            sorted_common_keywords, user_ratings, final = dataset_transform(keywords, ratings, links, spark)

            # Фильтрация результатов для конкретного пользователя
            user_recommendations = final.filter(col('id') == user_id)

            # Если пользователю не подобрало ни 1 фильма среди ключевых слов или он первый раз пользуется сервисом -- ему предлагают самые популярные фильмы
            if user_recommendations.count() > 0:
                # Вывод результатов
                user_recommendations.show()
            else:
                popular_films(10)




    except AnalysisException as e:
        print(f'Ошибка при выполнении функции: {str(e)}')


# Пример вызова функции
#recommendation_of_movies(13)
#recommendation_of_movies(1)