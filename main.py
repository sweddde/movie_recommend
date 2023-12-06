from src.scripts import recommendation_of_movies
from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder.master('local[*]').appName("MoviesApp").getOrCreate()

    user_id = (input("Введите идентификатор пользователя: "))
    recommendation_of_movies(user_id, spark)
