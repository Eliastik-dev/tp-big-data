# Job 1 : nettoyage du play-by-play
# Lit les CSV bruts dans HDFS (/nba/raw), nettoie et ecrit en parquet dans /nba/clean
#
# lancement :
# docker exec spark-master /spark/bin/spark-submit --master spark://spark-master:7077 /jobs/01_nettoyage.py

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

HDFS = "hdfs://namenode:9000/nba"

spark = SparkSession.builder.appName("nba-01-nettoyage").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# --- lecture ---
# on lit tout en string (pas d'inferSchema) : plus rapide et ca evite
# que spark perde les 0 au debut des game_id
pbp = spark.read.option("header", True).option("multiLine", True).option("escape", '"') \
    .csv(HDFS + "/raw/play_by_play")
matchs = spark.read.option("header", True).csv(HDFS + "/raw/game")

print("lignes play_by_play brutes :", pbp.count())

# --- infos des matchs ---
# on garde seulement saison reguliere + playoffs, et le resultat du match (label a predire)
matchs = matchs.filter(F.col("season_type").isin("Regular Season", "Playoffs")) \
    .select(
        F.col("game_id").cast("long").alias("game_id"),
        F.to_date("game_date").alias("date_match"),
        F.substring("season_id", 2, 4).cast("int").alias("saison"),
        F.col("season_type").alias("type_saison"),
        F.col("team_abbreviation_home").alias("equipe_dom"),
        F.col("team_abbreviation_away").alias("equipe_ext"),
        F.when(F.col("wl_home") == "W", 1).otherwise(0).alias("victoire_dom"),
    ) \
    .filter(F.col("game_id").isNotNull()) \
    .dropDuplicates(["game_id"])

# --- colonnes utiles du play-by-play ---
pbp = pbp.select(
    F.col("game_id").cast("long").alias("game_id"),
    F.col("eventnum").cast("int").alias("num_action"),
    F.col("eventmsgtype").cast("int").alias("type_action"),
    F.col("eventmsgactiontype").cast("int").alias("sous_type"),
    F.col("period").cast("int").alias("periode"),
    F.col("pctimestring").alias("chrono"),
    F.col("homedescription").alias("desc_dom"),
    F.col("visitordescription").alias("desc_ext"),
    F.col("score"),
    F.col("scoremargin"),
    F.col("player1_id").cast("long").alias("joueur_id"),
)

pbp = pbp.filter(F.col("game_id").isNotNull() & F.col("num_action").isNotNull())
pbp = pbp.dropDuplicates(["game_id", "num_action"])

# --- temps restant dans le match (en secondes) ---
# chrono = "MM:SS" restant dans la periode. 4 periodes de 12 min, prolongations de 5 min
min_sec = F.split("chrono", ":")
sec_periode = min_sec.getItem(0).cast("int") * 60 + min_sec.getItem(1).cast("int")
pbp = pbp.withColumn(
    "temps_restant",
    F.when(F.col("periode") <= 4, (4 - F.col("periode")) * 720 + sec_periode)
     .otherwise(sec_periode)   # en prolongation on garde juste le temps de la prolongation
)

# --- quelle equipe fait l'action ---
# 1 = domicile, -1 = exterieur, 0 = neutre (debut de periode, etc)
pbp = pbp.withColumn(
    "equipe_action",
    F.when(F.col("desc_dom").isNotNull() & F.col("desc_ext").isNull(), 1)
     .when(F.col("desc_ext").isNotNull() & F.col("desc_dom").isNull(), -1)
     .otherwise(0)
)

# --- score ---
# le score n'est rempli que quand il y a un panier, il faut le propager aux lignes suivantes
# format "AAA - BBB" : on suppose exterieur - domicile (verifie plus bas avec scoremargin)
parties = F.split(F.col("score"), " - ")
pbp = pbp.withColumn("score_ext", parties.getItem(0).cast("int")) \
         .withColumn("score_dom", parties.getItem(1).cast("int"))

w = Window.partitionBy("game_id").orderBy("periode", "num_action") \
          .rowsBetween(Window.unboundedPreceding, 0)
pbp = pbp.withColumn("score_dom", F.coalesce(F.last("score_dom", ignorenulls=True).over(w), F.lit(0))) \
         .withColumn("score_ext", F.coalesce(F.last("score_ext", ignorenulls=True).over(w), F.lit(0))) \
         .withColumn("ecart", F.col("score_dom") - F.col("score_ext"))

# verif : scoremargin (quand il est rempli) doit etre egal a dom - ext
verif = pbp.filter(F.col("scoremargin").isNotNull()) \
    .withColumn("marge", F.when(F.col("scoremargin") == "TIE", 0).otherwise(F.col("scoremargin").cast("int"))) \
    .agg(F.avg((F.col("marge") == F.col("ecart")).cast("int")).alias("ok")).collect()[0]["ok"]
print("verif sens du score (doit etre proche de 1) :", verif)

# --- jointure avec les matchs pour avoir la saison et le resultat ---
propre = pbp.join(matchs, "game_id", "inner") \
    .drop("score", "scoremargin", "chrono")

print("lignes apres nettoyage :", propre.count())
print("nombre de matchs :", propre.select("game_id").distinct().count())

# ecriture en parquet, partitionne par saison (pratique pour les requetes par saison)
propre.write.mode("overwrite").partitionBy("saison").parquet(HDFS + "/clean/play_by_play")
matchs.write.mode("overwrite").parquet(HDFS + "/clean/matchs")

print("ok, donnees ecrites dans", HDFS + "/clean")
spark.stop()
