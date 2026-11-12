# Job 3 : agregations pour les graphes d'analyse du dashboard
#  - stats_saisons    : evolution du jeu par saison (points, tirs a 3 pts, runs)
#  - stats_remontees  : % de victoire a domicile selon l'ecart a la mi-temps
#  - stats_avant_run  : quelles actions precedent un run (le "momentum")
#
# lancement :
# docker compose exec spark-master /spark/bin/spark-submit --master spark://spark-master:7077 \
#     --packages org.postgresql:postgresql:42.7.3 /jobs/03_analyses.py

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

HDFS = "hdfs://namenode:9000/nba"
PG_URL = "jdbc:postgresql://postgres:5432/nba"
PG_PROPS = {"user": "nba", "password": "nba", "driver": "org.postgresql.Driver"}

spark = SparkSession.builder.appName("nba-03-analyses").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

feat = spark.read.parquet(HDFS + "/gold/features")
clean = spark.read.parquet(HDFS + "/clean/play_by_play")
w_match = Window.partitionBy("game_id").orderBy("rang")

# ---------- debut des runs ----------
# un run commence quand en_run passe de 0 a +1 / -1
debuts_runs = feat.withColumn("avant", F.lag("en_run").over(w_match)) \
    .filter((F.col("en_run") != 0) & ((F.col("avant") == 0) | F.col("avant").isNull())) \
    .select("game_id", "saison", F.col("temps_ecoule").alias("t_run"), F.col("en_run").alias("sens"))

# ---------- 1) stats par saison ----------
fin_match = feat.withColumn("dernier", F.row_number().over(Window.partitionBy("game_id").orderBy(F.desc("rang")))) \
    .filter("dernier = 1")
points = fin_match.groupBy("saison").agg(
    F.count("*").alias("nb_matchs"),
    F.round(F.avg(F.col("score_dom") + F.col("score_ext")), 1).alias("pts_par_match"),
    F.round(F.avg("victoire_dom") * 100, 1).alias("pct_victoire_dom"),
)
runs = debuts_runs.groupBy("saison").agg(F.count("*").alias("nb_runs"))

# tirs a 3 points : on regarde si la description contient "3PT"
tirs = clean.filter(F.col("type_action").isin(1, 2)) \
    .withColumn("desc", F.coalesce("desc_dom", "desc_ext")) \
    .groupBy("saison").agg(
        F.round(F.avg(F.col("desc").contains("3PT").cast("int")) * 100, 1).alias("pct_tirs_3pts"))

stats_saisons = points.join(runs, "saison", "left").join(tirs, "saison", "left") \
    .withColumn("runs_par_match", F.round(F.col("nb_runs") / F.col("nb_matchs"), 2)) \
    .drop("nb_runs").orderBy("saison")
stats_saisons.show(30)

# ---------- 2) victoire selon l'ecart a la mi-temps ----------
mi_temps = feat.filter("periode = 2") \
    .withColumn("dernier", F.row_number().over(Window.partitionBy("game_id").orderBy(F.desc("rang")))) \
    .filter("dernier = 1") \
    .withColumn("ecart_mi_temps", F.greatest(F.lit(-20), F.least(F.lit(20), F.col("ecart"))))
stats_remontees = mi_temps.groupBy("ecart_mi_temps").agg(
    F.count("*").alias("nb_matchs"),
    F.round(F.avg("victoire_dom") * 100, 1).alias("pct_victoire_dom"),
).filter("nb_matchs >= 30").orderBy("ecart_mi_temps")
stats_remontees.show(50)

# ---------- 3) actions qui precedent un run ----------
# actions de l'equipe qui va faire le run, dans la minute avant le debut du run
TYPES_ETUDIES = [1, 2, 3, 4, 5, 6]
avant = feat.alias("f").join(debuts_runs.alias("r"), "game_id") \
    .filter((F.col("f.temps_ecoule") > F.col("r.t_run") - 60) &
            (F.col("f.temps_ecoule") <= F.col("r.t_run")) &
            (F.col("f.equipe_action") == F.col("r.sens"))) \
    .filter(F.col("type_action").isin(TYPES_ETUDIES))
total_avant = avant.count()
dist_avant = avant.groupBy("type_action").agg(F.round(F.count("*") / total_avant * 100, 1).alias("pct_avant_run"))

normal = feat.filter((F.col("equipe_action") != 0) & F.col("type_action").isin(TYPES_ETUDIES))
total_normal = normal.count()
dist_normal = normal.groupBy("type_action").agg(F.round(F.count("*") / total_normal * 100, 1).alias("pct_normal"))

stats_avant_run = dist_avant.join(dist_normal, "type_action") \
    .withColumn("ratio", F.round(F.col("pct_avant_run") / F.col("pct_normal"), 2)) \
    .orderBy("type_action")
stats_avant_run.show()

# ---------- ecriture postgres + hdfs ----------
for nom, d in [("stats_saisons", stats_saisons), ("stats_remontees", stats_remontees),
               ("stats_avant_run", stats_avant_run)]:
    d.write.mode("overwrite").parquet(HDFS + "/gold/" + nom)
    d.write.jdbc(PG_URL, nom, mode="overwrite", properties=PG_PROPS)
    print(nom, "ecrit")

spark.stop()
