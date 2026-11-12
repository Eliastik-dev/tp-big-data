# Job 2 : feature engineering pour le modele + chargement dans PostgreSQL
# Entree : /nba/clean/play_by_play (job 1)
# Sortie : /nba/gold/features (parquet, pour entrainer le LSTM)
#          + tables postgres : equipes, matchs, actions (pour le dashboard)
#
# lancement :
# docker compose exec spark-master /spark/bin/spark-submit --master spark://spark-master:7077 \
#     --packages org.postgresql:postgresql:42.7.3 /jobs/02_features.py

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

HDFS = "hdfs://namenode:9000/nba"
PG_URL = "jdbc:postgresql://postgres:5432/nba"
PG_PROPS = {"user": "nba", "password": "nba", "driver": "org.postgresql.Driver"}

# un "run" = une equipe met au moins 8 points de plus que l'autre sur les 3 dernieres minutes
FENETRE_RUN = 180
SEUIL_RUN = 8

spark = SparkSession.builder.appName("nba-02-features").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

df = spark.read.parquet(HDFS + "/clean/play_by_play")

# ordre des actions dans un match
w_match = Window.partitionBy("game_id").orderBy("periode", "num_action")

# --- temps ecoule depuis le debut du match (en secondes) ---
# plus pratique que le temps restant car il augmente toujours, meme en prolongation
df = df.withColumn(
    "temps_ecoule",
    F.when(F.col("periode") <= 4, 2880 - F.col("temps_restant"))
     .otherwise(2880 + (F.col("periode") - 5) * 300 + (300 - F.col("temps_restant")))
)

# --- points marques sur l'action ---
df = df.withColumn("pts_dom", F.col("score_dom") - F.coalesce(F.lag("score_dom").over(w_match), F.lit(0))) \
       .withColumn("pts_ext", F.col("score_ext") - F.coalesce(F.lag("score_ext").over(w_match), F.lit(0)))

# --- run sur les 3 dernieres minutes ---
# run = ecart maintenant - ecart il y a 3 minutes
# attention : au debut j'avais fait une somme sur une fenetre rangeBetween(-180, 0), mais elle prend
# aussi les actions qui arrivent dans la meme seconde APRES l'action (ex : lancers francs apres une faute)
# -> le modele voyait un peu le futur. Ici la fenetre s'arrete 3 min avant, donc pas de fuite.
w_avant_3min = Window.partitionBy("game_id").orderBy("temps_ecoule") \
    .rangeBetween(Window.unboundedPreceding, -FENETRE_RUN - 1)
df = df.withColumn("ecart_3min_avant", F.coalesce(F.last("ecart", ignorenulls=True).over(w_avant_3min), F.lit(0))) \
       .withColumn("run_3min", F.col("ecart") - F.col("ecart_3min_avant")) \
       .withColumn("en_run", F.when(F.col("run_3min") >= SEUIL_RUN, 1)
                              .when(F.col("run_3min") <= -SEUIL_RUN, -1)
                              .otherwise(0))

# --- serie de points sans reponse ---
# ex : domicile marque 2, puis 3, puis 2 sans que l'exterieur marque -> serie = +7
# on travaille seulement sur les actions qui marquent des points
paniers = df.filter((F.col("pts_dom") > 0) | (F.col("pts_ext") > 0)) \
    .withColumn("equipe_marque", F.when(F.col("pts_dom") > 0, 1).otherwise(-1)) \
    .withColumn("pts", F.col("pts_dom") + F.col("pts_ext"))
# nouveau groupe a chaque fois que l'equipe qui marque change
paniers = paniers.withColumn(
    "changement",
    F.when(F.lag("equipe_marque").over(w_match) == F.col("equipe_marque"), 0).otherwise(1)
).withColumn("groupe", F.sum("changement").over(w_match))
w_groupe = Window.partitionBy("game_id", "groupe").orderBy("periode", "num_action")
paniers = paniers.withColumn("serie_points", F.sum("pts").over(w_groupe) * F.col("equipe_marque")) \
    .select("game_id", "num_action", "serie_points")

df = df.join(paniers, ["game_id", "num_action"], "left")
# on propage la serie sur les actions sans panier
w_avant = w_match.rowsBetween(Window.unboundedPreceding, 0)
df = df.withColumn("serie_points", F.coalesce(F.last("serie_points", ignorenulls=True).over(w_avant), F.lit(0)))

# --- serie de tirs reussis d'affilee (par equipe) ---
# eventmsgtype 1 = tir reussi, 2 = tir rate
tirs = df.filter(F.col("type_action").isin(1, 2) & (F.col("equipe_action") != 0))
w_equipe = Window.partitionBy("game_id", "equipe_action").orderBy("periode", "num_action")
tirs = tirs.withColumn("rate", (F.col("type_action") == 2).cast("int")) \
    .withColumn("groupe_tir", F.sum("rate").over(w_equipe))
w_tir = Window.partitionBy("game_id", "equipe_action", "groupe_tir").orderBy("periode", "num_action")
tirs = tirs.withColumn("serie_tirs", F.sum(1 - F.col("rate")).over(w_tir)) \
    .select("game_id", "num_action",
            F.when(F.col("equipe_action") == 1, F.col("serie_tirs")).alias("s_dom"),
            F.when(F.col("equipe_action") == -1, F.col("serie_tirs")).alias("s_ext"))

df = df.join(tirs, ["game_id", "num_action"], "left")
df = df.withColumn("serie_tirs_dom", F.coalesce(F.last("s_dom", ignorenulls=True).over(w_avant), F.lit(0))) \
       .withColumn("serie_tirs_ext", F.coalesce(F.last("s_ext", ignorenulls=True).over(w_avant), F.lit(0))) \
       .drop("s_dom", "s_ext")

# --- possession approximative : l'equipe qui a fait la derniere action "active" ---
df = df.withColumn("possession", F.last(F.when(F.col("equipe_action") != 0, F.col("equipe_action")),
                                        ignorenulls=True).over(w_avant)) \
       .fillna({"possession": 0})

# --- rang de l'action dans le match (pour reconstruire les sequences du LSTM) ---
df = df.withColumn("rang", F.row_number().over(w_match))

features = df.select(
    "game_id", "saison", "rang", "num_action", "periode", "temps_restant", "temps_ecoule",
    "type_action", "equipe_action", "possession",
    "score_dom", "score_ext", "ecart", "pts_dom", "pts_ext",
    "run_3min", "en_run", "serie_points", "serie_tirs_dom", "serie_tirs_ext",
    "victoire_dom",
)

features.cache()
print("nb actions :", features.count())
print("repartition en_run :")
features.groupBy("en_run").count().orderBy("en_run").show()

# repartition par saison avant d'ecrire : sinon chaque tache ecrit un petit fichier par saison
# (des milliers de petits fichiers, ce que HDFS gere mal)
features.repartition("saison").write.mode("overwrite").partitionBy("saison").parquet(HDFS + "/gold/features")
print("features ecrites dans", HDFS + "/gold/features")

# --- chargement PostgreSQL ---
equipes = spark.read.option("header", True).csv(HDFS + "/raw/team") \
    .select(F.col("id").cast("long").alias("equipe_id"), F.col("full_name").alias("nom"),
            F.col("abbreviation").alias("abreviation"), F.col("city").alias("ville"))
matchs = spark.read.parquet(HDFS + "/clean/matchs")

equipes.write.jdbc(PG_URL, "equipes", mode="overwrite", properties=PG_PROPS)
matchs.write.jdbc(PG_URL, "matchs", mode="overwrite", properties=PG_PROPS)
features.repartition(8).write.option("batchsize", 10000) \
    .jdbc(PG_URL, "actions", mode="overwrite", properties=PG_PROPS)
print("tables equipes, matchs, actions chargees dans postgres")

spark.stop()
