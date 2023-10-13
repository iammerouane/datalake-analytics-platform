import sys
import pandas as pd
from sqlalchemy import create_engine
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import sum as sql_sum, lit, col, when, to_date, year, month, dayofmonth
from pyspark.ml.feature import VectorAssembler, StandardScaler, PCA
from pyspark.ml.regression import LinearRegression
from pyspark.ml.clustering import KMeans
from pyspark.ml.recommendation import ALS

ip = sys.argv[1]

spark = SparkSession.builder.appName("DataLakeAnalyticsPlatform").getOrCreate()

spark.sparkContext.setLogLevel("FATAL")

# Chargement des données à partir des fichiers CSV dans le HDFS
print("Chargement des données à partir des fichiers CSV dans le HDFS ...")
bikes_df: DataFrame = spark.read.csv("hdfs://hadoop-namenode:9000/bikes.csv", header=True, inferSchema=True)
bikeshops_df: DataFrame = spark.read.csv("hdfs://hadoop-namenode:9000/bikeshops.csv", header=True, inferSchema=True)
orders_df: DataFrame = spark.read.csv("hdfs://hadoop-namenode:9000/orders.csv", header=True, inferSchema=True)
customers_df: DataFrame = spark.read.csv("hdfs://hadoop-namenode:9000/customers.csv", header=True, inferSchema=True)
print("Chargement effectué.\n")

# Nettoyage des données
print("Nettoyage des données ...")

# Suppression des doublons
print("Supressions des doublons ...")
bikes_df = bikes_df.dropDuplicates()
bikeshops_df = bikeshops_df.dropDuplicates()
orders_df = orders_df.dropDuplicates()
customers_df = customers_df.dropDuplicates()
print("Supression terminée.")

# Correction des valeurs aberrantes
print("Correction des valeurs aberrantes ...")
# Remplacement des valeurs négatives dans les colonnes numériques par la médiane
numeric_columns_bikes = ["BikePrice"]
for col_name in numeric_columns_bikes:
    median_value = bikes_df.approxQuantile(col_name, [0.5], 0.01)[0]
    bikes_df = bikes_df.withColumn(col_name, when(bikes_df[col_name] < 0, median_value).otherwise(bikes_df[col_name]))
numeric_columns_orders = ["Quantity"]
for col_name in numeric_columns_orders:
    median_value = orders_df.approxQuantile(col_name, [0.5], 0.01)[0]
    orders_df = orders_df.withColumn(col_name, when(orders_df[col_name] < 0, median_value).otherwise(orders_df[col_name]))
numeric_columns_customers = ["AnnualIncome", "TotalChildren"]
for col_name in numeric_columns_customers:
    median_value = customers_df.approxQuantile(col_name, [0.5], 0.01)[0]
    customers_df = customers_df.withColumn(col_name, when(customers_df[col_name] < 0, median_value).otherwise(customers_df[col_name]))
print("Correction terminée.")


# Traitement des valeurs manquantes 
print("Traitement des valeurs manquantes ...")
# Remplacement des valeurs manquantes dans toutes les colonnes par des valeurs par défaut
default_values_bikes = {
    "BikeCategory1": "Road",
    "BikeCategory2": "Sport",
    "BikeFrame": "Aluminium",
    "BikePrice": bikes_df.approxQuantile("BikePrice", [0.5], 0.01)[0],
}

default_values_bikeshops = {
    "BikeshopCity": "Los Angeles",
    "BikeshopState": "CA",
    "Latitude": "34.052234",
    "Longitude": "-118.243685",
}

default_values_orders = {
    "Quantity": 1,
}

default_values_customers = {
    "Prefix":"Mr.",
    "FirstName":"John",
    "LastName":"Doe",
    "MaritalStatus":"Single",
    "Gender":"Male",
    "EmailAddress":"johndoe@email.com",
    "AnnualIncome": customers_df.approxQuantile("AnnualIncome", [0.5], 0.01)[0],
    "TotalChildren": customers_df.approxQuantile("TotalChildren", [0.5], 0.01)[0],
    "EducationLevel": "Bachelor's",
    "Occupation": "Engineer",
    "HomeOwner": "No"
}
for col_name, default_value in default_values_bikes.items():
    bikes_df = bikes_df.fillna(default_value, subset=[col_name])
for col_name, default_value in default_values_bikeshops.items():
    bikeshops_df = bikeshops_df.fillna(default_value, subset=[col_name])
for col_name, default_value in default_values_orders.items():
    orders_df = orders_df.fillna(default_value, subset=[col_name])
for col_name, default_value in default_values_customers.items():
    customers_df = customers_df.fillna(default_value, subset=[col_name])
print("Traitement des valeurs manquantes terminé.")

print("Nettoyage des données terminé.\n")

# Audit de la qualité des données
print("Audit de la qualité des données ...")

# Vérification de l'exactitude des dates
print("Vérification de l'exactitude des dates ...")
date_format = "dd/MM/yyyy"
customers = customers_df.withColumn("BirthDate", to_date(col("BirthDate"), date_format).alias("ValidDate"))
orders = orders_df.withColumn("OrderDate", to_date(col("OrderDate"), date_format).alias("ValidDate"))

# Affichage les lignes avec des dates invalides
try:
    customers.filter(col("ValidDate").isNull())
    orders.filter(col("ValidDate").isNull())
except:
    print("Dates vérifiés.")
else:
    print("Incohérence des dates !!!")

# Vérification de la cohérence des données textuelles
print("Vérification de la cohérence des données textuelles ...")
# Définition des listes de catégories valides
valid_bike_category1 = ["Mountain", "Road"]
valid_bike_category2 = ["Elite Road", "Endurance Road", "Triathalon", "Cyclocross", "Over Mountain", "Cross Country Race","Fat Bike","Trail", "Sport"]
valid_bike_frame = ["Carbon", "Aluminum"]

valid_state = ["AL", "AK", "AS", "AZ", "AR", "CA", "CO", "CT", "DE", "DC", "FM", "FL", "GA", "GU", "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MH", "MD", "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY", "NC", "ND", "MP", "OH", "OK", "OR", "PW", "PA", "PR", "RI", "SC", "SD", "TN", "TX", "UT", "VT", "VI", "VA", "WA", "WV", "WI", "WY"]

valid_marital_status = ["Single", "Married"]
valid_gender = ["Male", "Female"]
valid_education_level = ["High School", "Associate's", "Bachelor's", "Master's", "PhD"]
valid_home_owner = ["Yes", "No"]

# Vérification de la cohérence des catégories
if bikes_df.count() == bikes_df.filter(col("BikeCategory1").isin(valid_bike_category1)).filter(col("BikeCategory2").isin(valid_bike_category2)).filter(col("BikeFrame").isin(valid_bike_frame)).count()  and \
bikeshops_df.count() == bikeshops_df.filter(col("BikeshopState").isin(valid_state)).count() and \
customers_df.count() == customers_df.filter(col("MaritalStatus").isin(valid_marital_status)).filter(col("Gender").isin(valid_gender)).filter(col("EducationLevel").isin(valid_education_level)).filter(col("HomeOwner").isin(valid_home_owner)).count():
    print("Données textuelles vérifiées.")
else:
    print("Incohérence des données textuelles !!!")

# Vérification de la cohérence des données numériques
print("Vérification de la cohérence des données numériques ...")
if bikes_df.count() == bikes_df.filter(col("BikeKey") >= 1).count() and \
bikes_df.count() == bikes_df.filter(col("BikePrice") > 0).count() and \
bikeshops_df.count() == bikeshops_df.filter(col("BikeshopKey") >= 1).count() and \
orders_df.count() == orders_df.filter(col("OrderKey") >= 1).count() and \
orders_df.count() == orders_df.filter(col("OrderId") >= 1).count() and \
orders_df.count() == orders_df.filter(col("OrderLine") >= 1).count() and \
orders_df.count() == orders_df.filter(col("CustomerKey") >= 1).count() and \
orders_df.count() == orders_df.filter(col("BikeKey") >= 1).count() and \
orders_df.count() == orders_df.filter(col("Quantity") >= 1).count() and \
customers_df.count() == customers_df.filter(col("CustomerKey") >= 1).count() and \
customers_df.count() == customers_df.filter(col("TotalChildren") >= 0).count() and \
customers_df.count() == customers_df.filter(col("AnnualIncome") >= 0).count():
    print("Données numériques vérifiées.")
else:
    print("Incohérence des données numériques !!!")

print("Audit de la qualité des données terminé.\n")


# Vérification de l'historique des données
print("Vérification de l'historique des données ...")
date_format = "dd/MM/yyyy"
customers = customers_df.withColumn("BirthDate", to_date(col("BirthDate"), date_format).alias("ValidDate"))
orders = orders_df.withColumn("OrderDate", to_date(col("OrderDate"), date_format).alias("ValidDate"))

# Affichage des lignes avec des dates invalides
try:
    customers.filter(col("ValidDate").isNull())
    orders.filter(col("ValidDate").isNull())
except:
    print("Historique des données vérifié.\n")
else:
    print("Incohérence de l'historique des données !!!")
    

    
# Analyse prédictive des ventes
print("Analyse prédictive des ventes ...")

# Supression des colonnes non nécessaires et cast de la date
orders = orders_df.select("OrderDate", "Quantity").withColumn("OrderDate", to_date(col("OrderDate"), "d/M/y"))

# Agrégation des ventes par date
daily_sales = orders.groupBy("OrderDate").agg(sql_sum("Quantity").alias("TotalSales")).orderBy("OrderDate")

# Supression de la colonne des ventes totales
condition = (daily_sales["TotalSales"] != 12572)
daily_sales = daily_sales.filter(condition)

# Extraction de l'année, du mois et du jour de la date
daily_sales = daily_sales.withColumn("Year", year("OrderDate")).withColumn("Month", month("OrderDate")).withColumn("Day", dayofmonth("OrderDate"))

# Combinaison des caractéristiques en une seule colonne "features"
assembler = VectorAssembler(inputCols=["Year", "Month", "Day"], outputCol="features")
assembled_data = assembler.transform(daily_sales)

# Création d'un modèle de régression linéaire
print("Création d'un modèle de régression linéaire ...")
lr = LinearRegression(featuresCol="features", labelCol="TotalSales")

# Entraînement du modèle
print("Entraînement du modèle ...")
lr_model = lr.fit(assembled_data)

print("Analyse prédictive des ventes terminée.\n")





# Segmentation de la clientèle
print("Segmentation de la clientèle ...")

# Regroupement des données d'achat par client
customer_purchase_data = orders_df.groupBy("CustomerKey").sum("Quantity").withColumnRenamed("sum(Quantity)", "TotalPurchase")

# Fusion des données de comportement d'achat avec les données clients
combined_df = customers_df.join(customer_purchase_data, on="CustomerKey", how="inner")

# Sélection des caractéristiques pertinentes pour la PCA
feature_columns = ["TotalPurchase"]

# Création d'un vecteur d'assemblage des caractéristiques
assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")
assembled_data = assembler.transform(combined_df)

# Mise à l'échelle des caractéristiques
scaler = StandardScaler(inputCol="features", outputCol="scaledFeatures", withStd=True, withMean=True)
scaler_model = scaler.fit(assembled_data)
scaled_data = scaler_model.transform(assembled_data)

# Réduction de dimension avec PCA
pca = PCA(k=1, inputCol="scaledFeatures", outputCol="pcaFeatures")
pca_model = pca.fit(scaled_data)
pca_result = pca_model.transform(scaled_data)

# Création du modèle K-Means
print("Création d'un modèle de régression linéaire ...")
kmeans = KMeans(k=4, seed=1)

# Entraînement du modèle
print("Entraînement du modèle pour la segmentation ...")
kmeans_model = kmeans.fit(pca_result)

print("Segmentation de la clientèle terminée.\n")







# Recommandation de produits
print("Recommandation de produits ...")

# Prétraiter les données pour les adapter à l'approche de recommandation
data = orders_df.join(customers_df, 'CustomerKey', 'inner').join(bikes_df, 'BikeKey', 'inner')

# Création d'un modèle ALS (Alternating Least Squares)
print("Création d'un modèle ALS (Alternating Least Squares) ...")
als = ALS(maxIter=5, regParam=0.01, userCol="CustomerKey", itemCol="BikeKey", ratingCol="Quantity")

# Entraînement du modèle
print("Entraînement du modèle pour la recommendation ...")
als_model = als.fit(data)

print("Recommandation de produits terminée.\n")






# Optimisation de la chaîne d’approvisionnement
print("Optimisation de la chaîne d’approvisionnement ...")

# Convertir la colonne OrderDate au format date
orders = orders_df.withColumn("OrderDate", to_date(orders_df["OrderDate"], "d/M/y"))

# Sélectionner les colonnes pertinentes pour la prédiction
data = orders.join(bikes_df, "BikeKey").select("OrderDate", "BikeFrame", "Quantity")

# Regroupement des données par mois
data = data.withColumn("Year", year("OrderDate"))
data = data.withColumn("Month", month("OrderDate"))

# Agrégation des quantités vendues par mois pour chaque type de BikeFrame
agg_data = data.groupBy("Year", "Month", "BikeFrame").agg(sql_sum("Quantity").alias("TotalQuantity"))

# Supression de la colonne des ventes totales Carbon
condition = (agg_data["TotalQuantity"] != 5858)
agg_data = agg_data.filter(condition)

# Supression de la colonne des ventes totales Aluminium
condition = (agg_data["TotalQuantity"] != 6714)
agg_data = agg_data.filter(condition)

# Création d'un vecteur d'entités pour le ML
assembler = VectorAssembler(inputCols=["Year", "Month"], outputCol="features")
assembled_data = assembler.transform(agg_data)

# Création du modèle de régression linéaire
print("Création d'un modèle de régression linéaire ...")
lr2 = LinearRegression(featuresCol="features", labelCol="TotalQuantity")

# Entraînement du modèle
print("Entraînement du modèle ...")
lr_model_2 = lr2.fit(assembled_data)

print("Optimisation de la chaîne d’approvisionnement terminée.\n")





# Chargement des modèles dans HDFS
print("Chargement des modèles de Machine Learning dans HDFS ...")
lr_model.save("hdfs://hadoop-namenode:9000/mllib_models/sales_prediction_model")
kmeans_model.save("hdfs://hadoop-namenode:9000/mllib_models/customers_segmentation_model")
als_model.save("hdfs://hadoop-namenode:9000/mllib_models/recommendation_system_model")
lr_model_2.save("hdfs://hadoop-namenode:9000/mllib_models/supplychain_optimization_model")
print("Chargement des modèles de Machine Learning dans HDFS terminé.\n")


# Chargement des données dans la base de données MySQL
print("Chargement des données dans la base de données MySQL ...")

db_user = "nebula_user"
db_password = "nebula_pw"
db_host = ip
db_port = 3306
db_name = "nebula_db"

db_url = f"mysql+mysqlconnector://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"

orders_df = orders_df.toPandas()
bikes_df = bikes_df.toPandas()
bikeshops_df = bikeshops_df.toPandas()
customers_df = customers_df.toPandas()


engine = create_engine(db_url)

bikes_df.to_sql("bikes", con=engine, if_exists="replace", index=False)
bikeshops_df.to_sql("bikeshops", con=engine, if_exists="replace", index=False)
orders_df.to_sql("orders", con=engine, if_exists="replace", index=False)
customers_df.to_sql("customers", con=engine, if_exists="replace", index=False)

engine.dispose()

print("Chargement des données dans la base de données MySQL effectué.\n")


spark.stop()
