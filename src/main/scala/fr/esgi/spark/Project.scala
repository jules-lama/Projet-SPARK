package fr.esgi.spark

import org.apache.spark.sql.SparkSession

object Project {
  def main(args: Array[String]) = {

    //Instancier le spark session
    val spark = SparkSession.builder()
      .appName("project-Spark")
      .master("local[2]")
      .getOrCreate();

    //Création du dataframe pour le fichier client
    val dfClients = spark.read.format("csv")
      .option("header", "true")
      .option("delimiter", ";")
      .option("inferSchema", "true")
      .csv("src/main/resources/clients.csv")

    println("Taille fichier en ligne: " + dfClients.count())
    dfClients.show(20)

    //Création du dataframe pour le fichier items (complet)
    val dfProducts = spark.read.format("csv")
      .option("header", "true")
      .option("delimiter", ";")
      .option("inferSchema", "true")
      .csv("src/main/resources/items.csv")

    println("Taille fichier en ligne: " + dfProducts.count())
    dfProducts.show(20)

    //Création du dataframe pour le fichier transaction
    val dfTransactions = spark.read.format("csv")
      .option("header", "true")
      .option("delimiter", ";")
      .option("inferSchema", "true")
      .csv("src/main/resources/transaction.csv")

    println("Taille fichier en ligne: " + dfTransactions.count())
    dfTransactions.show(20)

    import spark.implicits._
    import org.apache.spark.sql.functions._

    //Table de données clients - transactions
    var joinedDfQ1 = dfTransactions.join(dfClients, dfTransactions("Client_id") === dfClients("id"))
    joinedDfQ1 = joinedDfQ1.join(dfProducts, joinedDfQ1("item_id") === dfProducts("id"), "left")
      .withColumn("Year", year(to_timestamp($"Date_Of_Transaction", "yy/dd/MM")))
      .withColumn("Month", month(to_timestamp($"Date_Of_Transaction", "yy/dd/MM")))

    //Table de données transcaction - produit
    var joinedDfQ2 = dfProducts.join(dfTransactions, dfProducts("id") === dfTransactions("Item_Id"))
      .withColumn("Year", year(to_timestamp($"Date_Of_Transaction", "yy/dd/MM")))
      .withColumn("Month", month(to_timestamp($"Date_Of_Transaction", "yy/dd/MM")))


    //Clients les plus anciens
    dfClients
      .select($"Name", $"First_Name", $"Date_Of_Creation")
      .sort($"Date_Of_Creation")
      .show(5)

    //Nombre de vente par client
    var nbVenteClient = joinedDfQ1
      .groupBy("Name")
      .count()

    nbVenteClient.coalesce(1).write.option("header", "true").csv("nbVenteClient.csv")

    //Chiffre d'affaire par client
    var caParClient = joinedDfQ1
      .groupBy("Name")
      .agg(sum($"Item_Buyed_Price" * $"Number") as "CA")
      .sort($"CA".desc)

    caParClient.coalesce(1).write.option("header", "true").csv("caParClient.csv")

    //Produits les plus vendus en quantité
    var prodPlusVendu = joinedDfQ2
      .groupBy($"Item_Name")
      .count()
      .sort($"count".desc)

    prodPlusVendu.coalesce(1).write.option("header", "true").csv("prodPlusVendu.csv")

    //Produits les plus vendus en chiffres d'affaires
    var caParProd = joinedDfQ2
      .groupBy($"Item_Name")
      .agg(sum($"Item_Selling_Price" * $"Number") as "CA du produit")
      .sort($"CA du produit".desc)

    caParProd.coalesce(1).write.option("header", "true").csv("caParProd.csv")

    //Quantité des produits les plus vendus à Paris
    joinedDfQ2.select($"Item_Name").filter($"Shop_Name" === "Paris").groupBy($"Item_Name").count().sort($"count".desc).show()

    //Chiffre d'affaires par villes
    var chiffreAffairesParVille = joinedDfQ2
      .groupBy($"Shop_Name")
      .agg(sum($"Item_Selling_Price" * $"Number") as "CA")
      .orderBy($"Shop_Name")
      .withColumn("CA",  format_number($"CA", 2))

    chiffreAffairesParVille.coalesce(1).write.option("header", "true").csv("chiffreAffairesParVille.csv")

    //Chiffre d'affaires par mois
    var chiffreAffairesParMois = joinedDfQ2
      .groupBy($"Month")
      .agg(sum($"Item_Selling_Price" * $"Number") as "CA")
      .orderBy($"Month")
      .withColumn("CA", format_number($"CA", 2))

    chiffreAffairesParMois.coalesce(1).write.option("header", "true").csv("chiffreAffairesParMois.csv")

    //Benefice par mois
    var beneficeParMois = joinedDfQ2
      .groupBy($"Month")
      .agg(sum($"Item_Selling_Price" * $"Number" - $"Item_Buyed_Price" * $"Number") as "Beneficts")
      .orderBy($"Month")
      .withColumn("Beneficts", format_number($"Beneficts", 2))

    beneficeParMois.coalesce(1).write.option("header", "true").csv("beneficeParMois.csv")

    //Benefice par villes
    var beneficeParVille = joinedDfQ2
      .groupBy($"Shop_Name")
      .agg(sum($"Item_Selling_Price" * $"Number" - $"Item_Buyed_Price" * $"Number") as "Beneficts")
      .orderBy($"Shop_Name")
      .withColumn("Beneficts", format_number($"Beneficts", 2))

    beneficeParVille.coalesce(1).write.option("header", "true").csv("beneficeParVille.csv")

    //Chiffre d'affaires par catégorie
    var CAParCategorie = joinedDfQ2
      .groupBy($"Category")
      .agg(sum($"Item_Selling_Price" * $"Number") as "CA")
      .orderBy($"Category")

    CAParCategorie.coalesce(1).write.option("header", "true").csv("CAParCategorie.csv")

    //Chiffres d'affaires par année
    var CAParAnnee = joinedDfQ2
      .groupBy($"Year")
      .agg(sum($"Item_Selling_Price" * $"Number") as "CA")
      .orderBy($"Year")

    CAParAnnee.coalesce(1).write.option("header", "true").csv("CAParAnnee.csv")

    //Nombre de produits par catégorie
    var nbParCategorie = joinedDfQ2
      .groupBy($"Category")
      .count()
      .orderBy($"Category")

    nbParCategorie.coalesce(1).write.option("header", "true").csv("nbParCategorie.csv")

    //Moyenne d'achats par client
    var moyAcheteParClient = joinedDfQ1
      .groupBy($"Name")
      .agg(mean($"Item_Selling_Price" * $"Number"))

    moyAcheteParClient.coalesce(1).write.option("header", "true").csv("moyAcheteParClient.csv")
  }
}
