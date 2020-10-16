package fr.esgi.spark


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.round
import org.apache.spark.sql.types.IntegerType
object ExplorationDeDonnees {

  def main(args: Array[String]): Unit = {

    //Instancier le spark session
    val spark = SparkSession.builder()
      .appName("Spark SQL TD1")
      .config("spark.driver.memory","512m")
      .master("local[8]")
      .getOrCreate()
    import spark.implicits._


    // recupération des données
    val data_items = spark.read
      .option("inferSchema", "true")
      .option("header", "true")
      .option("delimiter",";")
      .format("csv")
      .load("../../../../resources/data/items.csv")

    val data_clients = spark.read
      .option("inferSchema", "true")
      .option("header", "true")
      .option("delimiter",";")
      .format("csv")
      .load("../../../../resources/data/clients.csv")

    val data_transaction = spark.read
      .option("inferSchema", "true")
      .option("header", "true")
      .option("delimiter",";")
      .format("csv")
      .load("../../../../resources/data/transaction.csv")

/*
    data_items.printSchema()
    data_items.show()
    data_clients.printSchema()
    data_clients.show()
    data_transaction.printSchema()
    data_transaction.show()
*/
    //fusion de données
    val data_fusionItemTransaction_tmp = data_items.join(data_transaction,data_items("id")=== data_transaction("Item_Id"),"inner")

    val data_fusionItemTransaction  = data_fusionItemTransaction_tmp
        .select(col("Id") as "id item",col("Item_Name"),col("Category") as "Category_item",col("Category_Id"),col("Item_Buyed_Price"),
    col("Item_Selling_Price"),col("Transaction_Id"),col("Client_id"),col("Item_Id"),col("Number"),year(col("Date_Of_Transaction")) as "annee",
          month(col("Date_Of_Transaction")) as "mois",dayofmonth(col("Date_Of_Transaction")) as "jour mois",dayofweek(col("Date_Of_Transaction")) as "jour semaine", col("Date_Of_Transaction"),
          col("Shop_Name"),col("Shop_Id"))

    //  data_fusionItemTransaction.show()
    val allFusion = data_clients.join(data_fusionItemTransaction,data_clients("id")=== data_fusionItemTransaction("Client_Id"),"inner")

    /* *************************
       Exploration des donneés
    ************************** */

   // 1.1)Nombre Article vendu par mois
    data_fusionItemTransaction
      .groupBy(col("annee"),col("mois"))
      .agg(sum(col("Number")))
      .orderBy(col("annee"),col("mois"))
      .show(Int.MaxValue)


    // 2) Moyenne article vendu par transaction par mois en 2019
    (data_fusionItemTransaction
      .groupBy(col("annee"), col("mois"))
      .agg(mean(col("Number")))
      .orderBy(col("mois"))
      .where (col("annee") === 2019))
      .show(Int.MaxValue)


    // 3) Prix moyen d’une transaction en 2019
    val tmp3 = allFusion
                    .where($"annee" ===2019)
                    .agg(mean(col("Item_Selling_Price")*col("Number")) as "Vente")
    tmp3.withColumn("Vente",round(tmp("Vente"),2)).show(Int.MaxValue)


    // 4) Prix moyen d’une transaction en 2019 par magasin
    allFusion
      .where($"annee" ===2019)
      .groupBy($"Shop_id",$"Shop_Name" )
      .agg(mean(col("Item_Selling_Price")*col("Number")) as "Vente")
      .orderBy($"Shop_Id")
      .show()

    // 5) Nombre de transaction différente en 2019 par magasin
    allFusion
      .where($"annee" ===2019)
      .groupBy($"Shop_id",$"Shop_Name" )
      .agg(countDistinct($"Transaction_Id"))
      .orderBy($"Shop_Id")
      .show(Int.MaxValue)



    // 6) Nombre moyen de transaction différente par jours en 2019
    val tmp6 = allFusion
                    .where($"annee"===2019)
                    .groupBy($"Shop_id",$"Shop_Name" , $"mois",$"jour mois")
                    .agg(countDistinct($"Transaction_Id"))
                    .orderBy($"Shop_Id")
    tmp6.agg(mean($"count(DISTINCT Transaction_Id)")as ).show()


    // 7) Nombre moyen de transaction différente par jours en 2019 par magasin
    val tmp7 = allFusion
                   .where($"annee" ===2019)
                   .groupBy($"Shop_id",$"Shop_Name" , $"mois",$"jour mois")
                   .agg(countDistinct($"Transaction_Id"))
                   .orderBy($"Shop_Id")
    tmp7.groupBy($"Shop_id",$"Shop_Name").agg(mean($"count(DISTINCT Transaction_Id)")).show()



    // 8) La transaction la plus chère. ( Plusieurs transactions sont égales, donc on en affiche 10 )
    allFusion.createOrReplaceTempView("data8")
    val sql_variable8 = spark.sql(""" Select * from data8
     | where (Item_Selling_Price*Number)=(SELECT max( Item_Selling_Price*Number) FROM data8)
     """.stripMargin)
    sql_variable8.show(10,false)



    // 9) Nombres d’articles achetés par catégories de clients en 2019
    allFusion.createOrReplaceTempView("data9")
    val sql_variable9 = spark.sql(""" Select category ,sum(Number) from data9 where annee = 2019
    group by category """.stripMargin)
    sql_variable9.show(Int.MaxValue)


    // 10) Nombres d’articles achetés par catégories items par catégories de clients.
      allFusion.createOrReplaceTempView("data10")
    val sql_variable = spark.sql(""" Select category ,sum(Number),Category_item from data10
    where annee = 2019 group by Category,Category_item order by category, sum(Number) desc""".stripMargin)
    sql_variable.show(Int.MaxValue)


    // 11.1) Capital de vente en 2019 par mois.
    val tmp111 = (data_fusionItemTransaction
                     .groupBy(col("annee"), col("mois"))
                     .agg(sum(col("Item_Selling_Price")*col("Number")) as "somme")
                     .orderBy(col("mois"))
                     .where (col("annee") === 2019))
                  // .show(Int.MaxValue)
    tmp111.withColumn("somme", format_number($"somme", 2)).show()


    // 11.2) Bénéfice de vente en 2019 par mois
      val tmp112 = (data_fusionItemTransaction
                       .groupBy(col("annee"), col("mois"))
                       .agg(sum(col("Item_Selling_Price")*col("Number") - col("Item_Buyed_Price")*col("Number")) as "benefice")
                       .orderBy(col("mois"))
                       .where (col("annee") === 2019))
                    // .show(Int.MaxValue)
    tmp112.withColumn("benefice", format_number($"benefice", 2)).show()


    // 12.1) Capitale de vente en 2019 par boutique
    val tmp121 = (data_fusionItemTransaction
                       .where (col("annee") === 2019))
                       .groupBy(col("Shop_Id"), col("Shop_Name"))
                       .agg(sum(col("Item_Selling_Price")*col("Number")) as "Vente totale")
                       .orderBy(col("Shop_Id"))
    tmp121.withColumn("Vente",round(tmp("Vente totale"),2)).show()



    // 12.2) Bénéfice de vente en 2019 par boutique
    val tmp122 = (data_fusionItemTransaction
                     .where (col("annee") === 2019))
                     .groupBy(col("Shop_Id"), col("Shop_Name"))
                     .agg(sum(col("Item_Selling_Price")*col("Number") - col("Item_Buyed_Price")*col("Number")) as "benefice")
                     .orderBy(col("Shop_Id"))
    tmp122.withColumn("benefice",round(tmp("benefice"),2)).show()

    // 13) Capitale de vente en 2019 par mois dans la ville de Paris par catégorie d’items
    val tmp13 = data_fusionItemTransaction
                   .where (col("annee") === 2019 && col("Shop_Name")==="Paris")
                   .groupBy(col("mois"),col("Shop_Id"), col("Shop_Name"),col("Category_Id"),col("Category_item"))
                   .agg(sum(col("Item_Selling_Price")*col("Number") ) as "Vente")
                   .orderBy(col("Category_Id"),col("mois"),col("Shop_Id"))
    tmp13.withColumn("Vente",tmp13("Vente").cast(IntegerType)).show(Int.MaxValue)


    // 14) Bénéfice de vente en 2019 à Paris par catégories d’items
    val tmp14 = data_fusionItemTransaction
                    .where (col("annee") === 2019 && col("Shop_Name")==="Paris")
                    .groupBy(col("mois"),col("Shop_Id"), col("Shop_Name"),col("Category_item"))
                    .agg(sum(col("Item_Selling_Price")*col("Number") - col("Item_Buyed_Price")*col("Number") ) as "Vente")
                    .orderBy(col("Category_item"),col("mois"),col("Shop_Id"))
    tmp14.withColumn("Vente",tmp14("Vente").cast(IntegerType)).show(Int.MaxValue)


    // 15) Nombres items vendu a paris par catégories items
    val tmp15 = data_fusionItemTransaction
                   .where (col("annee") === 2019 && col("Shop_Name")==="Paris")
                   .groupBy(col("mois"),col("Shop_Id"), col("Shop_Name"),col("Category_item"))
                   .agg(sum(ccol("Number")) as "NBVente")
                   .orderBy(col("Category_item"),col("mois"),col("Shop_Id"))
    tmp15.withColumn("NBVente",tmp15("NBVente").cast(IntegerType)).show(Int.MaxValue)


    // 16) Nombres moyens items acheté par jours(1-30) en 2019 à Paris
    val tmp16 = data_fusionItemTransaction
                  .where (col("annee") === 2019 && col("Shop_Name")==="Paris")
                  .groupBy(col("jour mois"),col("Shop_Id"), col("Shop_Name"),col("Category_item"))
                  .agg(sum(col("Number")) as "NBVente")
                  .orderBy(col("Category_item"),col("jour mois"),col("Shop_Id"))
    tmp16.withColumn("NBVente",(tmp16("NBVente")/12).cast(IntegerType)).show(Int.MaxValue)


    // 17) Nombres moyens items acheté par jours(lundi, mardi, …, dimanche) en 2019 à Paris
    val tmp17 = data_fusionItemTransaction
                   .where (col("annee") === 2019 && col("Shop_Name")==="Paris")
                   .groupBy(col("jour mois"),col("Shop_Id"), col("Shop_Name"),col("Category_item"))
                   .agg(sum(col("Number")) as "NBVente")
                   .orderBy(col("Category_item"),col("mois"),col("Shop_Id"))
    tmp17.withColumn("Vente",tmp17("Vente").cast(IntegerType)).show(Int.MaxValue)

  }
}