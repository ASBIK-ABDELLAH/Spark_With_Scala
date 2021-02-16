package Init_Cleaning

import org.apache.spark.sql.functions.{col, when}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, functions}

object Sales {

  def Sales_Dataset: DataFrame = {
    val spark = Spark.spark()
    val schemaUntyped = new StructType()
      .add("txID", StringType)
      .add("custID", StringType)
      .add("prodID", StringType)
      .add("timestamp", StringType)
      .add("amount", IntegerType)
      .add("quantity", IntegerType)

    //Connexion a la table
    val df = spark.read.format("csv").option("delimiter", "|").schema(schemaUntyped).load("file:///C:/Users/21264/Desktop/scala/Sales.txt")

    val df2 = df.withColumn("J_M_A", functions.split(col("timestamp"), " ").getItem(0))
      .withColumn("Heure", functions.split(col("timestamp"), " ").getItem(1))
      .drop("timestamp")

    val df3 = df2.withColumn("Annee", functions.split(col("J_M_A"), "/").getItem(2))
      .drop("J_M_A")

    val df4 = df3.withColumn("Annee", when(col("Annee").equalTo(13), 2013)
      .otherwise(col("Annee")))
      .withColumn("Annee", when(col("Annee").equalTo(12), 2012)
        .otherwise(col("Annee")))
    return df4

  }
}
