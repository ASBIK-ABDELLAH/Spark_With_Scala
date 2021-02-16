package Init_Cleaning

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

object Products {
  def Products_Dataset: DataFrame = {
    val spark = Spark.spark()
    val schemaUntyped = new StructType()
      .add("prodID", StringType)
      .add("name", StringType)
      .add("type", StringType)
      .add("version", StringType)
      .add("price", IntegerType)

    val df = spark.read.format("csv").option("delimiter", "|").schema(schemaUntyped).load("file:///C:/Users/21264/Desktop/scala/Product.txt")

    return df
  }
}
