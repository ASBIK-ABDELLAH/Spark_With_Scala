package Init_Cleaning

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

object Refund {
  def Refund_Dataset: DataFrame = {
    val spark = Spark.spark()
    val schemaUntyped = new StructType()
      .add("refID", StringType)
      .add("txID", StringType)
      .add("custID", StringType)
      .add("prodID", StringType)
      .add("timestamp", StringType)
      .add("amountR", IntegerType)
      .add("quantityR", IntegerType)

    val df = spark.read.format("csv").option("delimiter", "|").schema(schemaUntyped).load("file:///C:/Users/21264/Desktop/scala/Refund.txt")

    return df
  }
}
