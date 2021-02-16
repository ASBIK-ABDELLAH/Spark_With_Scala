package Init_Cleaning

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{StringType, StructType}

object Customer {
  def Customer_Dataset: DataFrame = {
    val spark = Spark.spark()
    val schemaUntyped = new StructType()
      .add("CustID", StringType)
      .add("Firstname", StringType)
      .add("Lastname", StringType)
      .add("phone", StringType)

    val df = spark.read.format("csv").option("delimiter", "|").schema(schemaUntyped).load("file:///C:/Users/21264/Desktop/scala/Customer.txt")

    return df
  }
}
