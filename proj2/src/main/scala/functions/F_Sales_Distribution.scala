package functions

import Init_Cleaning.{Products, Sales, Spark}
import org.apache.spark.sql.functions._

object F_Sales_Distribution {
  def main(args: Array[String]): Unit = {
    val spark = Spark.spark()
    val df_sales = Sales.Sales_Dataset
    val df_Products = Products.Products_Dataset

    val df_join = df_Products.join(df_sales, "prodID")
    val df_distribution = df_join.groupBy("name").agg(sum("amount").alias("S_amount"),sum("quantity").alias("S_quantity"))
    df_distribution.show()
    val df_product = df_distribution.withColumn("Total", col("S_amount")*col("S_quantity"))
    val x = df_product.agg(max("Total")).collectAsList().get(0)(0)
    val df_best_product = df_product.filter(s"Total == ${x}").select("name", "Total")
    df_best_product.show()
    System.in.read()
    spark.stop()
  }
}
