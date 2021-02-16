package functions

import Init_Cleaning.{Refund, Sales, Spark}
import org.apache.spark.sql.functions._

object F_Sales_2013 {

  def main(args: Array[String]): Unit = {
    val spark = Spark.spark()
    val df_sales_2013 = Sales.Sales_Dataset
    val df_refund = Refund.Refund_Dataset

    val df_produit_2013 = df_sales_2013.withColumn("Produit",col("amount")*col("quantity"))
    val df_total_2013 = df_produit_2013.filter("Annee == 2013").agg(sum("Produit"))
    df_total_2013.show()

    val df_join = df_sales_2013.join(df_refund, "txID")
    val df_multi = df_join.withColumn("Produit_S",(col("amount")*col("quantity"))-(col("amountR")*col("quantityR")))
    val Sales_Refund = df_multi.agg(sum("Produit_S"))
    Sales_Refund.show()
    System.in.read()
    spark.stop()
  }

}
