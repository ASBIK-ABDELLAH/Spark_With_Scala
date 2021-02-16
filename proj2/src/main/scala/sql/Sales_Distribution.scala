package sql

import Init_Cleaning._

object Sales_Distribution {
  def main(args: Array[String]): Unit = {
    val spark = Spark.spark()
    val df_sales: Unit = Sales.Sales_Dataset.createOrReplaceTempView("Sales")
    val df_Products: Unit = Products.Products_Dataset.createOrReplaceTempView("Product")


    val sum_table = spark.sql("SELECT P.name as name, SUM(S.amount) as sum_amount, SUM(S.quantity) as sum_quantity, SUM(S.amount)*SUM(S.quantity) as Total  " +
      "FROM Sales as S " +
      "INNER JOIN Product as P ON S.prodID = P.prodID " +
      "GROUP BY P.name")

    sum_table.createOrReplaceTempView("sum_table")
    val Best_product = spark.sql("select name, Total " +
      "from sum_table " +
      "where Total = (select Max(Total) from sum_table)").show()

    val Avg_product = spark.sql("select AVG(Total) as AVG_Sales " +
      "From sum_table")
    System.in.read()
    spark.stop()
  }
}
