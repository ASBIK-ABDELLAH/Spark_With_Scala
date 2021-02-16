package sql

import Init_Cleaning._

object Sales_2013 {
  def main(args: Array[String]): Unit = {
    val spark = Spark.spark()
    val df_sales: Unit = Sales.Sales_Dataset.createOrReplaceTempView("Sales")
    val df_Refund: Unit = Refund.Refund_Dataset.createOrReplaceTempView("Refund")

    val Total_2013 = spark.sql("SELECT * " +
      "FROM (SELECT Annee, Sum(amount*quantity) as Total_2013 from Sales group by Annee) " +
      "Where Annee = 2013 ")
    Total_2013.show()
    val Total_refund = spark.sql("Select sum(S.amount*S.quantity)-sum(R.amountR*R.quantityR) as Net " +
      "From Sales as S " +
      "Inner join Refund as R ON S.txID = R.txID")
    Total_refund.show()
    System.in.read()
    spark.stop()
  }
}
