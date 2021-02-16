package sql

import Init_Cleaning._

object Total_Customer {
  def main(args: Array[String]): Unit = {
    val spark = Spark.spark()
    val df_sales: Unit = Sales.Sales_Dataset.createOrReplaceTempView("Sales")
    val df_Customer: Unit = Customer.Customer_Dataset.createOrReplaceTempView("Customer")

    val total_Customer = spark.sql("Select S.custID as Id, C.Firstname, C.Lastname, Count(S.prodID) as count " +
      "FROM Sales as S " +
      "Inner join Customer as C on S.custID = C.custID " +
      "Group by C.Firstname,C.Lastname,S.custID ")
    total_Customer.show()
    System.in.read()
    spark.stop()
  }
}
