package functions

import Init_Cleaning.{Customer, Sales, Spark}

object F_Customer {
  def main(args: Array[String]): Unit = {
    val spark = Spark.spark()
    val df_sales= Sales.Sales_Dataset
    val df_Customer = Customer.Customer_Dataset

    val df_join = df_Customer.join(df_sales,"custID")
    val Total_Customer = df_join.groupBy("custID", "Lastname", "Firstname").count()
    Total_Customer.show()
    System.in.read()
    spark.stop()
  }
}
