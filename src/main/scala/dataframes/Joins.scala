package dataframes

import dataframes.DataSources.spark
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, expr, max}
import org.apache.spark.sql.DataFrame

object Joins extends App {

  val spark = SparkSession.builder()
    .appName("Joins")
    .config("spark.master", "local")
    .getOrCreate()

  Logger.getRootLogger().setLevel(Level.ERROR)

  val guitarsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/guitars.json")

  guitarsDF.show()

  val guitaristsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/guitarPlayers.json")

  guitaristsDF.show()

  val bandsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/bands.json")

  bandsDF.show()

  // inner joins
  val joinCondition = guitaristsDF.col("band") === bandsDF.col("id")
  val guitaristsBandsDF = guitaristsDF.join(bandsDF, joinCondition, "inner")

  // outer joins
  // left outer = everything in the inner join + all the rows in the LEFT table, with nulls in where the data is missing
  guitaristsDF.join(bandsDF, joinCondition, "left_outer")

  // right outer = everything in the inner join + all the rows in the RIGHT table, with nulls in where the data is missing
  guitaristsDF.join(bandsDF, joinCondition, "right_outer")

  // outer join = everything in the inner join + all the rows in BOTH tables, with nulls in where the data is missing
  guitaristsDF.join(bandsDF, joinCondition, "outer")

  // semi-joins = everything in the left DF for which there is a row in the right DF satisfying the condition
  guitaristsDF.join(bandsDF, joinCondition, "left_semi")

  // anti-joins = everything in the left DF for which there is NO row in the right DF satisfying the condition
  guitaristsDF.join(bandsDF, joinCondition, "left_anti")

  guitaristsBandsDF.show()

  // things to bear in mind
  // guitaristsBandsDF.select("id", "band").show // this crashes

  // option 1 - rename the column on which we are joining
  guitaristsDF.join(bandsDF.withColumnRenamed("id", "band"), "band")

  // option 2 - drop the dupe column
  guitaristsBandsDF.drop(bandsDF.col("id"))

  // option 3 - rename the offending column and keep the data
  val bandsModDF = bandsDF.withColumnRenamed("id", "bandId")
  guitaristsDF.join(bandsModDF, guitaristsDF.col("band") === bandsModDF.col("bandId"))

  guitaristsDF.join(bandsDF, Seq("id"), "inner").show()

  // using complex types
  guitaristsDF.join(guitarsDF.withColumnRenamed("id", "guitarId"), expr("array_contains(guitars, guitarId)"),"outer").show()

  /**
    * Exercises
    *
    * 1. show all employees and their max salary
    * 2. show all employees who were never managers
    * 3. find the job titles of the best paid 10 employees in the company
    */

  val driver = "org.postgresql.Driver"
  val url = "jdbc:postgresql://localhost:5432/rtjvm"
  val user = "docker"
  val password = "docker"

  def readTable(name:String): DataFrame = spark.read
    .format("jdbc")
    .option("driver", driver)
    .option("url", url)
    .option("user", user)
    .option("password", password)
    .option("dbtable", s"public.$name")
    .load()

  val employeesDF = readTable("employees")
  employeesDF.printSchema()

  val salariesDF = readTable("salaries")
  salariesDF.printSchema()

  val deptManagerDF = readTable("dept_manager")
  deptManagerDF.printSchema()

  val titlesDF = readTable("titles")
  titlesDF.printSchema()

  // 1
  val maxSalaryPerEmp = salariesDF
    .groupBy("emp_no")
    .agg(max("salary")
      .alias("max_salary"))

  val EmpSalaryDF = employeesDF
    .join(maxSalaryPerEmp,Seq("emp_no"),"outer")
    .orderBy(col("max_salary").desc_nulls_last)

  EmpSalaryDF
    .select("first_name","last_name","max_salary")
    .show()

  // 2
  employeesDF
    .join(deptManagerDF,Seq("emp_no"),"left_anti")
    .select("first_name","last_name")
    .show()

  // 3
  val recentTitlesDF = titlesDF
    .groupBy("emp_no","title")
    .agg(max("to_date")
      .alias("date"))

  val bestPaidEmpDF = EmpSalaryDF
    .orderBy(col("max_salary").desc_nulls_last)
    .limit(10)

  bestPaidEmpDF
    .join(recentTitlesDF,Seq("emp_no"))
    .show()

}
