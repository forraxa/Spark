## Funciones en DataFrames




```scala
package funcionesDataframe

object funcionesDataframe extends App with Context {

  //crear dataframe con tuplas
  val donuts = Seq(("plain donut", 1.50), ("vanilla donut", 2.0), ("glazed donut", 2.50))
  val df = sparkSession
    .createDataFrame(donuts)
    .toDF("Donut Name", "Price")

  df.show()
```
```scala
  //nombres de columnas en DataFrame
  val columnNames: Array[String] = df.columns
  columnNames.foreach(name => println(s"$name"))

  //nombres y tipo de columnas en DataFrame
  val (columnNames2, columnDataTypes) = df.dtypes.unzip
  println(s"DataFrame column names = ${columnNames2.mkString(", ")}")
  println(s"DataFrame column data types = ${columnDataTypes.mkString(", ")}")
```
```scala
  //Json en DataFrame
  //leer Json
  import sparkSession.sqlContext.implicits._
  val tagsDF = sparkSession
    .read
    .option("multiLine", true)
    .option("inferSchema", true)
    .json("/home/rcaride/Descargas/tags_sample.json")

  //modificar alias de Json e imprimir schema
  import org.apache.spark.sql.functions._
  val df2 = tagsDF.select(explode($"stackoverflow") as "stackoverflow_tags")
  df2.printSchema()

  //seleccionar tags de Json
  df2.select(
    $"stackoverflow_tags.tag.id" as "id",
    $"stackoverflow_tags.tag.author" as "author",
    $"stackoverflow_tags.tag.name" as "tag_name",
    $"stackoverflow_tags.tag.frameworks.id" as "frameworks_id",
    $"stackoverflow_tags.tag.frameworks.name" as "frameworks_name"
  ).show()
```
```scala
  //join de DataFrames
  val donuts2 = Seq(("111","plain donut", 1.50), ("222", "vanilla donut", 2.0), ("333","glazed donut", 2.50))

  val dfDonuts = sparkSession
    .createDataFrame(donuts2)
    .toDF("Id","Donut Name", "Price")
  dfDonuts.show()

  val inventory = Seq(("111", 10), ("222", 20), ("333", 30))
  val dfInventory = sparkSession
    .createDataFrame(inventory)
    .toDF("Id", "Inventory")
  dfInventory.show()

  val dfDonutsInventory = dfDonuts.join(dfInventory, Seq("Id"), "inner")
  dfDonutsInventory.show()
```
```scala
  //buscar en columna dataFrame usando array_contains()
  val df3 = df2.select(
    $"stackoverflow_tags.tag.id" as "id",
    $"stackoverflow_tags.tag.author" as "author",
    $"stackoverflow_tags.tag.name" as "tag_name",
    $"stackoverflow_tags.tag.frameworks.id" as "frameworks_id",
    $"stackoverflow_tags.tag.frameworks.name" as "frameworks_name"
  ).toDF()
  import sparkSession.sqlContext.implicits._
  df3
    .select("*")
    .where(array_contains($"frameworks_name","Play Framework"))
    .show()
```
```scala
  //comprobar que existe una columna en el DataFrame
  val donuts = Seq(("plain donut", 1.50), ("vanilla donut", 2.0), ("glazed donut", 2.50))
  val df = sparkSession.createDataFrame(donuts).toDF("Donut Name", "Price")

  df.show()

  val priceColumnExists = df.columns.contains("Price")
  println(s"Does price column exist = $priceColumnExists")
```
```scala
  //dividir columna de Dataframe compuesta por array
  /*+----------------+----------+
  |            Name|    Prices|
  +----------------+----------+
  |     Plain Donut|[1.5, 2.0]|
  |   Vanilla Donut|[2.0, 2.5]|
  |Strawberry Donut|[2.5, 3.5]|
  +----------------+----------+*/
  val df2 = df
    .select(
      $"Name",
      $"Prices"(0).as("Low Price"),
      $"Prices"(1).as("High Price")
    )
/*  +----------------+---------+----------+
  |            Name|Low Price|High Price|
  +----------------+---------+----------+
  |     Plain Donut|      1.5|       2.0|
  |   Vanilla Donut|      2.0|       2.5|
  |Strawberry Donut|      2.5|       3.5|
  +----------------+---------+----------+*/
```
```
  //cambiar el nombre de una columna de un Dataframe
  //df.withColumnRenamed("existingName", "newName")
  val df2 = df.withColumnRenamed("Donut Name", "Name")
  df2.show()
```
```scala
  //crear una columna con una constante
  //la función lit crea una columna con una valor literal
  //typedLit es igual que lit pero maneja List, Seq y Map.
  val df2 = df
    .withColumn("Tasty", lit(true))
    .withColumn("Correlation", lit(1))
    .withColumn("Stock Min Max", typedLit(Seq(100, 500)))
```
```scala
  //nueva columna en DataFrame con UDF
  val donuts = Seq(("plain donut", 1.50), ("vanilla donut", 2.0), ("glazed donut", 2.50))
  val df = spark.createDataFrame(donuts).toDF("Donut Name", "Price")

  import org.apache.spark.sql.functions._
  import org.apache.spark.sqlContext.implicits._

  val stockMinMax: (String => Seq[Int]) = (donutName: String) => donutName match {
    case "plain donut"    => Seq(100, 500)
    case "vanilla donut"  => Seq(200, 400)
    case "glazed donut"   => Seq(300, 600)
    case _                => Seq(150, 150)
  }

  val udfStockMinMax = udf(stockMinMax)
  val df2 = df.withColumn("Stock Min Max", udfStockMinMax($"Donut Name"))
  df2.show()
```
```scala
  //seleccionar de la primera fila una columna concreta
  //toda primera fila
  val firstRow = df.first()
  println(s"First row = $firstRow")

  //la primera fila y la primera columna
  val firstRowColumn1 = df.first().get(0)
  println(s"First row column 1 = $firstRowColumn1")

  //la primera fila de la columna Price
  val firstRowColumnPrice = df.first().getAs[Double]("Price")
  println(s"First row column Price = $firstRowColumnPrice")
```
```scala
  //formato de la columna
  //formato de Price: usar format_number() para formatear a 2 decimales
  //formato de name: usar format_string() para anteponer "awesome"
  //Name en mayúsculas: usar upper()
  //Name en minúsculas: usar lower()
  //formato de date: usar date_format() para formato yyyMMdd
  //day: usar dayofmonth() para extraer el día del mes de una fecha
  //month: usar month() para extraer el mes de la fecha
  //year: usar year() para extraer el año de una fecha
  val donuts = Seq(("plain donut", 1.50, "2018-04-17"), ("vanilla donut", 2.0, "2018-04-01"), ("glazed donut", 2.50, "2018-04-02"))
  val df = spark.createDataFrame(donuts).toDF("Donut Name", "Price", "Purchase Date")

  import org.apache.spark.sql.functions._
  import spark.sqlContext.implicits._

  df
    .withColumn("Price Formatted", format_number($"Price", 2))
    .withColumn("Name Formatted", format_string("awesome %s", $"Donut Name"))
    .withColumn("Name Uppercase", upper($"Donut Name"))
    .withColumn("Name Lowercase", lower($"Donut Name"))
    .withColumn("Date Formatted", date_format($"Purchase Date", "yyyyMMdd"))
    .withColumn("Day", dayofmonth($"Purchase Date"))
    .withColumn("Month", month($"Purchase Date"))
    .withColumn("Year", year($"Purchase Date"))
    .show()
```
```scala
  //funciones de String en DataFrames
  //instr() genera el índice de las coincidencias al buscar una cadena
  //length() determina la longitud de una cadena
  //trim() elimina los espacios en blanco a ambos lados del texto
  //ltrim() elimina espacios en blanco por la izquierda
  //rtrim() elimina espacios en blanco por la derecha
  //reverse() genera el texto en orden inverso
  //substring() genera el texto para el texto entre los indices dados
  //isnull() genera true o false si el texto es nulo
  //concat_ws() concatena columnas o proporciona un separador textual dado
  //initcap() convierte la primera letra de cada palabra en mayúsculas
  val donuts = Seq(("plain donut", 1.50, "2018-04-17"), ("vanilla donut", 2.0, "2018-04-01"), ("glazed donut", 2.50, "2018-04-02"))
  val df = spark
    .createDataFrame(donuts)
    .toDF("Donut Name", "Price", "Purchase Date")

  import org.apache.spark.sql.functions._
  import spark.sqlContext.implicits._

  df
    .withColumn("Contains plain", instr($"Donut Name", "donut"))
    .withColumn("Length", length($"Donut Name"))
    .withColumn("Trim", trim($"Donut Name"))
    .withColumn("LTrim", ltrim($"Donut Name"))
    .withColumn("RTrim", rtrim($"Donut Name"))
    .withColumn("Reverse", reverse($"Donut Name"))
    .withColumn("Substring", substring($"Donut Name", 0, 5))
    .withColumn("IsNull", isnull($"Donut Name"))
    .withColumn("Concat", concat_ws(" - ", $"Donut Name", $"Price"))
    .withColumn("InitCap", initcap($"Donut Name"))
    .show()

  /*+-------------+-----+-------------+--------------+------+-------------+-------------+-------------+-------------+---------+------+-------------------+-------------+
  |   Donut Name|Price|Purchase Date|Contains plain|Length|         Trim|        LTrim|        RTrim|      Reverse|Substring|IsNull|             Concat|      InitCap|
    +-------------+-----+-------------+--------------+------+-------------+-------------+-------------+-------------+---------+------+-------------------+-------------+
  |  plain donut|  1.5|   2018-04-17|             7|    11|  plain donut|  plain donut|  plain donut|  tunod nialp|    plain| false|  plain donut - 1.5|  Plain Donut|
  |vanilla donut|  2.0|   2018-04-01|             9|    13|vanilla donut|vanilla donut|vanilla donut|tunod allinav|    vanil| false|vanilla donut - 2.0|Vanilla Donut|
  | glazed donut|  2.5|   2018-04-02|             8|    12| glazed donut| glazed donut| glazed donut| tunod dezalg|    glaze| false| glazed donut - 2.5| Glazed Donut|
  +-------------+-----+-------------+--------------+------+-------------+-------------+-------------+-------------+---------+------+-------------------+-------------+
*/
```
```scala
  //Eliminar nulos
  //na.drop() elimina los valores nulos
  val dfWithoutNull = dfWithNull.na.drop()
  dfWithoutNull.show()
```
