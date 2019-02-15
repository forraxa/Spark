## Spark SQL

Indice:  
- [listar tablas declaradas](#listar-tablas-declaradas)  
- [selección de columnas](#selección-de-columnas)  
- [filtrar valor](#filtrar-valor)  
- [contar filas](#contar-filas)  
- [buscar etiquetas](#buscar-etiquetas)  
- [clausula where, and, in, group by having](#clausula-where-and-in-group-by-having)  
- [*prepara dataframe para hacer join](#prepara-dataframe-para-hacer-join)  
- [inner join](#inner join)  
- [selección de elementos distintos distinct()](#selección-de-elementos-distintos-distinct)  
- [función definida por el usuario (UDF)](#función-definida-por-el-usuario-UDF)  


```scala
package spark_sql

object Spark_SQL extends App with Context{

  //Crear un marco de datos
  val dfTags = sparkSession
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("/home/rcaride/Descargas/question_tags_10K.csv")
    .toDF("id", "tag")

  //registrar tabla temporal
  dfTags.createOrReplaceTempView("so_tags")
  }
```
### listar tablas declaradas
```scala
  //listar las tablas declaradas
  sparkSession.catalog.listTables().show()

  //listar las tablas declaradas utilizando SQL
  sparkSession.sql("show tables").show()
```
### selección de columnas
```scala
  //seleccionar columnas
  sparkSession
    .sql("select id, tag from so_tags limit 10")
    .show()
```
### filtrar valor
```scala
  //filtrar por un valor
  sparkSession
    .sql("select * from so_tags where tag = 'php'")
    .show()
```
### contar filas
```scala
  //contar el número de filas
  sparkSession
    .sql(
      """select
        |count(*) as php_count
        |from so_tags where tag = 'php'""".stripMargin)
    .show(10)
```
### buscar etiquetas
```scala
  //buscar por etiqueta que comience por s
  sparkSession
    .sql(
      """
        |select *
        |from so_tags
        |where tag like 's%'""".stripMargin)
    .show(10)
```
### clausula where, and, in, group by having
```scala
  //clausula where y and
  sparkSession
    .sql(
      """
        |select *
        |from so_tags
        |where tag like 's%'
        |and (id = 25 or id = 108)""".stripMargin)
    .show(10)

  //clausula in
  sparkSession
    .sql(
      """
        |select *
        |from so_tags
        |where id in (25, 108)""".stripMargin)
    .show(10)

  //group by
  sparkSession
    .sql(
      """
        |select tag, count(*) as count
        |from so_tags group by tag""".stripMargin)
    .show(10)

  //clausula having ordenada
  sparkSession
    .sql(
      """
        |select tag, count(*) as count
        |from so_tags group by tag having count >5 order by tag""".stripMargin)

    .show(10)
```
### *prepara dataframe para hacer join
```scala
  //preparar dataframes para hacer join
  // Typed dataframe, filter and temp table
  val dfQuestionsCSV = sparkSession
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("dateFormat","yyyy-MM-dd HH:mm:ss")
    .csv("/home/rcaride/Descargas/questions_10K.csv")
    .toDF("id", "creation_date", "closed_date", "deletion_date", "score", "owner_userid", "answer_count")

  // cast columns to data types
  val dfQuestions = dfQuestionsCSV.select(
    dfQuestionsCSV.col("id").cast("integer"),
    dfQuestionsCSV.col("creation_date").cast("timestamp"),
    dfQuestionsCSV.col("closed_date").cast("timestamp"),
    dfQuestionsCSV.col("deletion_date").cast("date"),
    dfQuestionsCSV.col("score").cast("integer"),
    dfQuestionsCSV.col("owner_userid").cast("integer"),
    dfQuestionsCSV.col("answer_count").cast("integer")
  )

  // filter dataframe
  val dfQuestionsSubset = dfQuestions.filter("score > 400 and score < 410").toDF()

  // register temp table
  dfQuestionsSubset.createOrReplaceTempView("so_questions")
```
### inner join
```scala
  //inner join
  //otros joins:left outer join, right outer join
  sparkSession
    .sql(
      """
        |select t.*, q.*
        |from so_questions q
        |inner join so_tags t
        |on t.id = q.id""".stripMargin)
    .show(10)
```
### selección de elementos distintos distinct()
```scala
  //distinct
  sparkSession
    .sql("select distinct tag from so_tags")
    .show(10)
```
### función definida por el usuario (UDF)
```scala
    //crear una función y utilizarla dentro del SQL
  def addPrefix(s: String): String = s"so_$s"

  sparkSession
    .udf
    .register("prefix_so", addPrefix _)

  sparkSession
    .sql("select id, prefix_so(tag) from so_tags")
    .show(10)
```
