## DataFrame SQL


```
package dataframe_sql

object DataFrame_Tutorial extends App with Context {

  // Create a DataFrame from reading a CSV file
  val dfTags = sparkSession
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("./question_tags_10K.csv")
    .toDF("id", "tag")

  dfTags.show(10)
```
```
  dfTags.select("id", "tag").show(10)
```
```
  //filter
  dfTags.filter("tag == 'php'").show(10)
  println(s"El numero de tags php es: ${dfTags.filter("tag == 'php'").count()}")
  dfTags.filter("tag like 'so%k%s'").show(10)
  dfTags
    .filter("tag like 's%'")
    .filter("id == 25 or id == 108")
    .show(10)
  dfTags.filter("id in (25, 108)").show(10)
```
```  
  //groupBy
  dfTags.groupBy("tag").count().show(10)
  dfTags.groupBy("tag").count().filter("count > 5").show(10)
  dfTags.groupBy("tag").count().filter("count > 5").orderBy("count").show(10)
```
```
  // DataFrame Query: Cast columns to specific data type
  val dfQuestionsCSV = sparkSession
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("dateFormat", "yyyy-MM-dd HH:mm:ss")
    .csv("/home/rcaride/Descargas/questions_10K.csv")
    .toDF("id", "creation_date", "closed_date", "deletion_date", "score", "owner_userid", "answer_count")

  dfQuestionsCSV.printSchema()
```
```
  //conversión de tipos
  val dfQuestions = dfQuestionsCSV.select(
    dfQuestionsCSV.col("id").cast("integer"),
    dfQuestionsCSV.col("creation_date").cast("timestamp"),
    dfQuestionsCSV.col("closed_date").cast("timestamp"),
    dfQuestionsCSV.col("deletion_date").cast("date"),
    dfQuestionsCSV.col("score").cast("integer"),
    dfQuestionsCSV.col("owner_userid").cast("integer"),
    dfQuestionsCSV.col("answer_count").cast("integer")
  )

  dfQuestions.printSchema()
  dfQuestions.show(10)
```
```
  //marco de datos acotado
  val dfQuestionsSubset = dfQuestions.filter("score > 400 and score < 410").toDF()
  dfQuestionsSubset.show()
```
  //join
  dfQuestionsSubset.join(dfTags, "id").show(10)

  //join especificando columna de unión
  dfQuestionsSubset
    .join(dfTags, dfTags("id") === dfQuestionsSubset("id"))
    .show(10)

  //inner join
  //otros joins: inner, cross, outer, full, full_outer, left, left_outer, right, right_outer, left_semi, left_anti
  dfQuestionsSubset
    .join(dfTags, Seq("id"), "inner")
    .show(10)
```
  //Distinct
  dfTags
    .select("tag")
    .distinct()
    .show(10)

}
```
```
