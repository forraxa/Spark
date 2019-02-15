## Operaciones con DataFrame

package operaciones_dataframe

```
import org.apache.spark.sql.Dataset

object DataFrameOperations extends App with Context{

  val dfTags = sparkSession
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("/home/rcaride/Descargas/question_tags_10K.csv")
    .toDF("id", "tag")

  dfTags.show(10)

  val dfQuestionsCSV = sparkSession
    .read
    .option("header", false)
    .option("inferSchema", true)
    .option("dateFormat","yyyy-MM-dd HH:mm:ss")
    .csv("/home/rcaride/Descargas/questions_10K.csv")
    .toDF("id", "creation_date", "closed_date", "deletion_date", "score", "owner_userid", "answer_count")

  val dfQuestions = dfQuestionsCSV
    .filter("score > 400 and score < 410")
    .join(dfTags, "id")
    .select("owner_userid", "tag", "creation_date", "score")
    .toDF()

  dfQuestions.show(10)
}
```
```
  //convertir el dataFrame en un dataset con la case class Tag creada
  case class Tag(id: Int, tag: String)
  import sparkSession.implicits._
  val dfTagsOfTag: Dataset[Tag] = dfTags.as[Tag]
  dfTagsOfTag
    .take(10)
    .foreach(t => println(s"id = ${t.id}, tag = ${t.tag}"))
```
```
  //convertir filas de dataFrame a case class con map()
  //creación de case class Question
  case class Question(owner_userid: Int, tag: String, creationDate: java.sql.Timestamp, score: Int)

  println("\nStep 4: Using String interpolation for formatting text")
  val donutName: String = "Vanilla Donut"
  val donutTasteLevel: String = "Tasty"
  println(f"$donutName%20s $donutTasteLevel")
```
```
  //crear dataFrame desde una colección
  val seqTags = Seq(
    1 -> "so_java",
    1 -> "so_jsp",
    2 -> "so_erlang",
    3 -> "so_scala",
    3 -> "so_akka"
  )
  val dfMoreTags = seqTags.toDF("id", "tag")
  dfMoreTags.show(15)
```
```
  //dataFrame union
  //Para combinar dos dataframe
  val dfUnionOfTags = dfTags
    .union(dfMoreTags)
    .filter("id in (1,3)")
  dfUnionOfTags.show(10)

  //dataFrame intersección
  val dfIntersectionTags = dfMoreTags
    .intersect(dfUnionOfTags)
    .show(10)

  //agregar columnas a un dataFrame
  import org.apache.spark.sql.functions._
  val dfSplitColumn = dfMoreTags
    .withColumn("tmp", split($"tag", "_"))
    .select(
      $"id",
      $"tag",
      $"tmp".getItem(0).as("so_prefix"),
      $"tmp".getItem(1).as("so_tag")
    ).drop("tmp")
  dfSplitColumn.show(10)
```
