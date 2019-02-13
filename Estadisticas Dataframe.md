## Estadísticas Dataframe

```
package estadisticas_dataframe

object Estadisticas extends App with Context{

  // Create a dataframe from tags file question_tags_10K.csv
  val dfTags = sparkSession
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("/home/rcaride/Descargas/question_tags_10K.csv")
    .toDF("id", "tag")

  // Create a dataframe from questions file questions_10K.csv
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
 }
```
```
 //average
  //necesario importar functions
  import org.apache.spark.sql.functions._
  dfQuestions
    .select(avg("score"))
    .show(10)

  //máximo
  import org.apache.spark.sql.functions._
  dfQuestions
    .select(max("score"))
    .show(10)

  //mínimo
  import org.apache.spark.sql.functions._
  dfQuestions
    .select(min("score"))
    .show(10)

  //mean
  import org.apache.spark.sql.functions._
  dfQuestions
    .select(mean("score"))
    .show(10)

  //sum
  import org.apache.spark.sql.functions._
  dfQuestions
    .select(sum("score"))
    .show(10)
```
```
  //estadística de datos agrupados
  dfQuestions
    .filter("id > 400 and id < 450")
    .filter("owner_userid is not null")
    .join(dfTags, dfQuestions.col("id").equalTo(dfTags("id")))
    .groupBy(dfQuestions.col("owner_userid"))
    .agg(avg("score"), max("answer_count"))
    .show(10)

  //describe() para estadísticas básicas
  val dfQuestionsStadistics = dfQuestions.describe()
  dfQuestionsStadistics.show()
```
```
  //correlación
  val correlation = dfQuestions.stat.corr("score", "answer_count")
  println(s"la correlación entre las columnas score y answer_count es = $correlation")

  //covarianza
  val covariance = dfQuestions.stat.cov("score", "answer_count")
  println(s"la correlación entre las columnas score y answer_count es = $covariance")
```
```
  //frecuencia de términos
  val dfFrequentScore = dfQuestions.stat.freqItems(Seq("answer_count"))
  dfFrequentScore.show()

  //tabla de frecuencia o contingencia
  val dfScoreUserid = dfQuestions
    .filter("owner_userid > 0 and owner_userid < 20")
    .stat
    .crosstab("score", "owner_userid")
  dfScoreUserid.show(10)
```
```
  //hacer selección aleatoria de muestras bajo la premisa de un tanto porciento
  //de las muestras para cada valor determinado.
  //selección de las muestras con valor 5, 10 o 20
  val dfQuestionsByAnswerCount = dfQuestions
    .filter("owner_userid > 0")
    .filter("answer_count in (5, 10, 20)")

  //contar las muestras para cada valor
  dfQuestionsByAnswerCount
    .groupBy("answer_count")
    .count()
    .show()

  //se crea una colección Map especificando el % de muestras para cada valor.
  //especificando el % en rango [0,1]
  val fractionKeyMap = Map(5 -> 0.5, 10 -> 0.1, 20 -> 1.0)

  dfQuestionsByAnswerCount
    .stat
    .sampleBy("answer_count", fractionKeyMap, 7L)
    .groupBy("answer_count")
    .count()
    .show()
```
```
  //Cuantile aproximado
  //en el metodo approxQuantile() el primer parámetro el la columna sobre la
  //que ejecutar las estadísticas, el segundo es la matriz de probabilidades (0=mínimo, 0.5 = mediana y 1 = máximo)
  // y el tercer parametro es el factor de error de precisión.
  val quantiles = dfQuestions
    .stat
    .approxQuantile("score", Array(0, 0.5, 1), 0.25)
  println(s"Quantiles segments = ${quantiles.toSeq}")

  //se pueden verificar los resultados consultando los cuantiles con Spark SQL
  dfQuestions.createOrReplaceTempView("so_questions")
  sparkSession
    .sql("select min(score), percentile_approx(score, 0.25), max(score) from so_questions")
    .show()
```
```
  //filtro bloom
  //estructura probabilística usada para verificar si un elemento es miembro de un
  // conjunto. Los falsos positivos son posibles pero los falsos negativos no.
  //primer parámetro = columna de datos
  //segundo parámetro = número de elementos en el conjunto de filtros bloom
  //tercer parámetro = factor positivo falso.
  val tagsBloomFilter = dfTags.stat.bloomFilter("tag", 1000L, 0.1)

  //en lugar de consultar directamente el dfTags del marco de datos,
  //se usará el método mightContain() de filter Bloom para comprobar si existen ciertas coincidencias
  println(s"bloom filter contains java tag = ${tagsBloomFilter.mightContain("java")}")
  println(s"bloom filter contains some unknown tag = ${tagsBloomFilter.mightContain("unknown tag")}")
```
```
  //muestreo con remplazo
  //sample(reemplazo, num_filas, semilla)
  val dfTagsSample = dfTags.sample(true, 0.2, 37L)
  println(s"Number of rows in sample dfTagsSample = ${dfTagsSample.count()}")
  println(s"Number of rows in dfTags = ${dfTags.count()}")
```
