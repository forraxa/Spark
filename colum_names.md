## Nombre de columnas

```
package scala.ejercicios

import org.apache.spark.sql.SparkSession

object column_name {

  def main(args: Array[String]) {
    val spark = SparkSession.builder()
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._

    val someDF = Seq(
      (8, "bat"),
      (64, "mouse"),
      (-27, "horse")
    ).toDF("number", "word")
    
    val colNames = Seq("numbers", "words")
    val secondDF = someDF.toDF(colNames: _*)
  }
}
```
