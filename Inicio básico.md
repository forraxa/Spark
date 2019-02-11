## Código de inicio con SparkSession.


### crear SparkSession

```
package basico

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object principal {

  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .master("local")
      .appName("example of SparkSession")
      .master("local[*]")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

       spark.conf.set ("spark.sql.shuffle.partitions", 6)
       spark.conf.set ("spark.executor.memory", "2g")

    val dataDF = spark.read
      .option("header","true")
      .option("inferSchema","true")
      .format("csv")
      .load("/home/rcaride/Descargas/fakefriends.csv")

    dataDF.show()
  }
}
```
