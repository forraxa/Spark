## Código de inicio con SparkSession.


### crear SparkSession

#### Crear Dataframe
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
    
    //leer a DataFrame
    val dataDF = spark.read
      .option("header","true")
      .option("inferSchema","true")
      .format("csv")
      .load("/home/rcaride/Descargas/fakefriends.csv")

    dataDF.show()
  }
}
```

#### Crear RDD
```
package basico

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object principal {

 case class Person(ID:Int, name:String, age:Int, numFriends:Int)
  
  def mapper(line:String): Person = {
    val fields = line.split(',')  
    
    val person:Person = Person(fields(0).toInt, fields(1), fields(2).toInt, fields(3).toInt)
    return person
  }

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
    
    //leer a RDD
    val dataRdd =sc.textFile("./fakefriends.txt")
    val people = lines.map(mapper)
    people.collect()
  }
}
```
