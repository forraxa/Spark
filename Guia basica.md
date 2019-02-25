## Guía básica de Spark  

#### Trait con sparkSession
```scala
package spark_rcaride

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

trait Context {

  //configurar el log para imprimir sólo errores
  Logger.getLogger("org").setLevel(Level.ERROR)

  //se crea un sparkConf
  lazy val sparkConf = new SparkConf()
    .setAppName("practica Spark")
    .setMaster("local[*]")
    .set("spark.cores.max", "2")

  //se crea el sparkSession
  lazy val spark = SparkSession
    .builder()
    .config(sparkConf)
    .getOrCreate()
}
```

#### Leer datos 
```scala
val =  flightData = spark
  .read
  .option("inferSchema", "true")
  .option("header", "true")
  .format("csv")
  .load("ruta/fichero.csv")
```
**Tip**:  
//para ver el plan de ejecución  
```scala
flightData.sort("count").explain()
```

#### DataFrame Code
```scala
val dataFrameCode = flightData
  .groupBy($"Dest_country_name")
  .count()
```
