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

#### Ejemplo de DataFrame Code
//encontrar los Top5 destinos 
```scala
import org.apache.spark.sql.functions._

val dataTops = flightData
  .groupBy($("Destino")
  .sum("count")
  .withColumnRenamed("sum(count)", "destino_total")
  .sort(desc("destino_total"))
  .limit(5)
```

#### Crear con case class un DataSet a partir de un RDD

```scala

import spark.implicits._

case class valueAndDouble(value: Long, valueDouble: Long)

spark.range(2000)
  .map(value =>valueAndDouble(value, value *2))
  .filter(vAndD => vAndD.valueDouble % 2 == 0)
  .where("value %3 = 0")
  .count()
```

#### Streaming
Para trabajar con datos en streaming es recomendable primero crear un schema para evitar inferir los datos continuamente.  

```scala
val staticDataFrame = spark
  .read
  .format("csv")
  .option("header", "true")
  .option("inferSchema", "true")
  .load("ruta/fichero.csv")
  
val staticSchema = staticDataFrame.schema
```

Lectura de los datos estáticos
```scala
import org.apache.spark.sql.functions.{window, col,desc, colum}

staticDataFrame
  .selectExpr("CustomerId", "UnitPrice * Quantity) as total_cost", "invoiceDate")
  .groupBy(col("customerID"), window(col("InvoiceDate"), "1 day"))
  .sum("total_cost")
  .orderBy(desc("sum(total_cost)"))
  .take(5)
```
Con el schema staticSchema creado se puede leer datos en streaming

```scala
val streamingDataFrame = spark
  .readStream
  .schema(staticSchema)
  .option("header","true")
  .format("csv")
  .load("ruta/fichero.csv")
```

Trabajar con el streaming creado
```scala
val purchaseByCustomerPerHour = streamingDataFrame
  .selectExpr("CustomerId", "UnitPrice * Quantity) as total_cost", "invoiceDate")
  .groupBy($"CustomerId", window($"InvoiceDate", "1 day"))
  .sum("total_cost")
```
Almacenar el/los streaming creados
```scala
purchaseByCustomerPerHour
  .writeStream
  .format("memory") //memory = el almacenamiento en tabla de memoria
  .queryName("customer_purchase") //nombre de la tabla de memoria
  .outputMode("complete") //complete = todo se guardará en la tabla
  .start()
```
