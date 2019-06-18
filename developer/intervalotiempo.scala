// Objetivo:
// Evaluar cuanto tiempo están inactivas una serie de maquinas

// id: referencia de la máquina
// start: ciclo o periodo temporal de comienzo
// end: ciclo o periodo temporal de finalización


package com.rcaride.spark
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType};
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.lag
import org.apache.spark.sql.functions._
import org.apache.log4j._

object intervalotiempo {
  def main(args: Array[String]) {
    
  // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    
  val spark = SparkSession.builder()
      .appName("intervalotiempo")
      .config("spark.some.config.option", "some-value")
      .master("local[*]")
      .getOrCreate()
      
   
  import spark.implicits._
  
  //declarar dataframe  
  val data = Seq(
      (1, 0, 3),
      (1, 4, 8),
      (1, 10, 20),
      (1, 20, 31),
      (1, 412, 578),
      (2, 231, 311),
      (2, 781, 790)      
      ).toDF("id", "start", "end")
      
   data.show()

/*
+---+-----+---+
| id|start|end|
+---+-----+---+
|  1|    0|  3|
|  1|    4|  8|
|  1|   10| 20|
|  1|   20| 31|
|  1|  412|578|
|  2|  231|311|
|  2|  781|790|
+---+-----+---+
*/
   
  //definimos una ventana ordenada por "start"  
  //necesitamos partitionBy para agrupar por id
  val ventana = Window.partitionBy($"id").orderBy("start")
 
  //definimos una nueva columna "prevEnd" con la columna "end" lageada un posición sobre la ventana creada.
  val prevEnd = lag($"end", 1).over(ventana)

  //unir la columna "prevEnd" con el dataframe "data"
  val withPrevEnd = data.withColumn("prevEnd", prevEnd)
  withPrevEnd.show
  
/*
+---+-----+---+-------+
| id|start|end|prevEnd|
+---+-----+---+-------+
|  1|    0|  3|   null|
|  1|    4|  8|      3|
|  1|   10| 20|      8|
|  1|   20| 31|     20|
|  1|  412|578|     31|
|  2|  231|311|   null|
|  2|  781|790|    311|
+---+-----+---+-------+
*/
  
  //para cada fila calcular la diferencia entre el "start" y el "prevEnd" que es la finalización de la anterior 
  val idleIntervals = withPrevEnd.withColumn("diff", $"start"- $"prevEnd")
  idleIntervals.show
  
/*
+---+-----+---+-------+----+
| id|start|end|prevEnd|diff|
+---+-----+---+-------+----+
|  1|    0|  3|   null|null|
|  1|    4|  8|      3|   1|
|  1|   10| 20|      8|   2|
|  1|   20| 31|     20|   0|
|  1|  412|578|     31| 381|
|  2|  231|311|   null|null|
|  2|  781|790|    311| 470|
+---+-----+---+-------+----+
*/
  
  //calcular el total de ciclos de tiempo que han estado las máquinas.
  val totalIdleIntervals = idleIntervals.select($"id",$"diff").groupBy($"id").agg(sum("diff"))
  totalIdleIntervals.show()
  
/*
+---+---------+
| id|sum(diff)|
+---+---------+
|  1|      384|
|  2|      470|
+---+---------+  
 */
  
  }     
}
