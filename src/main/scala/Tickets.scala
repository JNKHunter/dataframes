import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SparkSession, types}
import org.apache.spark.sql._

/**
  * Created by John on 5/16/17.
  */

case class Abo(id: Int, v: (String, String))
case class Loc(id: Int, v: String)

object Tickets {
  val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("SimpleScalaTesting")
  val session: SparkSession = SparkSession.builder().config(conf).getOrCreate()
  import session.implicits._

  def main(args: Array[String]): Unit = {
    val as = List(Abo(101, ("Ruetli", "AG")), Abo(102, ("Brelaz", "DemiTarif")),
      Abo(103, ("Gress", "DemiTarifVisa")), Abo(104, ("Schatten", "DemiTarif")))


    val abosDF = as.toDF

    abosDF.printSchema()
    abosDF.show()
                        
    val ls = List(Loc(101, "Bern"), Loc(101, "Thun"), Loc(102, "Lausanne"), Loc(102, "Geneve"),
      Loc(102, "Nyon"), Loc(103, "Zurich"), Loc(103, "St-Gallen"), Loc(103, "Chur"))

    val locationsDF = ls.toDF

    locationsDF.printSchema()
    locationsDF.show()
    

    val trackedCustomerDF = abosDF.join(locationsDF, abosDF("id") === locationsDF("id"), "left_outer")
    trackedCustomerDF.printSchema()
    trackedCustomerDF.show()
  }

}
