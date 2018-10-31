import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{Dataset, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

case class ASET(key: String, aval1: Int, aval2: Int, aval3: Int)
case class BSET(key: String, bval1: Int, bval2: Int, bval3: Int, bval4: Int)
case class CSET(key: String, cval1: Int, cval2: Int, cval3: Int, cval4: Int, cval5: Int)
//case class DSET(key: String, val1: Int, val2: Int, val3: Int, val4: Int, val5: Int, val6:Int)
//case class ESET(key: String, val1: Int, val2: Int, val3: Int, val4: Int, val5: Int, val6:Int, val7: Int)
case class ABC(key: String, aval1: Int, aval2: Int, aval3: Int, bval1: Int, bval2: Int, bval3: Int, bval4: Int, cval1: Int, cval2: Int, cval3: Int, cval4: Int, cval5: Int)

object SparkSqlJoin{

  def mapSideJoin(spark:SQLContext, ds1: Dataset[ASET], ds2: Dataset[BSET], ds3: Dataset[CSET]) = {
    import spark.implicits._
    val uds: Dataset[ABC] = ds1.mapPartitions(partition =>{
      partition.map(iter =>{
        ABC(iter.key, iter.aval1, iter.aval2, iter.aval3, 0, 0, 0, 0, 0, 0, 0, 0, 0)    
      })
    })

    val uds1: Dataset[ABC] = ds2.mapPartitions(partition => {
      partition.map(iter =>{
        ABC(iter.key,  0, 0, 0, iter.bval1, iter.bval2, iter.bval3, iter.bval4, 0, 0, 0, 0, 0)    
      })
    })

    val uds2: Dataset[ABC] = ds3.mapPartitions(partition => {
      partition.map(iter =>{
        ABC(iter.key, 0, 0, 0, 0, 0, 0, 0, iter.cval1, iter.cval2, iter.cval3, iter.cval4, iter.cval5)
      })
    })

    uds.union(uds1).union(uds2).repartition($"key").mapPartitions(partition => {
      val par = partition.toSeq.groupBy(_.key).mapValues(_.toSeq).toIterator
      par.map{case((key, vals)) => 
        val (aval1,aval2,aval3,bval1,bval2,bval3,bval4,cval1,cval2,cval3,cval4,cval5) = 
          vals.foldLeft((0,0,0,0,0,0,0,0,0,0,0,0))(
            (acc,x) => (
              acc._1 + x.aval1,
              acc._2 + x.aval2,
              acc._3 + x.aval3,
              acc._4 + x.bval1,
              acc._5 + x.bval2,
              acc._6 + x.bval3,
              acc._7 + x.bval4,
              acc._8 + x.cval1,
              acc._9 + x.cval2,
              acc._10 + x.cval3,
              acc._11 + x.cval4,
              acc._12 + x.cval5
            ))
        ABC(key,aval1,aval2,aval3,bval1,bval2,bval3,bval4,cval1,cval2,cval3,cval4,cval5)
      }
    }).coalesce(1)
 }
  

 def main(args: Array[String]){
  val conf = new SparkConf().setAppName("examine custom join")
  val sc: SparkContext = new SparkContext(conf)
  val spark: SQLContext = new SQLContext(sc)

  import spark.implicits._
  val inBasePath = args(0)
  val outBasePath = args(1)

  val in1 = inBasePath + "/" + "Aa.csv"
  val in2 = inBasePath + "/" + "Bb.csv"
  val in3 = inBasePath + "/" + "Cc.csv"

  val out1 = outBasePath + "/join1/"
  val out2 = outBasePath + "/join2/"

  val dsA = spark.read.textFile(in1).map(str =>{
    val cols = str.split(",")
    val key = cols(0)
    val val1 = cols(1).toInt
    val val2 = cols(2).toInt
    val val3 = cols(3).toInt
    ASET(key,val1,val2,val3)
  }).persist()
  
  val dsB = spark.read.textFile(in2).map(str =>{
    val cols = str.split(",")
    val key = cols(0)
    val val1 = cols(1).toInt
    val val2 = cols(2).toInt
    val val3 = cols(3).toInt
    val val4 = cols(4).toInt
    BSET(key,val1,val2,val3,val4)
  }).persist()
  
  
  val dsC = spark.read.textFile(in3).map(str =>{
    val cols = str.split(",")
    val key = cols(0)
    val val1 = cols(1).toInt
    val val2 = cols(2).toInt
    val val3 = cols(3).toInt
    val val4 = cols(4).toInt
    val val5 = cols(5).toInt
    CSET(key,val1,val2,val3,val4,val5)
  }).persist()
  
  //sparksql join
  dsA.join(dsB,Seq("key"),"left_outer").join(dsC,Seq("key"), "left_outer").write.csv(out1)

  //cutom join
  mapSideJoin(spark, dsA, dsB, dsC).write.csv(out2)
 }
}



