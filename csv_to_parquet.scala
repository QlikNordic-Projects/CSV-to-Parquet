import org.apache.spark.sql.types.{StructType,StructField,StringType,IntegerType,DecimalType,DoubleType,BooleanType,DateType,LongType};
import org.apache.spark.sql.SQLContext;
import java.io.File;
import scala.io.StdIn;


//val selected_csv_folder = sys.env("")
//val srcDir = "/home/" + selected_csv_folder
val srcDir = "/home/" // Edit here, Apply Source directory
val dstDir = "/home/" // Edit here, Apply Target directory

def convert(sqlContext: SQLContext, filename: String, schema: StructType, tableName: String, outFileName: String) {
  var df = sqlContext.read
    .format("com.databricks.spark.csv")
    .schema(schema)
    .option("delimiter",",")
    .option("nullValue", "")
    .option("treatEmptyValueAsNulls","true")
    .load(filename)

  df = df.repartition(1)

  df.write.parquet(dstDir + "/" + tableName + ".table" + "/" + outFileName)
}

def getListOfFiles(dir: String, extensions: List[String]): List[File] = {
  val d = new File(dir)
  if (d.exists && d.isDirectory) {
    d.listFiles.filter(_.isFile).toList.filter { file =>
      extensions.exists(file.getName.endsWith(_))
    }
  } else {
    List[File]()
  }
}

//List All tables

val schematable= StructType(Array(
  //List All field name and field type in the current table, example below
  StructField("tablefield1", IntegerType), 
  StructField("tablefield2", DateType),
  StructField("tablefield3", DoubleType)

val sqlContext = new org.apache.spark.sql.SQLContext(sc)

val fileExtensions = List("csv")
val files = getListOfFiles(srcDir, fileExtensions)

for (file <- files) convertFile(file.getName())

var nbrOfFilesDone = 0

def convertFile(filename: String) {
  nbrOfFilesDone += 1
  println("\nProgress: " + nbrOfFilesDone + " / " + files.length)
  println("Convert " + srcDir + "/" + filename)

  if (filename contains "tablename") { // Change Here
    convert(sqlContext, srcDir + "/" + filename, schemaTablename, "tablename", "tablename.parquet")
  } 
}
