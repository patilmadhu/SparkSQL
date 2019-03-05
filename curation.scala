package curation

//import org.apache.spark
//import org.apache.spark.sql.hive.orc._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object curation_valid_invalid extends Serializable {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    //SparkSession
    val spark: SparkSession = SparkSession
      .builder()
      .master("yarn")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()
    val merge_df = spark.table("dbuser2.pc_account_merge")
    merge_df.show();
    import spark.implicits._

    //Write Spark program to Validate if the records have PublicId starts with “pc:” and the Service
    //Tier should be “Bronze or Gold or Platinum or Silver”

    val merge_df_filter = merge_df
        .withColumn("flag", when(lower($"ServiceTier").isin("bronze", "gold", "platinum", "silver") && ($"publicId".startsWith("pc:")),lit("valid")).otherwise("invalid"))




    //--If yes save it in valid_pc_account table, else save in invalid_pc_account

    merge_df_filter.filter($"flag" === "valid").drop("flag").write.saveAsTable("dbuser2.pc_account_valid_rd")

    merge_df_filter.filter($"flag" === "invalid").drop("flag").write.saveAsTable("dbuser2.pc_account_invalid_rd")

  }
}
