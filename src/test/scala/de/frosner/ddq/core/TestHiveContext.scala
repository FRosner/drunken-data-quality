package de.frosner.ddq.core

import org.apache.spark.SparkContext
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.hive.HiveContext

import scala.reflect.io.Path

case class TestHiveContext(sc: SparkContext) extends HiveContext(sc) {

  lazy val warehousePath = getTempFilePath("sparkHiveWarehouse").getCanonicalPath
  lazy val metastorePath = getTempFilePath("sparkHiveMetastore").getCanonicalPath

  setConf("javax.jdo.option.ConnectionURL",
    s"jdbc:derby:;databaseName=${metastorePath};create=true")
  setConf("hive.metastore.warehouse.dir", warehousePath)
  sql("CREATE DATABASE default")

  def deletePaths() = {
    Path(warehousePath).deleteRecursively()
    Path(metastorePath).deleteRecursively()
    Path("metastore_db").deleteRecursively()
  }

}
