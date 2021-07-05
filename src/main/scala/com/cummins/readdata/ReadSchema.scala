package com.cummins.readdata

import java.util.Properties

import com.cummins.utils.SuperJob
import org.apache.spark.sql.DataFrame

/**
 * 2021/3/9
 * create by rr398
 * (对Job的功能描述) 
 * READ:[table names]
 * SAVE:[table names]
 */
object ReadSchema extends SuperJob {
  private val properties = new Properties
  properties.load(this.getClass.getClassLoader.getResourceAsStream("webdb.properties"))

  override def run(): Unit = {
    Seq(
      "[IHub].[IHUB_ODS_MD_CUST_CATEGORY1]",
      "[IHub].[IHUB_ODS_MD_CUST_CATEGORY2]",
      "[IHub].[IHUB_ODS_MD_FIN_RESP_CODE_XREF]",
      "[IHub].[IHUB_ODS_MD_GL_PERIODS]",
      "[IHub].[IHUB_ODS_MD_PART_DESCRIPTION]",
      "[IHub].[IHUB_ODS_MD_PART_DFT_SUPP]",
      "[IHub].[IHUB_ODS_MD_SUPPLIER_PARTS]",
      "[IHub].[IHUB_ODS_V_GPL_BORECOVERY_PDC]",
      "[IHub].[IHUB_ODS_V_GPL_BORECOVERY_RDC]",
      "[IHub].[IHUB_ODS_V_INVOICE_ALL]",
      "[IHub].[IHUB_ODS_V_ORDER_ALL]",
      "[IHub].[IHUB_ODS_V_PO_ALL]",
      "[IHub].[IHUB_ODS_V_SHIP_LIST_ALL]",
      "[IHub].[IHUB_ODS_V_WO_ALL]"
    ).foreach((tableName: String) =>{

      val tableDf: DataFrame = spark.read.jdbc(properties.getProperty("url"),tableName,properties)
      println(tableName)
      tableDf.printSchema()

    })


  }
}
