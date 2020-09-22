import java.sql.Timestamp
import java.util.Calendar

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

/** Provides insert (row-based) methods
  *
  */
object InsertByRow {

  /** Creates an insert query
    *
    * @param value a list with the source kafka topic fields
    * @return an insert query
    */
  def insertParsing(value: org.apache.spark.sql.Row): String = {
    val model = value(1)
    var table:String = null
    var query:String = null
    val my14 = value(14)

    if(my14  == null) {
    val valueLength4 = getStringLength(value(4).toString)
    val valueLength5 = getStringLength(value(5).toString)
    val valueLength6 = getStringLength(value(6).toString)
    val valueLength7 = getStringLength(value(7).toString)
    val valueLength8 = getStringLength(value(8).toString)
    val valueLength9 = getStringLength(value(9).toString)
    val valueLength10 = getStringLength(value(10).toString)
    val valueLength11 = getStringLength(value(11).toString)
    val valueLength12 = getStringLength(value(12).toString)
    val valueLength13 = getStringLength(value(13).toString)



      println("******* s7 value ********************************");
      if(model.equals("s1500") | model.equals("s300")){
        table = "plc4x.s7_readings"
        query = "INSERT IGNORE INTO " + table + "  VALUES ('" + value(3) + "','" + value(0) + "',\"" +
          value(4).toString.substring(13,valueLength4) + "\",\"" + value(5).toString.substring(13,valueLength5) + "\",\"" +
          value(6).toString.substring(13,valueLength6) + "\",\"" + value(7).toString.substring(13,valueLength7) + "\",\"" +
          value(8).toString.substring(13,valueLength8) + "\",\"" + value(9).toString.substring(13,valueLength9) + "\",\"" +
          value(10).toString.substring(13,valueLength10) + "\",\"" + value(11).toString.substring(13,valueLength11) + "\");"
      }
      else{
        table = "plc4x.modbus_readings"
        query = "INSERT IGNORE INTO " + table + "  VALUES ('" + value(3) + "','" + value(0) +
          "','" + value(12).toString.substring(13,valueLength12) + "','" +
          value(13).toString.substring(13,valueLength13) + "');"
        }
    }
    else if(my14 != null){ // Alarm message topic
      table = "plc4x.alarms"
      println(" ----------/////////////////////******************* The alarm message is: " + my14.toString)
      query = "INSERT IGNORE INTO " + table + "  VALUES ('" + value(16) + "','" + value(15) + "','" + my14.toString  + "');"
    }


    query
  }

  /** Calculates the length - 1 of a String
   *
   * @param pString a String
   * @return the length of the String minus 1
   */
  def getStringLength(pString: String): Int ={
    pString.length - 1
  }


  /** Generates the timestamp with the correct format to use in MySQL
   *
   * @return the timestamp
   */
   def getTimestamp: Timestamp = {
    val calendar = Calendar.getInstance
    val ourJavaTimestampObject = new Timestamp(calendar.getTime.getTime)
    ourJavaTimestampObject
  }

}
