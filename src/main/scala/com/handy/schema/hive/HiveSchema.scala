package com.handy.schema.hive

import java.sql.SQLException
import java.sql.Connection
import java.sql.ResultSet
import java.sql.Statement
import java.sql.DriverManager

import atto._, Atto._

import com.handy.schema._
import com.handy.schema.hive.SchemaParser._

case class HiveSchema(schema: String) extends AnyVal

object HiveSchema {
  def forTable(db: String, tableName: String) = HiveSchema {
    def resSetToList(rs: ResultSet): List[(String, String)] = {
      import scala.collection.mutable.ListBuffer
      val mutList: ListBuffer[(String, String)] = new ListBuffer()
      while (rs.next()) {
        try {
          val (name, typ) = (rs.getString(1), rs.getString(2))
          if (!name.isEmpty && !typ.isEmpty && !(name + typ).contains(" ")) {
            println(name); println(typ)
            mutList.append((name, typ))
          }
        } catch {
          case npe: NullPointerException => ()
        }
      }
      mutList.toList
    }
    val conn = DriverManager.getConnection(
      s"jdbc:hive2://r-hadoopeco-hive-0d215a95.hbinternal.com:10000/$db",
      "",
      ""
    )
    val stmt = conn.createStatement
    println("executing sql")
    val rs = stmt.executeQuery(s"describe $tableName")
    println("done")
    val schema = 
      "struct<" + resSetToList(rs).map { case (name, typ) =>
        name + ":" + typ
      }.mkString(",") + ">"
    conn.close()
    schema
  }
  
  implicit val hiveToSchema =
    new ToSchema[HiveSchema] {
      def apply(schema: HiveSchema): Either[SchemaParseError, SchemaType] =
        schemaParser.parseOnly(schema.schema)
          .either
          .fold(msg => Left(SchemaParseError(msg)), st => Right(st))
    }
}
