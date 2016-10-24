package com.handy.util.hive

import java.sql.SQLException
import java.sql.Connection
import java.sql.ResultSet
import java.sql.Statement
import java.sql.DriverManager

import org.apache.avro.Schema

import scalaz._, Scalaz.{char => _, _}

import atto._, Atto._

object SchemaParser {
  trait HiveType
  case object HString                                   extends HiveType
  case object HInt                                      extends HiveType
  case object HTinyInt                                  extends HiveType
  case object HSmallInt                                 extends HiveType
  case object HBigInt                                   extends HiveType
  case object HBoolean                                  extends HiveType
  case object HFloat                                    extends HiveType
  case object HDouble                                   extends HiveType
  case object HBinary                                   extends HiveType
  case class  HArray(typ: HiveType)                     extends HiveType
  case class  HStruct(fields: List[(String, HiveType)]) extends HiveType

  val hiveType: Parser[HiveType] = {
    string("string")                                        >| HString   |
    string("int ")                                          >| HInt      |
    string("tinyint")                                       >| HTinyInt  |
    string("smallint")                                      >| HSmallInt |
    string("bigint")                                        >| HBigInt   |
    string("boolean")                                       >| HBoolean  |
    string("float")                                         >| HFloat    |
    string("binary")                                        >| HBinary   |
    string("double")                                        >| HDouble   |
    string("array") ~> angles(hiveType)                     -| HArray    |
    string("struct") ~> angles(sepBy(hiveField, char(','))) -| HStruct
  }
  
  val hiveField: Parser[(String, HiveType)] =
    pairBy(untilChar(':'), char(':'), hiveType)

  def angles[A](p: Parser[A]) = bracket(char('<'), p, char('>'))
  def untilChar(c: Char) = takeWhile(_ != c)

 
  def tableSchema(db: String, tableName: String): String = {
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
}
