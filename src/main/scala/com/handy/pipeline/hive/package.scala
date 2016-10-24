package com.handy.pipeline

import scalaz._, Scalaz.{char => _, _}

import atto._, Atto._

import java.sql.SQLException
import java.sql.Connection
import java.sql.ResultSet
import java.sql.Statement
import java.sql.DriverManager

import org.apache.avro.Schema

import com.handy.pipeline.json._
import com.handy.pipeline.error.SchemaParseError

package object hive {
  private val schemaParser: Parser[SchemaType] = {
      string("string")                                          >| StringType  |
      string("binary")                                          >| StringType  |
      string("int ")                                            >| LongType    |
      string("tinyint")                                         >| LongType    |
      string("smallint")                                        >| LongType    |
      string("bigint")                                          >| LongType    |
      string("boolean")                                         >| BooleanType |
      string("float")                                           >| DoubleType  |
      string("double")                                          >| DoubleType  |
      string("array") ~> angles(schemaParser)                   -| ArrayType   |
      string("struct") ~> angles(sepBy(schemaField, char(','))) -| ObjectType
    }
    
  private def angles[A](p: Parser[A]) = bracket(char('<'), p, char('>'))
  private def untilChar(c: Char) = takeWhile(_ != c)
  private val schemaField: Parser[(String, SchemaType)] =
      pairBy(untilChar(':'), char(':'), schemaParser)

  def hiveSchemaType(schema: String): Either[SchemaParseError, SchemaType] =
    schemaParser.parseOnly(schema)
                .either
                .fold(msg => Left(SchemaParseError(msg)), st => Right(st))

  def schemaForTable(db: String, tableName: String): String = {
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
