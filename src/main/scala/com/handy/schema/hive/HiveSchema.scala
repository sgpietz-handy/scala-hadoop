package com.handy.schema.hive

import java.sql.SQLException
import java.sql.Connection
import java.sql.ResultSet
import java.sql.Statement
import java.sql.DriverManager

import atto._, Atto._, compat.stdlib._

import com.handy.schema._

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

  val schemaParser: Parser[SchemaType] = {
      // TODO: add date types!
      string("string")                    >| (StringType: SchemaType)  |
      string("char")                      >| (StringType: SchemaType)  |
      string("varchar")                   >| (StringType: SchemaType)  |
      string("binary")                    >| (BinaryType: SchemaType)  |
      string("tinyint")                   >| (ByteType: SchemaType)    |
      string("smallint")                  >| (ShortType: SchemaType)   |
      string("int")                       >| (IntType: SchemaType)     |
      string("bigint")                    >| (LongType: SchemaType)    |
      string("boolean")                   >| (BooleanType: SchemaType) |
      string("float")                     >| (FloatType: SchemaType)   |
      string("double")                    >| (DoubleType: SchemaType)  |
      string("map")       ~> mapParser    -| toMapType                 |
      string("array")     ~> arrayParser  -| toArrayType               |
      string("uniontype") ~> unionParser  -| toUnionType               |
      string("struct")    ~> structParser -| ( f => toStructType(f))
    }

  private def toArrayType(t: SchemaType): SchemaType = ArrayType(t)
  private def toUnionType(t: List[SchemaType]): SchemaType = UnionType(t)
  // TODO: allow for non-nullable fields
  private def toStructType(t: List[(String, SchemaType)]): SchemaType =
    StructType {
      t.map(kv => SchemaField(kv._1, kv._2, true))
    }
  private def toMapType(kv: (SchemaType, SchemaType)): SchemaType =
    kv match {
      case (k: PrimitiveType, v: SchemaType) => MapType(k, v)
      case _ => throw new Exception("invalid map type parameters")
    }

  private def angles[A](p: Parser[A]) = bracket(char('<'), p, char('>'))
  private def untilChar(c: Char) = takeWhile(_ != c)
  private val schemaField: Parser[(String, SchemaType)] =
      pairBy(untilChar(':'), char(':'), schemaParser)

  private val arrayParser = angles(schemaParser)
  private val mapParser = angles(pairBy(schemaParser, char(','), schemaParser))
  private val unionParser = angles(sepBy(schemaParser, char(',')))
  private val structParser = angles(sepBy(schemaField, char(',')))
}
