package com.handy

import atto._, Atto._

import com.handy.pipeline.hive._
import com.handy.pipeline.hive._

object App {
  def main(args: Array[String]): Unit = {
    val schema = schemaForTable("stripetest", "balance_history")
    println(schema)
    val st = hiveSchemaType(schema)
    println(st)
  }
}
