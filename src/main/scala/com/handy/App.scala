package com.handy

import atto._, Atto._

import com.handy.util.hive.SchemaParser._

object App {
  def main(args: Array[String]): Unit = {
    val schema = tableSchema("stripetest", "balance_history")
    println(schema)
    val ht = hiveType.parse(schema)
    println(ht)
  }
}
