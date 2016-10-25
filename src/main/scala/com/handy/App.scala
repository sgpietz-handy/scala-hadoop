package com.handy

import atto._, Atto._

import com.handy.schema.hive._

object App {
  def main(args: Array[String]): Unit = {
    val schema = HiveSchema.forTable("stripetest", "balance_history")
    println(schema)
  }
}
