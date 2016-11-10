package com.handy.schema

trait ConvertsTo[A] {
  def fromByte(a: Byte): A
  def fromShort(a: Short) : A
  def fromInt(a: Int): A
  def fromLong(a: Long): A
  def fromFloat(a: Float): A
  def fromDouble(a: Double): A
  def fromDecimal(a: BigDecimal): A
  def fromString(a: String): A
  def fromBinary(a: Array[Byte]): A
  def fromBoolean(a: Boolean): A
  def fromNull: A

  def fromArray(a: List[A]): A
  def fromMap(a: Map[A, A]): A
  def fromStruct(a: Map[String, A]): A
}
