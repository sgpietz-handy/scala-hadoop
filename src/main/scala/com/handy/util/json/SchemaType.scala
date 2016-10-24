package com.handy.util.json

sealed trait SchemaType
case object SLong extends SchemaType
case object SDouble extends SchemaType
case object SString extends SchemaType
case object SBoolean extends SchemaType
case object SNull extends SchemaType
case class SArray(t: SchemaType) extends SchemaType
case class SObject(fields: List[(String, SchemaType)]) extends SchemaType

