package com.handy.pipeline.json

import io.circe.{Json, JsonObject}
import io.circe.syntax._

import cats.implicits._
import cats.data.Validated
import cats.data.Validated._

import com.handy.pipeline.error._

sealed trait SchemaType
case object LongType extends SchemaType
case object DoubleType extends SchemaType
case object StringType extends SchemaType
case object BooleanType extends SchemaType
case object NullType extends SchemaType
case class ArrayType(t: SchemaType) extends SchemaType
case class ObjectType(fields: List[(String, SchemaType)]) extends SchemaType

object SchemaType {
  private type ValidJson = Validated[List[JsonTypeError], Json]

  def fitJsonToSchema(
    json: Json,
    schema: SchemaType,
    conv: Converter = Converter.defaultConverter)
  : ValidJson = {
    lazy val err = Invalid(List(UnexpectedType(json, schema)))
    // function to attempt convert json to value specified by schema that
    // returns a validation type of JsonTypeError on failure
    def orInvalid(opt: Option[Json]): ValidJson  = 
      opt match {
        case None => err
        case Some(a) => Valid(a)
      }

    schema match {
      case NullType => Valid(None.asJson)
      case LongType => orInvalid(conv.convLong(json))
      case DoubleType => orInvalid(conv.convDouble(json))
      case StringType => orInvalid(conv.convString(json))
      case BooleanType => orInvalid(conv.convBoolean(json))
      case ArrayType(t) => json.asArray.map(arr =>
        arr.traverseU(a => fitJsonToSchema(a, t, conv))
           .map(_.asJson)).getOrElse(err)
      case s @ ObjectType(_) => json.asObject.map(obj =>
        fitJsonObjectToSchema(obj, s, conv)
      ).getOrElse(err)
    }
  }
  
  private def fitJsonObjectToSchema(
    obj: JsonObject,
    s: ObjectType,
    conv: Converter)
  : ValidJson = {
    s.fields.traverseU { case(name, t) =>
      obj(name).map(j =>
          fitJsonToSchema(j, t, conv).map(a => (name, a))
      ).getOrElse(
        if (conv.allowMissing) Valid((name, None.asJson))
        else Invalid(List(MissingField(name, obj.asJson))))
    }.map(pairs => pairs.toMap.asJson)
  }
}
