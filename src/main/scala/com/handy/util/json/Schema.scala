package com.handy.util.json

import io.circe.{Json, JsonObject}
import io.circe.syntax._

import cats.implicits._
import cats.data.Validated
import cats.data.Validated._

import com.handy.util.json.Converter

trait Schema[A] {
  def toSchemaType(a: A): SchemaType
} 

object Schema {
  private type ValidJson = Validated[List[JsonTypeError], Json]

  private def jsonWithSchema[A](json: Json, schema: SchemaType, conv: Converter)
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
      case SNull => Valid(None.asJson)
      case SLong => orInvalid(conv.convLong(json))
      case SDouble => orInvalid(conv.convDouble(json))
      case SString => orInvalid(conv.convString(json))
      case SBoolean => orInvalid(conv.convBoolean(json))
      case SArray(t) => json.asArray.map(arr =>
        arr.traverseU(a => jsonWithSchema(a, t, conv)).map(_.asJson)
      ).getOrElse(err)
      case s @ SObject(_) => json.asObject.map(obj =>
        jsonObjectWithSchema(obj, s, conv)
      ).getOrElse(err)
    } 
  }

  private def jsonObjectWithSchema(obj: JsonObject, s: SObject, conv: Converter)
  : ValidJson = {
    s.fields.traverseU { case(name, t) =>
      obj(name).map(j =>
          jsonWithSchema(j, t, conv).map(a => (name, a))
      ).getOrElse(Invalid(List(MissingField(name, obj.asJson))))
    }.map(pairs => pairs.toMap.asJson)
  }

  def convertJson[A](a: A, json: Json, conv: Converter)
  (implicit ev: Schema[A]): ValidJson = {
    jsonWithSchema(json, ev.toSchemaType(a), conv)
  }
}
