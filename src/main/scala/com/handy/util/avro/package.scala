package com.handy.util

import org.apache.avro.{Schema => AvroSchema}

import scala.collection.JavaConverters._

import com.handy.util.json._

package object avro {
  implicit val avroSchemaInstance = new Schema[AvroSchema] {
    import org.apache.avro.Schema.Type._
    def toSchemaType(s: AvroSchema): SchemaType =
      s.getType() match {
        case STRING => SString
        case BOOLEAN => SBoolean
        case LONG => SLong
        case DOUBLE => SDouble
        case ARRAY => SArray(toSchemaType(s.getElementType()))
        case RECORD => SObject(
          s.getFields().asScala.map(
            f => (f.name(), toSchemaType(f.schema()))).toList
        )
      }
  }
}

