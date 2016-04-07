package aws.dynamo

import com.amazonaws.services.{dynamodbv2 ⇒ aws}

import scala.collection.JavaConversions._
import scala.collection.immutable

object Item {
  def apply(attributes: java.util.Map[String, aws.model.AttributeValue]): Item = new Item(
    attributes = attributes.toMap.mapValues(AttributeValue(_))
  )
}

/**
  * Item in a DynamoDB table
  *
  * {{{
  *   val amazonians: Seq[Item] = members.scan(Seq("Company" -> cond.eq("Amazon")))
  *   amazonians.map(_.attributes("Name").s.get) should equal(Seq("Pedro", "Robert"))
  *   //                                  ^
  *   //                                  \-string attribute: Option[String]
  * }}}
  *
  * @param attributes elements in an item are stored as a Map of attribute name to [[AttributeValue]]
  */
case class Item(attributes: immutable.Map[String, AttributeValue])

