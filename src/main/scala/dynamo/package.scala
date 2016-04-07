package aws

import com.amazonaws.services.{ dynamodbv2 ⇒ jdynamo }

package object dynamo {

  type TableStatus = jdynamo.model.TableStatus
  type KeyType = jdynamo.model.KeyType
  type AttributeAction = jdynamo.model.AttributeAction
  type ProjectionType = jdynamo.model.ProjectionType
  type ReturnConsumedCapacity = jdynamo.model.ReturnConsumedCapacity

  type ComparisonOperator = jdynamo.model.ComparisonOperator
  type Select = jdynamo.model.Select

  val cond = aws.dynamo.DynamoDBCondition
}

