package aws.dynamo

import java.util

import aws._
import com.amazonaws.AmazonClientException
import collection.immutable.VectorBuilder
import collection.immutable.IndexedSeq
import com.amazonaws.services.dynamodbv2.model
import scala.collection.JavaConverters._
import com.amazonaws.services.{dynamodbv2 => aws}
import com.amazonaws.services.dynamodbv2.model._
import scalaz.{-\/, \/-, \/}

/**
  * Companion object to instance DynamoDB clients.
  * Example: connect to default region:
  * {{{
  *   val client = DynamoDB()
  * }}}
  */
object DynamoDB {
  def apply(credentials: Credentials)(implicit region: Region): DynamoDB = new DynamoDBClient(BasicCredentialsProvider(credentials.getAWSAccessKeyId, credentials.getAWSSecretKey)).at(region)

  def apply(credentialsProvider: CredentialsProvider = CredentialsLoader.load())(implicit region: Region = Region.default()): DynamoDB = new DynamoDBClient(credentialsProvider).at(region)

  def apply(accessKeyId: String, secretAccessKey: String)(implicit region: Region): DynamoDB = new DynamoDBClient(BasicCredentialsProvider(accessKeyId, secretAccessKey)).at(region)

  def at(region: Region): DynamoDB = apply()(region)

  /**
    * @return a DynamoDB client that connects to localhost, useful for testing with DynamoDB local
    */
  def local(): DynamoDB = {
    val client = DynamoDB("", "")(Region.default())
    client.setEndpoint("http://localhost:8000")
    client
  }
}

/**
  * Amazon DynamoDB Java client wrapper
  *
  * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/dynamodbv2/AmazonDynamoDBClient.html]]
  */
trait DynamoDB extends aws.AmazonDynamoDB {

  def at(region: Region): DynamoDB = {
    this.setRegion(region)
    this
  }

  private[this] var consistentRead = false

  def consistentRead(consistentRead: Boolean): DynamoDB = {
    this.consistentRead = consistentRead
    this
  }

  def tableNames: Seq[String] = listTables.getTableNames.asScala

  def lastEvaluatedTableName: Option[String] = Option(listTables.getLastEvaluatedTableName)

  def describe(table: Table): AmazonClientException \/ TableMeta = describe(table.name)

  def describe(tableName: String): AmazonClientException \/ TableMeta = \/.fromTryCatchThrowable[TableMeta, AmazonClientException] {
    TableMeta(describeTable(new aws.model.DescribeTableRequest().withTableName(tableName)).getTable)
  }

  def table(name: String): AmazonClientException \/ Table = describe(name).map(_.table)

  def createTable(
    name: String,
    hashPK: (String, aws.model.ScalarAttributeType)
  ): TableMeta = {
    create(Table(
      name = name,
      hashPK = hashPK._1,
      rangePK = None,
      attributes = Seq(AttributeDefinition(hashPK._1, hashPK._2))
    ))
  }

  def createTable(
    name: String,
    hashPK: (String, aws.model.ScalarAttributeType),
    otherAttributes: Seq[(String, aws.model.ScalarAttributeType)]
  ): TableMeta = {
    create(Table(
      name = name,
      hashPK = hashPK._1,
      rangePK = None,
      attributes = Seq(
        AttributeDefinition(hashPK._1, hashPK._2)
      ) ++: otherAttributes.map(a => AttributeDefinition(a._1, a._2))
    ))
  }


  def createTable(
    name: String,
    hashPK: (String, aws.model.ScalarAttributeType),
    rangePK: (String, aws.model.ScalarAttributeType),
    otherAttributes: Seq[(String, aws.model.ScalarAttributeType)] = Nil,
    localSecondaryIndexes: Seq[LocalSecondaryIndex] = Nil,
    globalSecondaryIndexes: Seq[GlobalSecondaryIndex] = Nil,
    provisionedThroughput: Option[ProvisionedThroughput] = None
  ): TableMeta = {
    create(Table(
      name = name,
      hashPK = hashPK._1,
      rangePK = Some(rangePK._1),
      attributes = Seq(
        AttributeDefinition(hashPK._1, hashPK._2),
        AttributeDefinition(rangePK._1, rangePK._2)
      ) ++: otherAttributes.map(a => AttributeDefinition(a._1, a._2)),
      localSecondaryIndexes = localSecondaryIndexes,
      globalSecondaryIndexes = globalSecondaryIndexes,
      provisionedThroughput = provisionedThroughput
    ))
  }

  def create(table: Table): TableMeta = createTable(table)

  def createTable(table: Table): TableMeta = {
    val keySchema: Seq[aws.model.KeySchemaElement] = Seq(
      Some(KeySchema(table.hashPK, aws.model.KeyType.HASH)),
      table.rangePK.map(n => KeySchema(n, aws.model.KeyType.RANGE))
    ).flatten.map(_.asInstanceOf[aws.model.KeySchemaElement])

    val req = new aws.model.CreateTableRequest()
      .withTableName(table.name)
      .withAttributeDefinitions(table.attributes.map(_.asInstanceOf[aws.model.AttributeDefinition]).asJava)
      .withKeySchema(keySchema.asJava)
      .withProvisionedThroughput(
        table.provisionedThroughput.map(_.asInstanceOf[aws.model.ProvisionedThroughput]).getOrElse {
          ProvisionedThroughput(readCapacityUnits = 10, writeCapacityUnits = 10)
        }
      )

    if (!table.localSecondaryIndexes.isEmpty) {
      req.setLocalSecondaryIndexes(table.localSecondaryIndexes.map(_.asInstanceOf[aws.model.LocalSecondaryIndex]).asJava)
    }
    if (!table.globalSecondaryIndexes.isEmpty) {
      req.setGlobalSecondaryIndexes(table.globalSecondaryIndexes.map(_.asInstanceOf[aws.model.GlobalSecondaryIndex]).asJava)
    }

    TableMeta(createTable(req).getTableDescription)
  }

  def updateTableProvisionedThroughput(table: Table, provisionedThroughput: ProvisionedThroughput): TableMeta = {
    TableMeta(updateTable(
      new aws.model.UpdateTableRequest(table.name, provisionedThroughput)
    ).getTableDescription)
  }

  def delete(table: Table): Unit = deleteTable(table)

  def deleteTable(table: Table): Unit = deleteTable(table.name)

  // ------------------------------------------
  // Items
  // ------------------------------------------

  def get(table: Table, hashPK: Any): AmazonClientException \/ Item = getItem(table, hashPK)

  def getItem(table: Table, hashPK: Any): AmazonClientException \/ Item = getItem(table.name, (table.hashPK, hashPK))

  def getItem(tableName: String, hashPK: (String, Any)): AmazonClientException \/ Item = {
    \/.fromTryCatchThrowable[Item, AmazonClientException] {
      val res = getItem(new aws.model.GetItemRequest()
        .withTableName(tableName)
        .withKey(Map(hashPK._1 -> AttributeValue.toJavaValue(hashPK._2)).asJava)
        .withConsistentRead(consistentRead))
      if (res.getItem == null)
        throw new ResourceNotFoundException(s"$tableName ${hashPK._1}")
      Item(res.getItem)
    }
  }

  def getItem(tableName: String, hashPK: (String, Any), rangePK: (String, Any)): AmazonClientException \/ Item = {
    \/.fromTryCatchThrowable[Item, AmazonClientException] {
      val res = getItem(new aws.model.GetItemRequest()
        .withTableName(tableName)
        .withKey(Map(
          hashPK._1 -> AttributeValue.toJavaValue(hashPK._2),
          rangePK._1 → AttributeValue.toJavaValue(rangePK._2)
        ).asJava)
        .withConsistentRead(consistentRead))

      if (res.getItem == null)
        throw new ResourceNotFoundException(s"$tableName ${hashPK._1}")
      Item(res.getItem)
    }
  }

  def get(table: Table, hashPK: Any, rangePK: Any): AmazonClientException \/ Item = getItem(table, hashPK, rangePK)

  def getItem(table: Table, hashPK: Any, rangePK: Any): AmazonClientException \/ Item = {
    rangePK match {
      case None =>
        getItem(table, hashPK)

      case _ =>
        \/.fromTryCatchThrowable[Item, AmazonClientException] {
          val res = getItem(new aws.model.GetItemRequest()
            .withTableName(table.name)
            .withKey(Map(
              table.hashPK -> AttributeValue.toJavaValue(hashPK),
              table.rangePK.get -> AttributeValue.toJavaValue(rangePK)
            ).asJava)
            .withConsistentRead(consistentRead))

          if (res.getItem == null)
            throw new ResourceNotFoundException(s"${table.name} ${table.hashPK}")
          Item(res.getItem)
        }
    }
  }

  def attributeValues(attributes: Seq[(String, Any)]): java.util.Map[String, aws.model.AttributeValue] =
    attributes.toMap.mapValues(AttributeValue.toJavaValue(_)).asJava

  def put(table: Table, hashPK: Any, attributes: (String, Any)*): AmazonClientException \/ PutItemResult =
    putItem(table, hashPK, attributes: _*)

  def putItem(table: Table, hashPK: Any, attributes: (String, Any)*): AmazonClientException \/ PutItemResult =
    put(table, Seq(table.hashPK -> hashPK) ++: attributes: _*)

  def put(table: Table, hashPK: Any, rangePK: Any, attributes: (String, Any)*): AmazonClientException \/ PutItemResult =
    putItem(table, hashPK, rangePK, attributes: _*)

  def putItem(table: Table, hashPK: Any, rangePK: Any, attributes: (String, Any)*): AmazonClientException \/ PutItemResult =
    put(table, Seq(table.hashPK -> hashPK, table.rangePK.get -> rangePK) ++: attributes: _*)

  def put(table: Table, attributes: (String, Any)*): AmazonClientException \/ PutItemResult =
    putItem(table.name, attributes: _*)

  def putItem(tableName: String, attributes: (String, Any)*): AmazonClientException \/ PutItemResult =
    \/.fromTryCatchThrowable[PutItemResult, AmazonClientException] {

      putItem(new aws.model.PutItemRequest()
        .withTableName(tableName)
        .withItem(attributeValues(attributes)))
    }

  def putConditional(
    tableName: String,
    attributes: (String, Any)*)
    (cond: Seq[(String, aws.model.ExpectedAttributeValue)]): AmazonClientException \/ PutItemResult =
    \/.fromTryCatchThrowable[PutItemResult, AmazonClientException] {

      putItem(new aws.model.PutItemRequest()
        .withTableName(tableName)
        .withItem(attributeValues(attributes))
        .withExpected(cond.toMap.asJava))
    }

  def addAttributes(table: Table, hashPK: Any, attributes: (String, Any)*): AmazonClientException \/ Unit =
    \/.fromTryCatchThrowable[Unit, AmazonClientException] {
      updateAttributes(table, hashPK, None, aws.model.AttributeAction.ADD, attributes)
    }

  def addAttributes(table: Table, hashPK: Any, rangePK: Any, attributes: (String, Any)*) =
    updateAttributes(table, hashPK, Some(rangePK), aws.model.AttributeAction.ADD, attributes)

  def deleteAttributes(table: Table, hashPK: Any, attributes: (String, Any)*) =
    updateAttributes(table, hashPK, None, aws.model.AttributeAction.DELETE, attributes)

  def deleteAttributes(table: Table, hashPK: Any, rangePK: Any, attributes: (String, Any)*) =
    updateAttributes(table, hashPK, Some(rangePK), aws.model.AttributeAction.DELETE, attributes)

  def putAttributes(table: Table, hashPK: Any, attributes: (String, Any)*) =
    updateAttributes(table, hashPK, None, aws.model.AttributeAction.PUT, attributes)

  def putAttributes(table: Table, hashPK: Any, rangePK: Any, attributes: (String, Any)*) =
    updateAttributes(table, hashPK, Some(rangePK), aws.model.AttributeAction.PUT, attributes)

  private[dynamo] def updateAttributes(
    table: Table, hashPK: Any, rangePK: Option[Any], action: AttributeAction, attributes: Seq[(String, Any)]
  ): AmazonClientException \/ UpdateItemResult = \/.fromTryCatchThrowable[UpdateItemResult, AmazonClientException] {

    val tableKeys = Map(table.hashPK -> AttributeValue.toJavaValue(hashPK)) ++ rangePK.flatMap(rKey => table.rangePK.map(_ -> AttributeValue.toJavaValue(rKey)))

    updateItem(new aws.model.UpdateItemRequest()
      .withTableName(table.name)
      .withKey(tableKeys.asJava)
      .withAttributeUpdates(attributes.map {
        case (key, value) =>
          (key, new aws.model.AttributeValueUpdate().withAction(action).withValue(AttributeValue.toJavaValue(value)))
      }.toMap.asJava))
  }

  def deleteItem(table: Table, hashPK: Any): AmazonClientException \/ DeleteItemResult =
    \/.fromTryCatchThrowable[DeleteItemResult, AmazonClientException] {

      deleteItem(new aws.model.DeleteItemRequest()
        .withTableName(table.name)
        .withKey(Map(table.hashPK -> AttributeValue.toJavaValue(hashPK)).asJava))
    }

  def deleteItem(table: Table, hashPK: Any, rangePK: Any): AmazonClientException \/ DeleteItemResult =
    \/.fromTryCatchThrowable[DeleteItemResult, AmazonClientException] {

      deleteItem(new aws.model.DeleteItemRequest()
        .withTableName(table.name)
        .withKey(Map(
          table.hashPK -> AttributeValue.toJavaValue(hashPK),
          table.rangePK.get -> AttributeValue.toJavaValue(rangePK)
        ).asJava))
    }

  def deleteItem(tableName: String, hashPK: (String, Any)): AmazonClientException \/ DeleteItemResult =
    \/.fromTryCatchThrowable[DeleteItemResult, AmazonClientException] {

      deleteItem(new aws.model.DeleteItemRequest()
        .withTableName(tableName)
        .withKey(Map(
          hashPK._1 -> AttributeValue.toJavaValue(hashPK._2)
        ).asJava))
    }

  def deleteItem(tableName: String, hashPK: (String, Any), rangePK: (String, Any)): AmazonClientException \/ DeleteItemResult =
    \/.fromTryCatchThrowable[DeleteItemResult, AmazonClientException] {

      deleteItem(new aws.model.DeleteItemRequest()
        .withTableName(tableName)
        .withKey(Map(
          hashPK._1 -> AttributeValue.toJavaValue(hashPK._2),
          rangePK._1 -> AttributeValue.toJavaValue(rangePK._2)
        ).asJava))
    }

  def delete(tableName: String, keyConditions: Seq[(String, aws.model.Condition)]): AmazonClientException \/ Unit = {
    // FIXME this only works for string attributes
    describe(tableName).map { table ⇒
      val attributes = table.keySchema.map {
        _.attributeName
      }
      val hashPK = table.keySchema.find {
        _.keyType == KeyType.Hash
      }.get.attributeName
      val maybeRangePK = table.keySchema.find {
        _.keyType == KeyType.Range
      }.map {
        _.attributeName
      }
      val items = query(tableName, keyConditions, attributes, true, true)
      if (maybeRangePK.nonEmpty) {
        val rangePK = maybeRangePK.get
        items.foreach { i ⇒
          // FIXME: here the attribute is assumed to be string
          val hashPKV = i.attributes(hashPK).s.get
          val rangePKV = i.attributes(rangePK).s.get
          deleteItem(new DeleteItemRequest()
            .withTableName(tableName)
            .withKey(Map(
              hashPK → AttributeValue.toJavaValue(hashPKV),
              rangePK → AttributeValue.toJavaValue(rangePKV)
            ).asJava))
        }
      } else {
        items.foreach { i ⇒
          val hashPKV = i.attributes(hashPK).s.get
          deleteItem(new DeleteItemRequest()
            .withTableName(tableName)
            .withKey(Map(
              hashPK → AttributeValue.toJavaValue(hashPKV)
            ).asJava))
        }
      }
    }
  }

  def queryWithIndex(
    tableName: String,
    index: SecondaryIndex,
    keyConditions: Seq[(String, aws.model.Condition)],
    select: Select = aws.model.Select.ALL_ATTRIBUTES,
    attributesToGet: Seq[String] = Nil,
    scanIndexForward: Boolean = true,
    consistentRead: Boolean = false
  ): IndexedSeq[Item] = try {
    val req = new aws.model.QueryRequest()
      .withTableName(tableName)
      .withIndexName(index.name)
      .withKeyConditions(keyConditions.toMap.asJava)
      .withSelect(select)
      .withScanIndexForward(scanIndexForward)
      .withConsistentRead(consistentRead)
    if (!attributesToGet.isEmpty) {
      req.setAttributesToGet(attributesToGet.asJava)
    }

    val allItems = new VectorBuilder[Item]()
    // Collect paginated results
    var lastEvaluatedKey: util.Map[String, model.AttributeValue] = null
    do {
      val queryResult = query(req)
      lastEvaluatedKey = queryResult.getLastEvaluatedKey
      val items = queryResult.getItems.asScala.map(i => Item(i))
      allItems ++= items
    } while (lastEvaluatedKey != null)
    allItems.result
  } catch {
    case e: aws.model.ResourceNotFoundException => Vector.empty[Item]
  }


  def queryWithIndex(
    table: Table,
    index: SecondaryIndex,
    keyConditions: Seq[(String, aws.model.Condition)],
    select: Select,
    attributesToGet: Seq[String],
    scanIndexForward: Boolean,
    consistentRead: Boolean
  ): Seq[Item] = queryWithIndex(table.name, index, keyConditions, select, attributesToGet, scanIndexForward, consistentRead)


  def query(
    tableName: String,
    keyConditions: Seq[(String, aws.model.Condition)],
    attributesToGet: Seq[String] = Nil,
    scanIndexForward: Boolean = true,
    consistentRead: Boolean = false
  ): IndexedSeq[Item] = try {

    val req = new aws.model.QueryRequest()
      .withTableName(tableName)
      .withKeyConditions(keyConditions.toMap.asJava)
      .withScanIndexForward(scanIndexForward)
      .withConsistentRead(consistentRead)
    if (!attributesToGet.isEmpty) {
      req.setAttributesToGet(attributesToGet.asJava)
    }
    val allItems = new VectorBuilder[Item]()
    // Collect paginated results
    var lastEvaluatedKey: util.Map[String, model.AttributeValue] = null
    do {
      val queryResult = query(req)
      lastEvaluatedKey = queryResult.getLastEvaluatedKey
      val items = queryResult.getItems.asScala.map(i => Item(i))
      allItems ++= items
    } while (lastEvaluatedKey != null)
    allItems.result
  } catch {
    case e: aws.model.ResourceNotFoundException => Vector.empty[Item]
  }

  def query(
    table: Table,
    keyConditions: Seq[(String, aws.model.Condition)],
    attributesToGet: Seq[String],
    scanIndexForward: Boolean,
    consistentRead: Boolean
  ): IndexedSeq[Item] = query(table.name, keyConditions, attributesToGet, scanIndexForward, consistentRead)


  def scan(
    tableName: String,
    filter: Seq[(String, aws.model.Condition)] = Seq.empty[(String, aws.model.Condition)],
    limit: Option[Int] = None,
    segment: Int = 0,
    totalSegments: Int = 1,
    attributesToGet: Seq[String] = Nil
  ): IndexedSeq[Item] = try {

    val req = new aws.model.ScanRequest()
      .withTableName(tableName)
      .withScanFilter(filter.toMap.asJava)
      .withSegment(segment)
      .withTotalSegments(totalSegments)

    limit.foreach { x ⇒ req.setLimit(x) }


    if (!attributesToGet.isEmpty)
      req.setAttributesToGet(attributesToGet.asJava)

    val allItems = new VectorBuilder[Item]()
    // Collect paginated results
    var lastEvaluatedKey: util.Map[String, model.AttributeValue] = null
    do {
      val result = scan(req)
      lastEvaluatedKey = result.getLastEvaluatedKey
      val items = result.getItems.asScala.map(i => Item(i))
      allItems ++= items
    } while (lastEvaluatedKey != null)
    allItems.result
  } catch {
    case e: aws.model.ResourceNotFoundException => Vector.empty[Item]
  }


  def scan(
    table: Table,
    filter: Seq[(String, aws.model.Condition)],
    limit: Int,
    segment: Int,
    totalSegments: Int,
    attributesToGet: Seq[String]
  ): IndexedSeq[Item] = scan(table.name, filter, Some(limit), segment, totalSegments, attributesToGet)

}

/**
  * Default Implementation
  *
  * @param credentialsProvider credentialsProvider
  */
class DynamoDBClient(credentialsProvider: CredentialsProvider = CredentialsLoader.load())
  extends aws.AmazonDynamoDBClient(credentialsProvider)
    with DynamoDB

