package aws

import aws.dynamo._
import com.amazonaws.services.{ dynamodbv2 => aws }
import org.scalatest._
import org.slf4j._
import scala.util.Try

class DynamoDBV2Spec extends FlatSpec with Matchers {

  behavior of "DynamoDB"

  val log = LoggerFactory.getLogger(this.getClass)

  it should "provide cool APIs for Hash PK tables" in {
    implicit val dynamoDB = DynamoDB.local()

    val tableName = s"Companies_${System.currentTimeMillis}"
    val createdTableMeta: TableMeta = dynamoDB.createTable(
      name = tableName,
      hashPK = "Id" -> AttributeType.String
    )
    log.info(s"Created Table: ${createdTableMeta}")

    println(s"Waiting for DynamoDB table activation...")
    var isTableActivated = false
    while (!isTableActivated) {
      dynamoDB.describe(createdTableMeta.table).map { meta =>
        isTableActivated = meta.status == aws.model.TableStatus.ACTIVE
      }
      Thread.sleep(1000L)
      print(".")
    }
    println("")
    println(s"Created DynamoDB table has been activated.")

    val companies: Table = dynamoDB.table(tableName).toOption.get

    companies.put("Amazon", "url" -> "http://www.amazon.com/")
    companies.put("Google", "url" -> "http://www.google.com/")
    companies.put("Microsoft")

    // get by primary key
    val google: Item = companies.get("Google").getOrElse(fail)
    google.attributes("url").s.get should equal("http://www.google.com/")

    val nonExistant = companies.get("I Don't Exist").toOption
    nonExistant shouldBe None

    // scan
    val foundCompanies: Seq[Item] = companies.scan(Seq("url" -> cond.isNotNull))
    foundCompanies.size should equal(2)

    val scanNonExistant: Seq[Item] = companies.scan(Seq("url" -> cond.eq("I Don't Exist")))
    scanNonExistant.size should equal(0)

    // putAttributes
    companies.putAttributes("Microsoft", Seq("url" -> "http://www.microsoft.com"))
    companies.get("Microsoft").getOrElse(fail).attributes("url").s.get should equal("http://www.microsoft.com")

    companies.destroy()
  }

  it should "provide cool APIs for Hash/Range PK tables" in {
    implicit val dynamoDB = DynamoDB.local()

    val tableName = s"Members_${System.currentTimeMillis}"
    val createdTableMeta: TableMeta = dynamoDB.createTable(
      name = tableName,
      hashPK = "Id" -> AttributeType.Number,
      rangePK = "Country" -> AttributeType.String,
      otherAttributes = Seq("Company" -> AttributeType.String),
      localSecondaryIndexes = Seq(
        LocalSecondaryIndex(
          name = "CompanyIndex",
          keySchema = Seq(KeySchema("Id", KeyType.Hash), KeySchema("Company", KeyType.Range)),
          projection = Projection(ProjectionType.Include, Seq("Company"))
        )
      )
    )
    log.info(s"Created Table: ${createdTableMeta}")

    println(s"Waiting for DynamoDB table activation...")
    var isTableActivated = false
    while (!isTableActivated) {
      dynamoDB.describe(createdTableMeta.table).map { meta =>
        isTableActivated = meta.status == aws.model.TableStatus.ACTIVE
      }
      Thread.sleep(1000L)
      print(".")
    }
    println("")
    println(s"Created DynamoDB table has been activated.")

    val members: Table = dynamoDB.table(tableName).getOrElse(fail)

    members.put(1, "Japan", "Name" -> "Alice", "Age" -> 23, "Company" -> "Google")
    members.put(2, "U.S.", "Name" -> "Bob", "Age" -> 36, "Company" -> "Google")
    members.put(3, "Japan", "Name" -> "Chris", "Age" -> 29, "Company" -> "Amazon")
    members.put(4, "Germany", "Name" -> "Pedro", "Age" -> 35, "Company" -> "Amazon")
    members.put(5, "Germany", "Name" -> "Robert", "Age" -> 30, "Company" -> "Amazon")

    val nonExistant: Option[Item] = members.get(4, "U.K.").toOption
    nonExistant shouldBe None

    val googlers: Seq[Item] = members.scan(Seq("Company" -> cond.eq("Google")))
    googlers.map(_.attributes("Name").s.get) should equal(Seq("Bob", "Alice"))

    val amazonians: Seq[Item] = members.scan(Seq("Company" -> cond.eq("Amazon")))
    amazonians.map(_.attributes("Name").s.get) should contain only(Seq("Chris", "Pedro", "Robert"):_*)

    val scanNonExistant: Seq[Item] = members.scan(Seq("Company" -> cond.eq("I Don't Exist")))
    scanNonExistant.size should equal(0)

    // putAttributes
    members.putAttributes(3, "Japan", Seq("Company" -> "Microsoft"))
    members.get(3, "Japan").getOrElse(fail).attributes("Company").s.get should equal("Microsoft")

    val exp = DynamoDBExpectedAttributeValue
    dynamoDB.putConditional(tableName, "Id" -> 3, "Country" -> "Japan",
      "Name" -> "Kris")(Seq("Age" -> exp.lt(29))).isLeft shouldBe true

    dynamoDB.putConditional(tableName, "Id" -> 3, "Country" -> "Japan",
      "Name" -> "Kris")(Seq("Age" -> exp.lt(30))).isRight shouldBe true

    members.destroy()
  }

  it should "convert maps to attribute values implicitly" in {
    implicit val dynamoDB = DynamoDB.local()

    val tableName = s"Members_${System.currentTimeMillis}"
    val createdTableMeta: TableMeta = dynamoDB.createTable(
      name = tableName,
      hashPK = "Id" -> AttributeType.Number,
      rangePK = "Country" -> AttributeType.String,
      otherAttributes = Seq("Company" -> AttributeType.String),
      localSecondaryIndexes = Seq(
        LocalSecondaryIndex(
          name = "CompanyIndex",
          keySchema = Seq(KeySchema("Id", KeyType.Hash), KeySchema("Company", KeyType.Range)),
          projection = Projection(ProjectionType.Include, Seq("Company"))
        )
      )
    )
    log.info(s"Created Table: ${createdTableMeta}")

    println(s"Waiting for DynamoDB table activation...")
    var isTableActivated = false
    while (!isTableActivated) {
      dynamoDB.describe(createdTableMeta.table).map { meta =>
        isTableActivated = meta.status == aws.model.TableStatus.ACTIVE
      }
      Thread.sleep(1000L)
      print(".")
    }
    println("")
    println(s"Created DynamoDB table has been activated.")

    val members: Table = dynamoDB.table(tableName).getOrElse(fail)

    members.put(1, "Japan", "Name" -> Map("foo" -> Map("bar" -> "brack")), "Age" -> 23, "Company" -> "Google")
    members.get(1, "Japan").getOrElse(fail).attributes("Name").m.get.get("foo").getM().get("bar").getS() should equal("brack")

    members.put(2, "Micronesia", "Name" -> Map("aliases" -> List("foo", "bar", "other")), "Age" -> 26, "Company" -> "Spotify")
    members.get(2, "Micronesia").getOrElse(fail).attributes("Name").m.get.get("aliases").getSS() should contain allOf ("foo", "bar", "other")
    members.destroy()
  }

  it should "provide cool APIs to use global secondary index" in {
    implicit val dynamoDB = DynamoDB.local()

    val tableName = s"Users_${System.currentTimeMillis}"
    val globalSecondaryIndex = GlobalSecondaryIndex(
      name = "SexIndex",
      keySchema = Seq(KeySchema("Sex", KeyType.Hash), KeySchema("Age", KeyType.Range)),
      projection = Projection(ProjectionType.All),
      provisionedThroughput = ProvisionedThroughput(readCapacityUnits = 10, writeCapacityUnits = 10)
    )
    val table = Table(
      name = tableName,
      hashPK = "Id",
      attributes = Seq(
        AttributeDefinition("Id", AttributeType.Number),
        AttributeDefinition("Sex", AttributeType.String),
        AttributeDefinition("Age", AttributeType.Number)
      ),
      globalSecondaryIndexes = Seq(globalSecondaryIndex)
    )
    val createdTableMeta: TableMeta = dynamoDB.createTable(table)
    log.info(s"Created Table: ${createdTableMeta}")

    println(s"Waiting for DynamoDB table activation...")
    var isTableActivated = false
    while (!isTableActivated) {
      dynamoDB.describe(createdTableMeta.table).map { meta =>
        isTableActivated = meta.status == aws.model.TableStatus.ACTIVE
      }
      Thread.sleep(1000L)
      print(".")
    }
    println("")
    println(s"Created DynamoDB table has been activated.")

    val users: Table = dynamoDB.table(tableName).getOrElse(fail)

    users.put(1, "Name" -> "John", "Sex" -> "Male", "Age" -> 12)
    users.put(2, "Name" -> "Bob", "Sex" -> "Male", "Age" -> 14)
    users.put(3, "Name" -> "Chris", "Sex" -> "Female", "Age" -> 9)
    users.put(4, "Name" -> "Michael", "Sex" -> "Male", "Age" -> 65)

    val teenageBoys: Seq[Item] = users.queryWithIndex(
      index = globalSecondaryIndex,
      keyConditions = Seq("Sex" -> cond.eq("Male"), "Age" -> cond.lt(20))
    )

    teenageBoys.map(_.attributes("Name").s.get) should equal(Seq("John", "Bob"))

    users.destroy()
  }

  it should "delete items from a table by specifying key conditions" in {
    val d = DynamoDB.local()
    val t = "deleteMe"
    d.createTable(t, ("k" → AttributeType.String))
    (0 until 100).foreach { i ⇒
      d.putItem(t,
        "k" → i.toString,
        "data" → (2*i).toString
      )
    }
    d.delete(t, Seq("k" → cond.eq("0")))
    d.delete(t, Seq("k" → cond.eq("10")))
    d.delete(t, Seq("k" → cond.eq("20")))
    val xs = d.scan(t, Seq.empty[(String, aws.model.Condition)])
    xs should have length 97

    d.deleteTable(t)
  }

  it should "delete items from a table by specifying key conditions with sort key" in {
    val d = DynamoDB.local()
    val t = "deleteMe"
    d.createTable(t,
      ("k" → AttributeType.String),
      ("s" → AttributeType.String)
    )

    (0 until 100).foreach { i ⇒
      d.putItem(t,
        "k" → "a",
        "s" → i.toString,
        "data" → (2*i).toString
      )
    }
    d.delete(t, Seq("k" → cond.eq("a")))
    val xs = d.scan(t, Seq.empty[(String, aws.model.Condition)])
    xs should have length 0

    d.deleteTable(t)
  }
}