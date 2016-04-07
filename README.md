# DynamoDBScala
A DynamoDB wrapper for Scala, forked from the client in AWS Scala, with less dependencies and used in production.

## Testing
Use an instance of
[DynamoDBLocal](http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Tools.DynamoDBLocal.html)
Start with:
```bash
java -Djava.library.path=./DynamoDBLocal_lib -jar DynamoDBLocal.jar -sharedDb -inMemory
```

Then run

`sbt test`

## Examples
[see unit tests](src/test/scala/DynamoDBV2Spec.scala)
