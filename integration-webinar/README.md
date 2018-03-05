# Examples

## To run the JMS broker:

```sbt> runMain integration.Broker integration tcp://localhost:12345```

## To run the app from the command line, first run `assembly` on the sbt project:

```cat orders.json | java -jar target/scala-2.12/integration.jar```

```cat orders.json | java -jar target/scala-2.12/integration.jar file:invoices.json```

```java -jar target/scala-2.12/integration.jar file:orders.json file:invoices.json```

The assembled jar interprets:

  * 0 parameters to use stdin and stdout.
  * 1 parameter = use stdin as input and the parameter as the output.
  * 2 parameters = use the first parameter as input and the second parameter as output.

Parameters are resource URIs, the following schemes are supported: jms, file, stdio, test.

  * jms example: jms:tcp://localhost:12345
  * file example: file:orders.json (relative path) or file:///Users/foo/bar/orders.json (absolute path)
  * stdio example: stdio:_ (part after scheme is ignored)
  * test example: test:_ (part after scheme is ignored)
