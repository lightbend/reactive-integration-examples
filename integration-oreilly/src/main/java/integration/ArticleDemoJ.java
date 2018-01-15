/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package integration;

import java.util.List;
import java.util.Random;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.CompletionStage;

// Akka Streams
import akka.NotUsed;
import akka.Done;
import akka.util.ByteString;
import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.IOResult;
import akka.stream.javadsl.*;

// File IO
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;
import scala.concurrent.duration.FiniteDuration;

// JSON
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;

// JMS
import javax.jms.ConnectionFactory;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;

import akka.stream.alpakka.jms.javadsl.JmsSink;
import akka.stream.alpakka.jms.javadsl.JmsSource;
import akka.stream.alpakka.jms.JmsSinkSettings;
import akka.stream.alpakka.jms.JmsSourceSettings;

public final class ArticleDemoJ {

  /*
   * We define this enumeration to keep track of which transports are supported.
   */
  enum Transport {
    file, jms, test;
  }
 
  /*
   * For our example integration: Let's define an Order class.
   * Currently it only has the most basic of data,
   * and if you are interested, you should be able to
   * add more fields to it, and make this example your own.
   */
  final static class Order {
    public final long customerId;
    public final long orderId;
    // Add more fields here

    @JsonCreator
    public Order(
      @JsonProperty("customerId") long customerId,
      @JsonProperty("orderId") long orderId) {
        this.customerId = customerId;
        this.orderId = orderId;
    }

    // equals & hashCode omitted for brevity

    @Override public final String toString() {
      return "Order("+customerId+", "+orderId+")";
    }
  }

  /*
   * We're going to be receiving Orders, 
   * and from those Orders we'll want to create Invoices.
   * So let's create a bare-minimum data Invoice.
   * Feel encouraged to add more fields to both this, and the Order class.
   * Who knows, perhaps some day this becomes a fully fledged order processing system for someone?
   */
  final static class Invoice {
    public final long customerId;
    public final long orderId;
    public final long invoiceId;

    @JsonCreator
    public Invoice(
      @JsonProperty("customerId") long customerId, 
      @JsonProperty("orderId") long orderId,
      @JsonProperty("invoiceId") long invoiceId) {
        this.customerId = customerId;
        this.orderId = orderId;
        this.invoiceId = invoiceId;
    }

    // equals & hashCode omitted for brevity

    @Override public final String toString() {
      return "Invoice("+customerId+", "+orderId+", "+invoiceId+")";
    }
  }

  public static void main(final String[] args) throws Exception {
    if (args.length < 2) throw new IllegalArgumentException("Syntax: (source)[file, jms, test] (sink)[file, jms, test]");
    final Transport inputTransport = Transport.valueOf(args[0]);
    final Transport outputTransport = Transport.valueOf(args[1]);
    /*
     * In this integration example, we'll use files, in-memory data, and JMS.
     * If you don't have an existing JMS broker set up, then the following logic
     * will start an in-process broker that we can use for the demo.
     */
    final String brokerName = "integration";
    final BrokerService broker = new BrokerService() {
      { // We use an instance-initializer to set up our broker service.
        this.setUseJmx(false);
        this.setBrokerName(brokerName);
      }
    };
    broker.start(); // Once we've configured the broker, we'll start it.

    /* In order to communicate with the JMS broker we use a ConnectionFactory,
     * and we instruct it to *not* create the broker if it doesn't exist.
     *
     * If you want to, you can remove the BrokerService instatiation above,
     * and instead create a ConnectionFactory to your own test JMS broker instance.
     */
    final ConnectionFactory connectionFactory = new ActiveMQConnectionFactory("vm://"+brokerName+"?create=false");

    /*
     * Since we're going to use Akka Streams, we'll need to first create what's called an ActorSystem,
     * this is the main entrypoint to Akka-based applications. We name it "integration".
     *
     * When that it created, and because we're going to use Akka Streams, we're going to create an
     * ActorMaterializer. That will be responsible for taking our integration blueprints and making them execute their logic.
     *
     * Also, since we want to use CompletionStages at certain points in this exercise, we're going to have them execute their
     * logic on the Executor associated with the ActorSystem.
     */
    final ActorSystem system = ActorSystem.create("integration");
    final ActorMaterializer materializer = ActorMaterializer.create(system);
    final Executor ec = system.dispatcher();

    /*
     * If we want to generate some Orders (for example for testing, or as we'll see further along this example—have an in-memory data source.
     * We create an unbounded stream of Orders by repeating a dummy value (the empty String), and for each of those empty Strings we'll
     * generate an Order instance using a random generator.
     */
    final Source<Order, NotUsed> generateOrders = Source.repeat("").map(unused -> {
      final Random r = ThreadLocalRandom.current();
      return new Order(r.nextLong(), r.nextLong());
    });

    /*
     * In order (no pun intended) to create an test datasource, we'll random-generate 1000 Orders using the `generateOrders` Source.
     */
    final Source<Order, ?> fromTest =
      generateOrders.take(1000);

    /*
     * For this example, we're going to deserialize and serialize JSON (JavaScript Object Notation), here we define how bytes in UTF-8 encoding
     * will be converted to Strings. "1024" for the JsonFraming.objectScanner means that every Order is at most 1kB large, you may want to tweak that
     * if you augment the fields in the Order class.
     */
    final Flow<ByteString, String, NotUsed> bytesUtf8ToJson = JsonFraming.objectScanner(1024).map(bytes -> bytes.utf8String());
    final Flow<String, ByteString, NotUsed> jsonToUtf8Bytes = Flow.of(String.class).map(ByteString::fromString);

    /*
     * We also need to be able to convert Strings containing JSON to Java Objects and vice versa.
     * Most notably we'll need to read Orders from JSON and write Invoices to JSON.
     */
    final ObjectMapper mapper = new ObjectMapper();
    final ObjectReader readOrder = mapper.readerFor(Order.class);
    final ObjectWriter writeInvoice = mapper.writerFor(Invoice.class);
    final Flow<String, Order, NotUsed> jsonToOrder = Flow.of(String.class).map(readOrder::<Order>readValue);
    final Flow<Invoice, String, NotUsed> invoiceToJson = Flow.of(Invoice.class).map(writeInvoice::writeValueAsString).intersperse(",\n\r");

    /*
     * One of the possible datasources is a file containing orders, in this case named "orders.json".
     * Since the Orders in this file is stored as JSON (JavaScript Object Notation), we need to first
     * transform the raw bytes in the file to UTF-8 encoded characters and then construct Orders from the characters.
     */
    final Source<Order, ?> fromFile = FileIO
      .fromPath(Paths.get("orders.json"))
      .via(bytesUtf8ToJson)
      .via(jsonToOrder);
    
    /*
     * Another possible datasource for this example is JMS (Java Message Service).
     * So we're going to connect to a JMS broker, to its "orders" topic, and the orders which are sent to it.
     */
    final Source<Order, ?> fromJms = 
      JmsSource
      .textSource(JmsSourceSettings.create(connectionFactory).withQueue("orders").withBufferSize(8))
      //.takeWithin(FiniteDuration.create(2, TimeUnit.SECONDS)) // If you want to terminate after an idle timeout, uncomment this line and tweak the duration
      .via(jsonToOrder);
    
    /*
     * Once we have processed our Orders, we need to send them somewhere,
     * in this case, when testing, we define an in-memory destination for our Invoices,
     * since it will store the generated Invoices in memory it is crucial
     * that we don't create too many Invoices, but this is a good example
     * for how it could be used in a test.
     */
    final Sink<Invoice, ? extends CompletionStage<?>> toTest = 
      Sink.<Invoice>seq();

    /*
     * Another common destination for information is to a file,
     * in this case we'll want to be able to serialize our Invoices
     * as UTF-8 encoded JSON to the file system, more specifically to a file names "invoices.json".
     */
    final Sink<Invoice, ? extends CompletionStage<?>> toFile =
      invoiceToJson.via(jsonToUtf8Bytes).toMat(FileIO.toPath(Paths.get("invoices.json")), Keep.right());

    /*
     * The third option for a destination of Invoices in our example is to a JMS topic,
     * in this case it is named "invoices", and will be connecting to the in-process broker we started earlier.
     */
    final Sink<Invoice, ? extends CompletionStage<?>> toJms =
      invoiceToJson.toMat(JmsSink.textSink(JmsSinkSettings.create(connectionFactory).withQueue("invoices")), Keep.right());
    
    /*
     * In order to create some visibility into our processing pipeline, we're going to create two Flows which will
     * send everything which flows through them to a side-channel as well, in this case to a Sink prints them out to stdout.
     */
    final Flow<Order, Order, NotUsed> logInput =
      Flow.of(Order.class).alsoTo(Sink.foreach(order -> System.out.println("Processing order: " + order)));
    final Flow<Invoice, Invoice, NotUsed> logOutput =
      Flow.of(Invoice.class).alsoTo(Sink.foreach(invoice -> System.out.println("Created invoice: " + invoice)));

    /*
     * This is included because as an exercise, you might want to add validation logic to incoming Orders.
     * The example below simply passes anything through, and as an exercise you can change it to verify properties
     * of your orders, where one option is to discard all invalid orders, another to send invalid orders to a different output sink.
     */
    final Flow<Order, Order, NotUsed> validate = Flow.of(Order.class);
    
    /*
     * Another common situation is that one wants to "enrich" inbound data with additional information.
     * IF you want to do that, this would be a good place to do it. For example, one can make HTTP requests
     * to a service to look information up, and attach that to the Order.
     */
    final Flow<Order, Order, NotUsed> enrich = Flow.of(Order.class);
    
    /*
     * Perhaps rather unsurprisingly, we want to convert Orders into Invoices, so that our pipeline can start to add customer value.
     * You can make this transformation as elaborate as needed!
     */
    final Flow<Order, Invoice, NotUsed> xform =
      Flow.of(Order.class).map(order -> new Invoice(order.customerId, order.orderId, ThreadLocalRandom.current().nextLong()));

    /*
     * Let's take those pieces of reusable stream processing blueprints, and define a pipeline which logically encapsulates
     * or order processing logic. In our case that is the validation flow composed with the enrichment flow, which is then composed
     * with our order-to-invoice transformation logic.
     *
     * At this point, we have separated out the order processing logic such that it is separated from where the orders come from,
     * and where the invoices go to.
     */
    final Flow<Order, Invoice, NotUsed> orderProcessing = validate.via(enrich).via(xform);

    /*
     * Now, we want to try our orderProcessing logic out, so we need to select a source of orders.
     * Fortunately that has already been done, since that choice is the input arguments to the main method,
     * so we only need to map the transport type to an actual sink.
     */
    final Source<Order, ?> input =
       (inputTransport == Transport.file) ? fromFile :
       (inputTransport == Transport.jms) ? fromJms :
       (inputTransport == Transport.test) ? fromTest : Source.empty(); 

    /*
     * Using the arguments to the main method, we selected the transport to use for the destination.
     */
    final Sink<Invoice, ? extends CompletionStage<?>> output =
      (outputTransport == Transport.file) ? toFile :
      (outputTransport == Transport.jms) ? toJms :
      (outputTransport == Transport.test) ? toTest : Sink.ignore(); 

    /*
     * At this point we have all we need to compose a full integration pipeline, since we know which input, which processing logic, and with output.
     * Note that we are creating a full blueprint, so we can reuse the same blueprint many times, if we want to run several copies of this integration
     * pipeline in parallel.
     *
     * `toMat` and `Keep.right` means that we add the `output` as the Sink, and we keep what it materializes when we compose—a CompletionStage which is
     * completed once it is closed, so we can keep track of when we're done.
     */
    final RunnableGraph<? extends CompletionStage<?>> pipeline = input.via(logInput).via(orderProcessing).via(logOutput).toMat(output, Keep.right());

    /*
     * Also note that until this point we have only described what we want to happen, not actually instructed Akka Streams to make it happen,
     * which now becomes as simple as the following piece of logic. `run` will use the `materializer` to transform the blueprint `integration` into
     * a running stream which will turn Orders into Invoices between two selected transports.
     * The `pipeline` flow's Materialized Value is a CompletionStage which will become completed once the stream has finished its execution,
     * and we can use that to track when we should shut down the program.
     */
    final CompletionStage<?> done = pipeline.run(materializer);

    /*
     * When our integration pipeline's stream is finished, we'll want to clean up resources used, and exit the program.
     * This section of logic prints a completion message, then shuts down the ActorMaterializer, then terminates
     * the ActorSystem itself, and after all is done, stops the AMQP broker and waits for that to complete.
     */
    try {
      done.thenRunAsync(() -> System.out.println("Stream finished."), ec).
           thenRunAsync(() -> materializer.shutdown(), ec).
           thenRunAsync(() -> system.terminate(), ec).
           toCompletableFuture().get(); // Wait for finish.
    } finally {
      broker.stop();
      broker.waitUntilStopped();
    }
  }
}
