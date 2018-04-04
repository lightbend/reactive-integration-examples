/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package integration;

import java.util.List;
import java.util.Random;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.CompletionStage;
import static java.util.Objects.requireNonNull;
import java.net.URI;

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

import akka.stream.alpakka.jms.javadsl.JmsProducer;
import akka.stream.alpakka.jms.javadsl.JmsConsumer;
import akka.stream.alpakka.jms.JmsConsumerSettings;
import akka.stream.alpakka.jms.JmsProducerSettings;

public final class PresentationDemo {

  /*
   * We create an enum to represent the different transports we currently support.
   */
  enum Transport {
    file, jms, test, stdio;
  }

  /*
   * Our domain consists of Orders and Invoices, so we define them below.
   * Feel free to add more fields.
   */
  final static class Order {
    public final long customerId;
    public final long orderId;
    public final long amountInCents;

    @JsonCreator
    public Order(
      @JsonProperty("customerId") long customerId,
      @JsonProperty("orderId") long orderId,
      @JsonProperty("amountInCents") long amountInCents) {
        this.customerId = customerId;
        this.orderId = orderId;
        this.amountInCents = amountInCents;
    }

    // equals / hashCode are omitted for brevity

    @Override public final String toString() {
      return "Order(customerId = "+customerId+", orderId = "+orderId+", amountInCents = "+amountInCents+")";
    }
  }



  final static class Invoice {
    public final long customerId;
    public final long orderId;
    public final long invoiceId;
    public final long amountInCents;

    @JsonCreator
    public Invoice(
      @JsonProperty("customerId") long customerId,
      @JsonProperty("orderId") long orderId,
      @JsonProperty("invoiceId") long invoiceId,
      @JsonProperty("amountInCents") long amountInCents) {
        this.customerId = customerId;
        this.orderId = orderId;
        this.invoiceId = invoiceId;
        this.amountInCents = amountInCents;
    }

    // equals / hashCode are omitted for brevity

    @Override public final String toString() {
      return "Invoice(customerId = "+customerId+", orderId = "+orderId+", invoiceId = "+invoiceId+", amountInCents = "+amountInCents+")";
    }
  }

  /*
   * This demo reads Orders and writes Invoices.
   *
   * The following source/destination schemes are supported:
   *
   *   - jms: a JMS broker
   *   - file: a relative or absolute path to a file
   *   - stdio: for source it will use stdin, for destination it will use stdout
   *   - test: for source it will generate random Orders, for destination it will report number of invoices created
   *
   * If this main method is invoked without parameters, it will read Orders as UTF-8 encoded JSON from stdin,
   * and it will produce Invoices as UTF-8 encoded JSON to stdout.
   *
   * If invoked with a single parameter, it will be an URI to the destination of Invoices.
   *
   * If invoked with two parameters, the first will be an URI the source of Orders and the second will be an URI to the destination of Invoices.
   *
   * Example invocations:
   *
   * Read order file using relative path and send invoices to the JMS broker which listens to localhost:12345
   * PresentationDemo.main(new String[] { "file:orders.json", "jms:tcp://localhost:12345"})
   *
   * Read orders from stdin and write invoices to stdout
   * PresentationDemo.main(new String[] {})
   *
   * Read orders from stdin and write invoices to a local file
   * PresentationDemo.main(new String[] { "file:///Invoices/invoices.json" })
   *
   */
  public static void main(final String[] args) throws Exception {

    // Handles the cases for 0..1..2 input parameters
    final URI inputURI = new URI(args.length > 1 ? args[0] : Transport.stdio + ":in");
    final URI outputURI = new URI(args.length > 1 ? args[1] : args.length > 0 ? args[0] : Transport.stdio + ":out");


    // Parses JSON-strings from a stream of UTF-8 encoded bytes
    final Flow<ByteString, String, NotUsed> bytesUtf8ToJson =
      JsonFraming.objectScanner(1024).map(bytes -> bytes.utf8String());
    
    // Converts Strings to a stream of bytes using UTF-8
    final Flow<String, ByteString, NotUsed> jsonToUtf8Bytes =
      Flow.of(String.class).map(ByteString::fromString);

    // Defines JSON converters from/to Orders and Invoices
    final ObjectMapper mapper = new ObjectMapper();
    final ObjectReader readOrder = mapper.readerFor(Order.class);
    final ObjectReader readInvoice = mapper.readerFor(Invoice.class);
    final ObjectWriter writeInvoice = mapper.writerFor(Invoice.class);
    final ObjectWriter writeOrder = mapper.writerFor(Order.class);

    final Flow<String, Order, NotUsed> jsonToOrder =
      Flow.of(String.class).map(readOrder::<Order>readValue);

    final Flow<String, Invoice, NotUsed> jsonToInvoice =
      Flow.of(String.class).map(readInvoice::<Invoice>readValue);

    final Flow<Invoice, String, NotUsed> invoiceToJson =
      Flow.of(Invoice.class).map(writeInvoice::writeValueAsString);

    final Flow<Order, String, NotUsed> orderToJson =
      Flow.of(Order.class).map(writeOrder::writeValueAsString);

    // Converts a stream of Invoices to a stream of UFT-8 encoded JSON, with commas between each Invoice
    final Flow<Invoice, ByteString, NotUsed> invoiceToUTF8JsonDelimited =
      invoiceToJson.intersperse(",\n\r").via(jsonToUtf8Bytes);

    // A utility which allows us to output to stderr every Order received
    final Sink<Order, ?> logOrder =
      Sink.foreach(order -> System.err.println("Order: " + order));

    // A utility which allows us to output to stderr every Invoice received
    final Sink<Invoice, ?> logInvoice =
      Sink.foreach(invoice -> System.err.println("Invoice: " + invoice));

    // A placeholder for an Order validation step, feel free to add a validation here.
    final Flow<Order, Order, NotUsed> validate = Flow.of(Order.class);

    // A placeholder for an Order enrichment step, feel free to add an enrichment operation here
    final Flow<Order, Order, NotUsed> enrich = Flow.of(Order.class);

    // The transformation from Orders to Invoices
    final Flow<Order, Invoice, NotUsed> xform =
      Flow.of(Order.class).map(order -> new Invoice(order.customerId, order.orderId, ThreadLocalRandom.current().nextLong(), order.amountInCents));

    // Composes validation with enrichment and the transformation from Order to Invoice
    final Flow<Order, Invoice, NotUsed> orderProcessing = validate.via(enrich).via(xform);

    // Defines an unbounded source of randomly generated Orders, great for generating sample data
    final Source<Order, NotUsed> generateOrders = Source.repeat("").map(unused -> {
      final Random r = ThreadLocalRandom.current();
      return new Order(r.nextLong(), r.nextLong(), r.nextLong());
    });

    // Since our demo can receive data from many different sources, we need to implement support for them below:
    final Source<Order, ?> input;
    switch(Transport.valueOf(requireNonNull(inputURI.getScheme(), "Input URI has null scheme"))) {
      case file:
        final Path path = inputURI.isOpaque() ? Paths.get(inputURI.getSchemeSpecificPart()) : Paths.get(inputURI);
        input = FileIO.fromPath(path).via(bytesUtf8ToJson).via(jsonToOrder);
        break;
      case jms:
        final URI connectionURI = new URI(inputURI.getSchemeSpecificPart());
        input = JmsConsumer.textSource(JmsConsumerSettings.create(new ActiveMQConnectionFactory(connectionURI)).withQueue("orders").withBufferSize(8)).via(jsonToOrder);
        break;
      case test:
        input = generateOrders.take(1000);
        break;
      case stdio:
        input = StreamConverters.fromInputStream(() -> System.in).via(bytesUtf8ToJson).via(jsonToOrder);
        break;
      default:
        input = Source.empty();
        break;
    }

    // And we need to be able to send Invoices to many different destinations, so we implement support for those here:
    final Sink<Invoice, ? extends CompletionStage<?>> output;
    switch(Transport.valueOf(requireNonNull(outputURI.getScheme(), "Output URI has null scheme"))) {
      case file:
        final Path path = outputURI.isOpaque() ? Paths.get(outputURI.getSchemeSpecificPart()) : Paths.get(outputURI);
        output = invoiceToUTF8JsonDelimited.toMat(FileIO.toPath(path), Keep.right());
        break;
      case jms:
        final URI connectionURI = new URI(outputURI.getSchemeSpecificPart());
        output = invoiceToJson.toMat(JmsProducer.textSink(JmsProducerSettings.create(new ActiveMQConnectionFactory(connectionURI)).withQueue("invoices")), Keep.right());
        break;
      case test:
        output = Flow.of(Invoice.class).fold(0L, (acc, i) -> acc + 1).toMat(Sink.foreach(sum -> System.out.println("Processed " + sum + " orders into invoices.")), Keep.right());
        break;
      case stdio:
        output = invoiceToUTF8JsonDelimited.toMat(StreamConverters.fromOutputStream(() -> System.out), Keep.right());
        break;
      default:
        output = Sink.ignore();
        break;
    }

    // Now we have all the different pieces of logic, and can compose our processing pipeline as follows:
    final RunnableGraph<? extends CompletionStage<?>> pipeline = 
      input.alsoTo(logOrder).via(orderProcessing).alsoTo(logInvoice).toMat(output, Keep.right());

    // In order to *run* our integration pipeline, we need an ActorSystem, and a Materializer.
    // The Materializer transforms the description of the pipeline to a running pipeline.
    final ActorSystem system = ActorSystem.create("integration");
    final ActorMaterializer materializer = ActorMaterializer.create(system);

    // Not until this line to we actually process any orders. We use the returned CompletionStage to track completion.
    final CompletionStage<?> done = pipeline.run(materializer);

    // And when our pipeline has completed, we want to clean up resources, so we terminate the ActorSystem.
    done.thenRunAsync(() -> system.terminate(), system.dispatcher());
  }
}
