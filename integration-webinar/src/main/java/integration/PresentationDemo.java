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

import akka.stream.alpakka.jms.javadsl.JmsSink;
import akka.stream.alpakka.jms.javadsl.JmsSource;
import akka.stream.alpakka.jms.JmsSinkSettings;
import akka.stream.alpakka.jms.JmsSourceSettings;

public final class PresentationDemo {

  enum Transport {
    file, jms, test, stdio;
  }

 
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

    @Override public final String toString() {
      return "Invoice(customerId = "+customerId+", orderId = "+orderId+", invoiceId = "+invoiceId+", amountInCents = "+amountInCents+")";
    }
  }


  public static void main(final String[] args) throws Exception {
    final URI inputURI = new URI(args.length > 1 ? args[0] : Transport.stdio + ":in");
    final URI outputURI = new URI(args.length > 1 ? args[1] : args.length > 0 ? args[0] : Transport.stdio + ":out");


    final Flow<ByteString, String, NotUsed> bytesUtf8ToJson = JsonFraming.objectScanner(1024).map(bytes -> bytes.utf8String());
    

    final Flow<String, ByteString, NotUsed> jsonToUtf8Bytes = Flow.of(String.class).map(ByteString::fromString);


    final ObjectMapper mapper = new ObjectMapper();
    final ObjectReader readOrder = mapper.readerFor(Order.class);
    final ObjectReader readInvoice = mapper.readerFor(Invoice.class);
    final ObjectWriter writeInvoice = mapper.writerFor(Invoice.class);
    final ObjectWriter writeOrder = mapper.writerFor(Order.class);

    final Flow<String, Order, NotUsed> jsonToOrder = Flow.of(String.class).map(readOrder::<Order>readValue);
    final Flow<String, Invoice, NotUsed> jsonToInvoice = Flow.of(String.class).map(readInvoice::<Invoice>readValue);
    final Flow<Invoice, String, NotUsed> invoiceToJson = Flow.of(Invoice.class).map(writeInvoice::writeValueAsString);
    final Flow<Order, String, NotUsed> orderToJson = Flow.of(Order.class).map(writeOrder::writeValueAsString);

    final Flow<Invoice, ByteString, NotUsed> invoiceToUTF8JsonDelimited = invoiceToJson.intersperse(",\n\r").via(jsonToUtf8Bytes);

    final Sink<Order, ?> logOrder = Sink.foreach(order -> System.err.println("Order: " + order));
    final Sink<Invoice, ?> logInvoice = Sink.foreach(invoice -> System.err.println("Invoice: " + invoice));


    final Flow<Order, Order, NotUsed> validate = Flow.of(Order.class);

    
    final Flow<Order, Order, NotUsed> enrich = Flow.of(Order.class);

    
    final Flow<Order, Invoice, NotUsed> xform =
      Flow.of(Order.class).map(order -> new Invoice(order.customerId, order.orderId, ThreadLocalRandom.current().nextLong(), order.amountInCents));


    final Flow<Order, Invoice, NotUsed> orderProcessing = validate.via(enrich).via(xform);


    final Source<Order, NotUsed> generateOrders = Source.repeat("").map(unused -> {
      final Random r = ThreadLocalRandom.current();
      return new Order(r.nextLong(), r.nextLong(), r.nextLong());
    });


    final Source<Order, ?> input;
    switch(Transport.valueOf(requireNonNull(inputURI.getScheme(), "Input URI has null scheme"))) {
      case file:
        final Path path = inputURI.isOpaque() ? Paths.get(inputURI.getSchemeSpecificPart()) : Paths.get(inputURI);
        input = FileIO.fromPath(path).via(bytesUtf8ToJson).via(jsonToOrder);
        break;
      case jms:
        final URI connectionURI = new URI(inputURI.getSchemeSpecificPart());
        input = JmsSource.textSource(JmsSourceSettings.create(new ActiveMQConnectionFactory(connectionURI)).withQueue("orders").withBufferSize(8)).via(jsonToOrder);
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


    final Sink<Invoice, ? extends CompletionStage<?>> output;
    switch(Transport.valueOf(requireNonNull(outputURI.getScheme(), "Output URI has null scheme"))) {
      case file:
        final Path path = outputURI.isOpaque() ? Paths.get(outputURI.getSchemeSpecificPart()) : Paths.get(outputURI);
        output = invoiceToUTF8JsonDelimited.toMat(FileIO.toPath(path), Keep.right());
        break;
      case jms:
        final URI connectionURI = new URI(outputURI.getSchemeSpecificPart());
        output = invoiceToJson.toMat(JmsSink.textSink(JmsSinkSettings.create(new ActiveMQConnectionFactory(connectionURI)).withQueue("invoices")), Keep.right());
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

    final RunnableGraph<? extends CompletionStage<?>> pipeline = 
      input.alsoTo(logOrder).via(orderProcessing).alsoTo(logInvoice).toMat(output, Keep.right());


    final ActorSystem system = ActorSystem.create("integration");
    final ActorMaterializer materializer = ActorMaterializer.create(system);


    final CompletionStage<?> done = pipeline.run(materializer);


    done.thenRunAsync(() -> system.terminate(), system.dispatcher());
  }
}
