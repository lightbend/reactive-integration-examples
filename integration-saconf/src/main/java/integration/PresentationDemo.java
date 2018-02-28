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

public final class PresentationDemo {

  enum Transport {
    file, jms, test;
  }


 
  final static class Order {
    public final long customerId;
    public final long orderId;

    @JsonCreator
    public Order(
      @JsonProperty("customerId") long customerId,
      @JsonProperty("orderId") long orderId) {
        this.customerId = customerId;
        this.orderId = orderId;
    }

    @Override public final String toString() {
      return "Order("+customerId+", "+orderId+")";
    }
  }



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

    @Override public final String toString() {
      return "Invoice("+customerId+", "+orderId+", "+invoiceId+")";
    }
  }



  public static void main(final String[] args) throws Exception {

    if (args.length < 2) throw new IllegalArgumentException("Syntax: [file, jms, test](source) [file, jms, test](sink)");


    final Transport inputTransport = Transport.valueOf(args[0]);
    final Transport outputTransport = Transport.valueOf(args[1]);



    final String brokerName = "integration";
    final BrokerService broker = new BrokerService() {
      {
        this.setUseJmx(false);
        this.setBrokerName(brokerName);
      }
    };
    broker.start();



    final ConnectionFactory connectionFactory = new ActiveMQConnectionFactory("vm://"+brokerName+"?create=false");



    final ActorSystem system = ActorSystem.create("integration");
    final ActorMaterializer materializer = ActorMaterializer.create(system);
    final Executor ec = system.dispatcher();



    final Source<Order, NotUsed> generateOrders = Source.repeat("").map(unused -> {
      final Random r = ThreadLocalRandom.current();
      return new Order(r.nextLong(), r.nextLong());
    });



    final Source<Order, ?> fromTest = generateOrders.take(1000);



    final Flow<ByteString, String, NotUsed> bytesUtf8ToJson = JsonFraming.objectScanner(1024).map(bytes -> bytes.utf8String());
    final Flow<String, ByteString, NotUsed> jsonToUtf8Bytes = Flow.of(String.class).map(ByteString::fromString);



    final ObjectMapper mapper = new ObjectMapper();
    final ObjectReader readOrder = mapper.readerFor(Order.class);
    final ObjectWriter writeInvoice = mapper.writerFor(Invoice.class);
    final Flow<String, Order, NotUsed> jsonToOrder = Flow.of(String.class).map(readOrder::<Order>readValue);
    final Flow<Invoice, String, NotUsed> invoiceToJson = Flow.of(Invoice.class).map(writeInvoice::writeValueAsString).intersperse(",\n\r");



    final Source<Order, ?> fromFile = FileIO
      .fromPath(Paths.get("orders.json"))
      .via(bytesUtf8ToJson)
      .via(jsonToOrder);
    


    final Source<Order, ?> fromJms = 
      JmsSource
      .textSource(JmsSourceSettings.create(connectionFactory).withQueue("orders").withBufferSize(8))
      .takeWithin(FiniteDuration.create(60, TimeUnit.SECONDS))
      .via(jsonToOrder);
    


    final Sink<Invoice, ? extends CompletionStage<?>> toTest = 
      Sink.<Invoice>seq();



    final Sink<Invoice, ? extends CompletionStage<?>> toFile =
      invoiceToJson.via(jsonToUtf8Bytes).toMat(FileIO.toPath(Paths.get("invoices.json")), Keep.right());



    final Sink<Invoice, ? extends CompletionStage<?>> toJms =
      invoiceToJson.toMat(JmsSink.textSink(JmsSinkSettings.create(connectionFactory).withQueue("invoices")), Keep.right());


    
    final Flow<Order, Order, NotUsed> logInput =
      Flow.of(Order.class).alsoTo(Sink.foreach(order -> System.out.println("Processing order: " + order)));

    final Flow<Invoice, Invoice, NotUsed> logOutput =
      Flow.of(Invoice.class).alsoTo(Sink.foreach(invoice -> System.out.println("Created invoice: " + invoice)));



    final Flow<Order, Order, NotUsed> validate = Flow.of(Order.class);


    
    final Flow<Order, Order, NotUsed> enrich = Flow.of(Order.class);


    
    final Flow<Order, Invoice, NotUsed> xform =
      Flow.of(Order.class).map(order -> new Invoice(order.customerId, order.orderId, ThreadLocalRandom.current().nextLong()));



    final Flow<Order, Invoice, NotUsed> orderProcessing = validate.via(enrich).via(xform);



    final Source<Order, ?> input =
       (inputTransport == Transport.file) ? fromFile :
       (inputTransport == Transport.jms) ? fromJms :
       (inputTransport == Transport.test) ? fromTest : Source.empty(); 

    final Sink<Invoice, ? extends CompletionStage<?>> output =
      (outputTransport == Transport.file) ? toFile :
      (outputTransport == Transport.jms) ? toJms :
      (outputTransport == Transport.test) ? toTest : Sink.ignore(); 



    final RunnableGraph<? extends CompletionStage<?>> pipeline = input.via(logInput).via(orderProcessing).via(logOutput).toMat(output, Keep.right());



    final CompletionStage<?> done = pipeline.run(materializer);



    try {
      done.thenRunAsync(() -> System.out.println("Stream finished."), ec).
           thenRunAsync(() -> materializer.shutdown(), ec).
           thenRunAsync(() -> system.terminate(), ec).
           toCompletableFuture().get();
    } finally {
      broker.stop();
      broker.waitUntilStopped();
    }
  }
}
