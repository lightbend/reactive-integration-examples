/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package integration;

import org.apache.activemq.broker.BrokerService;

public class Broker {
  public static void main(String[] args) throws Exception{
    final String brokerName = args[0];
    final String connector = args[1];
    final BrokerService broker = new BrokerService() {
      { // We use an instance-initializer to set up our broker service.
        this.setUseJmx(false);
        this.setSchedulerSupport(false);
        this.setPersistent(false);
        this.setAdvisorySupport(false);
        this.setBrokerName(brokerName);
        final String connectTo = this.addConnector(connector).getPublishableConnectString();
        System.out.println("MQ running on: " + connectTo);
      }
    };
    broker.start(); // Once we've configured the broker, we'll start it.
    System.in.read();
    broker.stop();
    broker.waitUntilStopped();
  }
}