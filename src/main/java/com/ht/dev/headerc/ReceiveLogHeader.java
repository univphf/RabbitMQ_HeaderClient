package com.ht.dev.headerc;

import com.rabbitmq.client.*;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class ReceiveLogHeader {
  private static final String EXCHANGE_NAME = "header_test";

  public static void main(String[] argv) throws Exception {
    if (argv.length < 1) {
      System.err.println("Utilisation: ReceiveLogsHeader queueName [headers]...");
      System.exit(1);
    }

    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost("localhost");
    Connection connection = factory.newConnection();
    Channel channel = connection.createChannel();

    //declarer un Exchange de type HEADERS
    channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.HEADERS);

    //la clé de routage ne sert a rien mais est obligatoire
    String routingKeyFromUser = "RoutingKeyInutile";

    //argument 0 est le nom de la file de messages
    String queueInputName = argv[0];


    Map<String, Object> headers = new HashMap<String, Object>();
    headers.put("x-match","any");  //all ou any

    //les autres arguments sont les clés qui permettront le routage.
    // a tester
    for (int i = 1; i < argv.length; i++) {
      headers.put(argv[i], argv[i + 1]);
      System.out.println("Binding header clé= " + argv[i] + " valeur= " + argv[i + 1] + " sur la file " + queueInputName);
      i++;
    }

    String queueName = channel.queueDeclare(queueInputName, true, false, false, null).getQueue();
    channel.queueBind(queueName, EXCHANGE_NAME, routingKeyFromUser, headers);

    System.out.println(" [*] En attente de messages.CTRL+C pour quitter");

    Consumer consumer = new DefaultConsumer(channel) {
      @Override
      public void handleDelivery(String consumerTag, Envelope envelope,
                                 AMQP.BasicProperties properties, byte[] body) throws IOException {
        String message = new String(body, "UTF-8");
        System.out.println(" [x] Received '" + envelope.getRoutingKey() + "':'" + message + "'");
      }
    };
    channel.basicConsume(queueName, true, consumer);
  }
}

