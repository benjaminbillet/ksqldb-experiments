package fr.benjaminbillet.querier;

import java.util.List;
import java.util.Map;

import io.confluent.ksql.api.client.AcksPublisher;
import io.confluent.ksql.api.client.Client;
import io.confluent.ksql.api.client.ClientOptions;
import io.confluent.ksql.api.client.InsertsPublisher;
import io.confluent.ksql.api.client.KsqlObject;
import io.confluent.ksql.api.client.BatchedQueryResult;
import io.confluent.ksql.api.client.Row;
import io.confluent.ksql.api.client.StreamedQueryResult;

public class Application {
  public static void main(String[] args) throws Exception {
    ClientOptions options = ClientOptions.create().setHost("localhost");
    Client client = Client.create(options);

    String pushQuery = "SELECT * FROM ORDERS WHERE PRODUCT_NAME = 'product50' EMIT CHANGES;";
    StreamedQueryResult streamedQueryResult = client.streamQuery(pushQuery, Map.of(
      "auto.offset.reset", "earliest"
    )).get();

    int count = 0;
    while (true) {
      // Block until a new row is available
      Row row = streamedQueryResult.poll();
      if (row != null) {
        count++;
        System.out.println("Row: " + row.values() + " " + count);
      } else {
        System.out.println("Query has ended.");
        break;
      }
    }
    
    /*String pullQuery = "SELECT * FROM ORDERS_AGG WHERE PRODUCT_NAME = 'product50';";
    BatchedQueryResult batchedQueryResult = client.executeQuery(pullQuery, Map.of(
      "auto.offset.reset", "earliest"
    ));

    List<Row> resultRows = batchedQueryResult.get();

    System.out.println("Received results. Num rows: " + resultRows.size());
    for (Row row : resultRows) {
      System.out.println("Row: " + row.values());
    }*/

    client.close();
  }
}
