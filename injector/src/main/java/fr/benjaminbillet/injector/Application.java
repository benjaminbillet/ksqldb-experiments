package fr.benjaminbillet.injector;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import io.confluent.ksql.api.client.AcksPublisher;
import io.confluent.ksql.api.client.Client;
import io.confluent.ksql.api.client.ClientOptions;
import io.confluent.ksql.api.client.InsertsPublisher;
import io.confluent.ksql.api.client.KsqlObject;
import io.confluent.ksql.api.client.QueryInfo;
import io.confluent.ksql.api.client.StreamInfo;
import io.confluent.ksql.api.client.TableInfo;

public class Application {
  public static void main(String[] args) throws Exception {
    ClientOptions options = ClientOptions.create().setHost("localhost");
    Client client = Client.create(options);

    // create the table if not exists
    List<QueryInfo> queries = client.listQueries().get();
    Optional<QueryInfo> ordersAggQuery = queries.stream()
      .filter(t -> t.getSink().map(x -> x.equalsIgnoreCase("ORDERS_AGG")).orElse(false))
      .findAny();
    if (ordersAggQuery.isPresent()) {
      client.executeStatement("TERMINATE " + ordersAggQuery.get().getId() + ";").get();
    }

    List<StreamInfo> streams = client.listStreams().get();
    Optional<StreamInfo> ordersStream = streams.stream()
      .filter(t -> t.getName().equalsIgnoreCase("ORDERS"))  
      .findAny();
    if (ordersStream.isPresent()) {
      client.executeStatement("DROP STREAM orders DELETE TOPIC;").get();
    }

    List<TableInfo> tables = client.listTables().get();
    Optional<TableInfo> ordersAggTable = tables.stream()
      .filter(t -> t.getName().equalsIgnoreCase("ORDERS_AGG"))  
      .findAny();
    if (ordersAggTable.isPresent()) {
      client.executeStatement("DROP TABLE orders_agg DELETE TOPIC;").get();
    }

    String sql = "CREATE STREAM orders (ORDER_ID BIGINT KEY, PRODUCT_NAME VARCHAR) "
    + "WITH (KAFKA_TOPIC='orders', VALUE_FORMAT='json', PARTITIONS=3);";
    client.executeStatement(sql).get();

    sql = "CREATE TABLE orders_agg "
    + "WITH (KAFKA_TOPIC='orders_agg', VALUE_FORMAT='json', PARTITIONS=3) "
    + "AS SELECT ORDER_ID, LATEST_BY_OFFSET(PRODUCT_NAME) AS PRODUCT_NAME FROM orders GROUP BY ORDER_ID EMIT CHANGES;";
    client.executeStatement(sql, Map.of(
      "auto.offset.reset", "earliest"
    )).get();

    /*InsertsPublisher insertsPublisher = new InsertsPublisher();
    AcksPublisher acksPublisher = client.streamInserts("ORDERS", insertsPublisher).get();
    acksPublisher.subscribe(new AcksSubscriber());

    for (long i = 0; i < 10000; i++) {
      KsqlObject row = new KsqlObject()
          .put("ORDER_ID", i)
          .put("PRODUCT_NAME", "product " + (i%100));
      insertsPublisher.accept(row);
      if ((i+1) % 100 == 0) {
        System.out.println("Inserted 100 elements");
      }
    }
    insertsPublisher.complete();*/


    for (long i = 0; i < 10000; i++) {
      KsqlObject row = new KsqlObject()
          .put("ORDER_ID", i)
          .put("PRODUCT_NAME", "product" + (i%100));
      client.insertInto("ORDERS", row);
      if ((i+1) % 100 == 0) {
        System.out.println("Inserted 100 elements");
      }
    }
    System.out.println("Completed");

    //client.close();
  }
}
