import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import io.debezium.config.Configuration;
import io.debezium.embedded.EmbeddedEngine;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.format.Json;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class Test1 {
    public static void main(String[] args) throws IOException {
        C1 c = new C1();
        c.run();
    }
}


class C1 {
    public void run() {

        Logger root = (Logger) LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
        root.setLevel(Level.DEBUG);

        Configuration config = Configuration.create()
                .with("connector.class", "io.debezium.connector.oracle.OracleConnector")
                .with("tasks.max", "1")
                .with("offset.storage", "org.apache.kafka.connect.storage.FileOffsetBackingStore")
                .with("offset.storage.file.filename", "a:\\tmp\\offset5.dat") //provide per your environment.
                .with("offset.flush.interval.ms", 2000)
                .with("name", "oracle_debezium_connector")
                .with("database.hostname", "172.16.16.88") //provide docker ip address if docker container db is used.
                .with("database.port", "1521")
                .with("database.user", "dbzuser")
                .with("database.password", "dbz")
                .with("database.dbname", "asurdb")
                .with("database.server.name", "asurdb")
//                .with("database.out.server.name", "dbzxout")

                .with("schema.include.list", "ARM")
                .with("table.include.list", "ARM.TMPTABLE")

                .with("database.history", "io.debezium.relational.history.FileDatabaseHistory")
                .with("database.history.file.filename", "a:\\tmp\\dbhistory3.dat") //provide per your environment.

                .with("snapshot.locking.mode", "none")
                .with("snapshot.mode", "schema_only")

                .with("poll.interval.ms", "1000")
                .with("log.mining.batch.size.default", "100000")
                .with("log.mining.batch.size.min", "10000")
                .with("log.mining.batch.size.max", "500000")



//                .with("database.dbname", "ORCLCDB")
//                .with("database.pdb.name", "ORCLPDB1")
//                .with("table.whitelist", "CUSTOMERS" )
                .build();


        EmbeddedEngine.CompletionCallback completionCallback = (b, s, throwable) -> {
            System.out.println("---------------------------------------------------");
            System.out.println("success status: " + b + ", message : " + s + ", Error: " + throwable);
        };


        EmbeddedEngine.ConnectorCallback connectorCallback = new EmbeddedEngine.ConnectorCallback() {
            /**
             * Called after a connector has been successfully started by the engine;
             */
            @Override
            public void connectorStarted() {
                System.out.println("connector started successfully");
            }

            /**
             * Called after a connector has been successfully stopped by the engine;
             */
            @Override
            public void connectorStopped() {
                System.out.println("connector stopped successfully");
            }

            /**
             * Called after a connector task has been successfully started by the engine;
             */
            @Override
            public void taskStarted() {
                System.out.println("connector task has been successfully started");
            }

            /**
             * Called after a connector task has been successfully stopped by the engine;
             */
            @Override
            public void taskStopped() {
                System.out.println("connector task has been successfully stopped");
            }
        };


        EmbeddedEngine engine = EmbeddedEngine.create()
                .using(config)
                .using(connectorCallback)
                .using(completionCallback)
                .notifying(this::handleEvent)
                .build();


        ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.execute(engine);
    }


    public void handleEvent(ConnectRecord connectRecord) {
        Struct rec = ((Struct)connectRecord.value());
        String operation = rec.get("op").toString();
        String beforeSum = ((Struct)rec.get("before")).get("SUMMA").toString();
        String afterSum = ((Struct)rec.get("after")).get("SUMMA").toString();

        try {
            synchronized (this) {
                PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter("a:\\tmp\\operations.csv", true)));
                out.println(operation + " \t " + beforeSum + " \t " + afterSum);
                out.close();
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
