package org.mongodb.tse.tests;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClientURI;
import com.mongodb.ReadPreference;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.time.*;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.IntSummaryStatistics;
import java.util.List;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;


/**
 * Created by royrim on 7/20/17.
 */
public class Throughput {
    private static final Logger LOGGER = LoggerFactory.getLogger(Throughput.class.getName());

    private static boolean STOP = false;

    private static void printHelp(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("org.mongodb.tse.tests.RunQueryThreadPool", options);
        System.exit(0);
    }

    private static String[] parseIdFile(CommandLine cline) {
        if (cline.hasOption("idFile")) {
            File file = new File(cline.getOptionValue("idFile"));
            List<String> list = new ArrayList<>();
            BufferedReader reader;
            try {
                reader = new BufferedReader(new FileReader(file));
                String line;
                while ((line = reader.readLine()) != null) {
                    list.add(line);
                }
            } catch (Exception e) {
                System.err.println("Error with your id file");
                e.printStackTrace();
                System.exit(1);
            }
            return list.toArray(new String[]{});
        } else return cline.getOptionValue("ids", "1").split(",");

    }

    private static MongoCollection getCollection(CommandLine cline, MongoDatabase database, String collectionParameter) {
        MongoCollection collection;
        if (cline.hasOption("readPreference")) {
            String p = cline.getOptionValue("readPreference");
            collection = database.getCollection(collectionParameter).withReadPreference(ReadPreference.valueOf(p));
        } else
            collection = database.getCollection(collectionParameter).withReadPreference(ReadPreference.secondaryPreferred());
        return collection;
    }

    public static void main(String[] args) {
        Option help = Option.builder("help")
            .argName("help")
            .desc("get help")
            .build();
        Option ouri = Option.builder("uri")
            .argName("uri")
            .desc("mongodb uri, required")
            .hasArg()
            .type(String.class)
            .build();
        Option odatabase = Option.builder("database")
            .argName("database")
            .desc("mongodb database, default test")
            .hasArg()
            .type(String.class)
            .build();
        Option ocollection = Option.builder("collection")
            .argName("collection")
            .desc("mongodb collection, default test")
            .hasArg()
            .type(String.class)
            .build();
        Option osleep = Option.builder("sleep")
            .argName("sleep")
            .desc("sleep between runs, default 0 milliseconds")
            .hasArg()
            .type(Long.class)
            .build();
        Option othreads = Option.builder("threads")
            .argName("threads")
            .desc("number of threads to run, default 5")
            .hasArg()
            .type(Integer.class)
            .build();
        Option readPreference = Option.builder("readPreference")
            .argName("readPreference")
            .desc("read preference, default is secondaryPreferred")
            .hasArg()
            .type(String.class)
            .build();
        Option oids = Option.builder("ids")
            .argName("ids")
            .desc("list of comma separated ids")
            .hasArg()
            .type(String.class)
            .build();
        Option oidFile = Option.builder("idFile")
            .argName("idFile")
            .desc("file containing ids per line")
            .hasArg()
            .type(String.class)
            .build();
        Option runTimeO = Option.builder("runTime")
            .argName("runTime")
            .desc("time to run, default 5 min")
            .hasArg()
            .type(Integer.class)
            .build();

        Options options = new Options();
        options.addOption(help);
        options.addOption(ouri);
        options.addOption(odatabase);
        options.addOption(ocollection);
        options.addOption(osleep);
        options.addOption(othreads);
        options.addOption(readPreference);
        options.addOption(oids);
        options.addOption(oidFile);
        options.addOption(runTimeO);

        CommandLineParser parser = new DefaultParser();
        CommandLine cline = null;
        try {
            // parse the command line arguments
            cline = parser.parse(options, args);
        } catch (ParseException exp) {
            // oops, something went wrong
            System.err.println("Parsing failed.  Reason: " + exp.getMessage());
        }

        if (args.length == 0 || cline.hasOption("help") || !cline.hasOption("uri")) {
            printHelp(options);
        }

        final String[] ids = parseIdFile(cline);

        String uriParameter = cline.getOptionValue("uri");
        String databaseParameter = cline.getOptionValue("database", "test");
        String collectionParameter = cline.getOptionValue("collection", "test");
        System.out.println("Using database: " + databaseParameter + " and collection: " + collectionParameter);

        MongoClientOptions.Builder builder = MongoClientOptions.builder();
        builder.socketKeepAlive(true);

        MongoClient mongoClient = new MongoClient(new MongoClientURI(uriParameter, builder));

        MongoDatabase database = mongoClient.getDatabase(databaseParameter);
        final MongoCollection<Document> collection = getCollection(cline, database, collectionParameter);

        long tsleep = 0;
        if (cline.hasOption("sleep")) tsleep = Integer.parseInt(cline.getOptionValue("sleep"));
        final long sleep = tsleep;
        int threadsN = 5;
        if (cline.hasOption("threads")) threadsN = Integer.parseInt(cline.getOptionValue("threads"));
        int runTime = 5;
        if (cline.hasOption("runTime")) runTime = Integer.parseInt(cline.getOptionValue("runTime", "5"));

        LocalDateTime endDate = LocalDateTime.now(ZoneOffset.UTC);
        endDate = endDate.plus(runTime, ChronoUnit.MINUTES);

        ThroughputThread[] threads = new ThroughputThread[threadsN];
        for (int i = 0; i < threadsN; i++) {
            threads[i] = new ThroughputThread(i, collection, ids, sleep);
            threads[i].start();
        }

        File stopFile = new File("/tmp/top");
        int count = 0;
        List<Integer> opsPerSecond = new ArrayList<>();

        try { Thread.sleep(1000); } catch ( Exception e ) {}
        while ( LocalDateTime.now(ZoneOffset.UTC).compareTo(endDate) < 0 ) {
            int current = 0;
            for (ThroughputThread t : threads) {
                current += t.getCounter();
            }
            opsPerSecond.add(current - count);
            System.out.println( current - count );
            count = current;
            Throughput.STOP = stopFile.exists();
            try { Thread.sleep(1000); } catch ( Exception e ) {}
        }

        IntSummaryStatistics stats = opsPerSecond.stream().mapToInt((x) -> x).summaryStatistics();
        System.out.println( stats );

        Throughput.STOP = true;

    }

    public static class ThroughputThread extends Thread {
        private int counter = 0;
        private MongoCollection mongoCollection;
        private String[] ids;
        private long sleep;

        public ThroughputThread(int i, MongoCollection mongoCollection, String[] ids, long sleep) {
            super("ThroughputThread" + i);
            this.mongoCollection = mongoCollection;
            this.ids = ids;
            this.sleep = sleep;
        }

        public int getCounter() {
            return counter;
        }

        public void run() {
            int max = ids.length;
            try {
                for (; ; ) {
                    if (Throughput.STOP)
                        break;

                    String id = ids[(counter % max)];
                    mongoCollection.find(and(eq("_id", id))).first();
                    counter++;

                    if (sleep > 0L) {
                        try { Thread.sleep(sleep); } catch (InterruptedException e) {}
                    }
                }
            } catch (Exception e) {
                LOGGER.error("Error running thread", e);
            }
        }
    }
}
