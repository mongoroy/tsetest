/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.mongodb.tse.tests;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.ReadPreference;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import static com.mongodb.client.model.Filters.eq;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.bson.Document;
import org.bson.RawBsonDocument;
import static org.mongodb.tse.tests.Timings.parseIdFile;

/**
 *
 * @author royrim
 */
public class RunQueryThreadPool {
    
    
    public static void printHelp(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp( "org.mongodb.tse.tests.RunQueryThreadPool", options );
        System.exit(0);        
    }
    
    public static String[] parseIdFile(CommandLine cline) {
        if ( cline.hasOption("idFile") ) {
            File file = new File( cline.getOptionValue("idFile" ) );
            List<String> list = new ArrayList<>();
            BufferedReader reader = null;
            try {
                reader = new BufferedReader( new FileReader( file ) );
                String line = null;
                while ( ( line = reader.readLine() ) != null ) {
                    list.add(line);
                }
            }
            catch ( Exception e ) {
                System.err.println( "Error with your id file" );
                e.printStackTrace();
                System.exit(1);
            }
            return list.toArray(new String[]{});
        }
        else return cline.getOptionValue("ids", "517886481000").split(",");
        
    }
    
    public static MongoCollection getCollection(CommandLine cline, MongoDatabase database, String collectionParameter) {
        MongoCollection collection = null;
        if ( cline.hasOption("readPreference") ) {
            String p = cline.getOptionValue("readPreference");
            collection = database.getCollection(collectionParameter).withReadPreference(ReadPreference.valueOf(p));
        }
        else
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
                .desc("mongodb database, default productpersistdb")
                .hasArg()
                .type(String.class)
                .build();
        Option ocollection = Option.builder("collection")
                .argName("collection")
                .desc("mongodb collection, default product")
                .hasArg()
                .type(String.class)
                .build();
        Option osleep = Option.builder("sleep")
                .argName("sleep")
                .desc("sleep between runs, default 10 seconds")
                .hasArg()
                .type(Integer.class)
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
        Option odoc = Option.builder("doc")
                .argName("doc")
                .desc("get a Document instead of RawBsonDocument, no size output with this option")
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
        options.addOption(odoc);
        
        CommandLineParser parser = new DefaultParser();
        CommandLine cline = null;
        try {
            // parse the command line arguments
            cline = parser.parse( options, args );
        }
        catch( ParseException exp ) {
            // oops, something went wrong
            System.err.println( "Parsing failed.  Reason: " + exp.getMessage() );
        }
        
        if ( args.length == 0 || cline.hasOption("help") || !cline.hasOption("uri") ) {
            printHelp(options);
        }

        final String[] ids = parseIdFile( cline );        
        
        String uriParameter = cline.getOptionValue("uri");
        String databaseParameter = cline.getOptionValue("database", "productpersistdb");
        String collectionParameter = cline.getOptionValue("collection", "product");
        System.out.println( "Using database: " + databaseParameter + " and collection: " + collectionParameter);
        
	MongoClientURI uri = new MongoClientURI( uriParameter );
        MongoClient mongoClient = new MongoClient( uri );

        MongoDatabase database = mongoClient.getDatabase( databaseParameter );
        final MongoCollection<Document> collection = getCollection(cline, database, collectionParameter);

        
        long tsleep = 10000;
        if ( cline.hasOption("sleep") ) tsleep = Integer.parseInt(cline.getOptionValue("sleep")) * 1000;
        final long sleep = tsleep;
        int threads = 5;
        if ( cline.hasOption("threads") ) threads = Integer.parseInt(cline.getOptionValue("threads"));
        
        int max = ids.length;
        
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        for ( int i = 0; i < threads; i++ ) {
            pool.execute( new Runnable() {
                public void run() {
                    int count = 0;
                    for ( ; ; ) {
                        String id = ids[ (count%max) ];
                        Document doc = null;
                        RawBsonDocument raw = null;

                        Date date = new Date();
                        long end = 0L;
                        long start = System.nanoTime();
                        doc = collection.find( eq( "_id", id ) ).first();
                        end = System.nanoTime();
                        if ( doc == null ) System.out.println( "Could not find " + id );

                        int size = 0;
                        System.out.println( String.format("%s: ,id: %s, start: %s, elasped: %s ns", Thread.currentThread().getName(), id, date, (end-start) ) );
                        try { Thread.sleep(sleep); } catch ( InterruptedException e ) {}
                        count++;
                        
                    }
                    
                }
            });
        }
        
    }
    
}
