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
import java.util.Collections;
import java.util.Date;
import java.util.List;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.bson.Document;
import org.bson.RawBsonDocument;

/**
 *
 * @author royrim
 */
public class Timings {
    
    public static void printHelp(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp( "org.mongodb.tse.tests.Timings", options );
        System.exit(0);        
    }
    
    public static String[] parseIdFile(File file) {
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
    
    public static void main(String[] args) throws ParseException {
        
        Option help = Option.builder("help")
                .argName("help")
                .desc("get help")
                .build();
        Option test = Option.builder("test")
                .argName("test")
                .desc("quick test")
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
        Option otimes = Option.builder("times")
                .argName("times")
                .desc("number of times to run, default 100")
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
        options.addOption(test);
        options.addOption(ouri);
        options.addOption(odatabase);
        options.addOption(ocollection);
        options.addOption(osleep);
        options.addOption(otimes);
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
        
        if ( cline.hasOption("test") ) {
            List<Double> testList = new ArrayList<Double>();
            for ( int i = 0; i < 100; i++ ) {
                testList.add( new Double(i) );
            }
            Collections.sort(testList);
            System.out.println(
                    String.format( "P50: %.2f, P75: %.2f, P90: %.2f, P95: %.2f, P99: %.2f", 
                            percentile(testList, 0.50),
                            percentile(testList, 0.75),
                            percentile(testList, 0.90),
                            percentile(testList, 0.95),
                            percentile(testList, 0.99) )
            );
            System.exit(0);
        }
        
        String[] ids = null;
        if ( cline.hasOption("idFile") ) {
            ids = parseIdFile( new File( cline.getOptionValue("idFile" ) ) );
        }
        else ids = cline.getOptionValue("ids", "517886481000").split(",");
        
        List<Double> timeList = new ArrayList<>();

        String uriParameter = cline.getOptionValue("uri");
        String databaseParameter = cline.getOptionValue("database", "productpersistdb");
        String collectionParameter = cline.getOptionValue("collection", "product");
        System.out.println( "Using database: " + databaseParameter + " and collection: " + collectionParameter);
        
	MongoClientURI uri = new MongoClientURI( uriParameter );
        MongoClient mongoClient = new MongoClient( uri );

        MongoDatabase database = mongoClient.getDatabase( databaseParameter );
        MongoCollection<Document> collection = null;
        MongoCollection<RawBsonDocument> rawCollection = null;
        
        boolean doDoc = cline.hasOption("doc");
        if ( doDoc ) {
            if ( cline.hasOption("readPreference") ) {
                String p = cline.getOptionValue("readPreference");
                collection = database.getCollection(collectionParameter).withReadPreference(ReadPreference.valueOf(p));
            }
            else
                collection = database.getCollection(collectionParameter).withReadPreference(ReadPreference.secondaryPreferred());
        }
        else {
            if ( cline.hasOption("readPreference") ) {
                String p = cline.getOptionValue("readPreference");
                rawCollection = database.getCollection(collectionParameter, RawBsonDocument.class).withReadPreference(ReadPreference.valueOf(p));
            }
            else
                rawCollection = database.getCollection(collectionParameter, RawBsonDocument.class).withReadPreference(ReadPreference.secondaryPreferred());
        }
        
        long sleep = 10000;
        if ( cline.hasOption("sleep") ) sleep = Integer.parseInt(cline.getOptionValue("sleep")) * 1000;
        int times = 100;
        if ( cline.hasOption("times") ) times = Integer.parseInt(cline.getOptionValue("times"));
        
        int count = 0;
        int max = ids.length;
        while ( count < times ) {
            String id = ids[ (count%max) ];
            Document doc = null;
            RawBsonDocument raw = null;
            
            Date date = new Date();
            long end = 0L;
            long start = System.nanoTime();
            if ( doDoc ) {
                doc = collection.find( eq( "_id", id ) ).first();
                end = System.nanoTime();
                if ( doc == null ) System.out.println( "Could not find " + id );
            }
            else {
                raw = rawCollection.find( eq( "_id", id ) ).first();
                end = System.nanoTime();
                if ( raw == null ) System.out.println( "Could not find " + id );
            }
            
            int size = 0;
            if ( raw != null ) size = raw.getByteBuffer().capacity();

            if ( raw != null ) {
                System.out.println( String.format("id: %s, start: %s, elasped: %s ns, docSize: %s", id, date, (end-start), size ) );
            }
            else {
                System.out.println( String.format("id: %s, start: %s, elasped: %s ns", id, date, (end-start) ) );
            }
            timeList.add( new Double( end-start ) );
            try { Thread.sleep(sleep); } catch ( InterruptedException e ) {}
            count++;
        }
        
        Collections.sort( timeList );
        
        System.out.println(
                String.format( "P50: %.2f, P75: %.2f, P90: %.2f, P95: %.2f, P99: %.2f", 
                        percentile(timeList, 0.50),
                        percentile(timeList, 0.75),
                        percentile(timeList, 0.90),
                        percentile(timeList, 0.95),
                        percentile(timeList, 0.99) )
        );
    }
    
    public static double percentile(List<Double> timeList, double percent) {
        double k = (timeList.size()-1) * percent;
        double f = Math.floor(k);
        double c = Math.ceil(k);
        if ( f == c ) {
            return timeList.get((int)k);
        }
        double d0 = timeList.get((int)f) * (c-k);
        double d1 = timeList.get((int)c) * (k-f);
        return d0 + d1;
    }
    
}
