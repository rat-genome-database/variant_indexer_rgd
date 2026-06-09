package edu.mcw.rgd.variantIndexerRgd.newtablestructure;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._helpers.bulk.BulkIngester;
import co.elastic.clients.elasticsearch._helpers.bulk.BulkListener;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.elastic.clients.util.BinaryData;
import com.fasterxml.jackson.core.JsonProcessingException;
import edu.mcw.rgd.datamodel.RgdIndex;
import edu.mcw.rgd.services.ClientInit;
import edu.mcw.rgd.variantIndexerRgd.model.Json;

import java.net.UnknownHostException;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class BulkIndexProcessor {
    public static BulkIngester<Void> bulkProcessor=null;
    private static BulkIndexProcessor bulkIndexProcessor=null;
    private BulkIndexProcessor(){}
    public static BulkIndexProcessor getInstance(){
        if(bulkIndexProcessor==null) {
            bulkIndexProcessor = new BulkIndexProcessor();
           bulkProcessor=init();
        }
        return bulkIndexProcessor;
    }
    public static BulkIngester<Void> getBulkProcessor(){
        return bulkProcessor;
    }

    private static BulkIngester<Void> init() {

            System.out.println("CREATING NEW BULK PROCESSOR....");
            BulkListener<Void> listener = new BulkListener<>() {
                @Override
                public void beforeBulk(long executionId, BulkRequest request, List<Void> contexts) {
                    //        System.out.println("ACTIONS: "+request.operations().size());
                }

                @Override
                public void afterBulk(long executionId, BulkRequest request, List<Void> contexts, BulkResponse response) {
                    //     System.out.println("in process...");
                }

                @Override
                public void afterBulk(long executionId, BulkRequest request, List<Void> contexts, Throwable failure) {

                }
            };
            ElasticsearchClient esClient;
            try {
                esClient = ClientInit.getClient();
            } catch (UnknownHostException e) {
                throw new RuntimeException(e);
            }
            return BulkIngester.of(b -> b
                    .client(esClient)
                    .maxOperations(10000)
                    .maxSize(5L * 1024 * 1024)
                    .flushInterval(5, TimeUnit.SECONDS)
                    .maxConcurrentRequests(10)
                    .listener(listener));


    }

    /**
     * Build a bulk index operation for the given document against the current new alias.
     * The document is serialized to JSON eagerly (so that callers may safely reuse/mutate
     * the source object after adding the operation) and sent as raw JSON via {@link BinaryData}.
     */
    public static BulkOperation indexOp(Object document){
        byte[] json;
        try {
            json = Json.serializer().mapper().writeValueAsBytes(document);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        BinaryData data = BinaryData.of(json, "application/json");
        return BulkOperation.of(b -> b.index(i -> i
                .index(RgdIndex.getNewAlias())
                .document(data)));
    }

    /** Index a document through the shared bulk ingester. */
    public static void index(Object document){
        bulkProcessor.add(indexOp(document));
    }

    /** Create a standalone bulk ingester (used by one-off indexing tasks that manage their own lifecycle). */
    public static BulkIngester<Void> newIngester(int concurrentRequests){
        ElasticsearchClient esClient;
        try {
            esClient = ClientInit.getClient();
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
        return BulkIngester.of(b -> b
                .client(esClient)
                .maxOperations(10000)
                .maxSize(5L * 1024 * 1024)
                .flushInterval(5, TimeUnit.SECONDS)
                .maxConcurrentRequests(concurrentRequests));
    }

    public  void destroy(){
        if(bulkProcessor!=null) {
            try {
                bulkProcessor.close();
            } finally {
                bulkProcessor = null;
                bulkIndexProcessor = null;
            }
        }
    }
}
