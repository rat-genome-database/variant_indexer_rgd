package edu.mcw.rgd.variantIndexerRgd.newtablestructure;

import edu.mcw.rgd.variantIndexerRgd.service.ESClient;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;

import java.util.concurrent.TimeUnit;

public class BulkIndexProcessor {
    public static BulkProcessor bulkProcessor=null;
    public static BulkProcessor getBulkProcessor(){
        if(bulkProcessor==null){
            init();
        }
        return bulkProcessor;
    }
    public static void init() {
        if (bulkProcessor == null) {
            System.out.println("CREATING NEW BULK PROCESSOR....");
            BulkProcessor.Listener listener = new BulkProcessor.Listener() {
                @Override
                public void beforeBulk(long executionId, BulkRequest request) {
                    //        System.out.println("ACTIONS: "+request.numberOfActions());
                }

                @Override
                public void afterBulk(long executionId, BulkRequest request,
                                      BulkResponse response) {
                    //     System.out.println("in process...");
                }

                @Override
                public void afterBulk(long executionId, BulkRequest request,
                                      Throwable failure) {

                }
            };
            bulkProcessor= BulkProcessor.builder(
                    (request, bulkListener) ->
                            ESClient.getClient().bulkAsync(request, RequestOptions.DEFAULT, bulkListener),
                    listener)
                    .setBulkActions(10000)
                    .setBulkSize(new ByteSizeValue(5, ByteSizeUnit.MB))
                    .setFlushInterval(TimeValue.timeValueSeconds(5))
                    .setConcurrentRequests(1)
                    .setBackoffPolicy(
                            BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(100), 3))
                    .build();
        }

    }
    public static void destroy(){
        try {
            bulkProcessor.awaitClose(10, TimeUnit.MINUTES);
            if(bulkProcessor!=null)
            bulkProcessor.close();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }finally {
            if(bulkProcessor!=null)
            bulkProcessor.close();
        }
    }
}