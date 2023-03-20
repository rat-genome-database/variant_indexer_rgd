package edu.mcw.rgd.variantIndexerRgd.newtablestructure;

import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;

import java.util.concurrent.TimeUnit;

public class BulkIndexProcessor {
    public static BulkProcessor bulkProcessor=null;
    private static BulkIndexProcessor bulkIndexProcessor=null;
    private BulkIndexProcessor(){}
    public static BulkIndexProcessor getInstance(){
        if(bulkIndexProcessor==null) {
            bulkIndexProcessor = new BulkIndexProcessor();
           bulkProcessor=init();
        }
        return bulkIndexProcessor;
    }

    private static BulkProcessor init() {

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
           return BulkProcessor.builder(
                    (request, bulkListener) ->
                            ESClient.getClient().bulkAsync(request, RequestOptions.DEFAULT, bulkListener),
                    listener)
                    .setBulkActions(10000)
                    .setBulkSize(new ByteSizeValue(5, ByteSizeUnit.MB))
                    .setFlushInterval(TimeValue.timeValueSeconds(5))
                    .setConcurrentRequests(10)
                    .setBackoffPolicy(
                            BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(100), 3))
                    .build();


    }
    public  void destroy(){
        try {
            if(this.bulkProcessor!=null) {
                bulkProcessor.flush();
                bulkProcessor.awaitClose(10, TimeUnit.MINUTES);
                bulkProcessor=null;
                bulkIndexProcessor=null;

            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }finally {
            if(bulkProcessor!=null) {
                bulkProcessor.flush();
                bulkProcessor.close();
                bulkProcessor=null;
                bulkIndexProcessor=null;
            }
        }
    }
}