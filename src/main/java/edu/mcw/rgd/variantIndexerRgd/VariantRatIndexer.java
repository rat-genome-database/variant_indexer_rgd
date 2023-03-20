package edu.mcw.rgd.variantIndexerRgd;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.mcw.rgd.variantIndexerRgd.model.RgdIndex;
import edu.mcw.rgd.variantIndexerRgd.model.VariantIndex;
import org.elasticsearch.action.bulk.*;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.XContentType;


import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Created by jthota on 1/16/2020.
 */
public class VariantRatIndexer implements Runnable {
    private int mapKey;
    private int speciesTypeKey;
    private String chromosome;
    private int sampleId;
    List<VariantIndex> vrs;
    public VariantRatIndexer(int sampleId, String chr, int mapKey, int speciesTypeKey, List<VariantIndex> vrs){
        this.mapKey=mapKey;
        this.speciesTypeKey=speciesTypeKey;
        this.chromosome=chr;
        this.sampleId=sampleId;
        this.vrs=vrs;
    }
    @Override
    public void run() {
     //   VariantDao variantDao= new VariantDao();
     //   List<VariantIndex> vrs=variantDao.getVariantResults(sampleId, chromosome, mapKey);
      //   System.out.println("Variants Size:"+vrs.size()+"\tMapKey:"+mapKey+"\tChr:"+chromosome+"\tSampleId:"+sampleId );
        if(vrs.size()>0){
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
            BulkProcessor bulkProcessor = BulkProcessor.builder(
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

            ObjectMapper mapper = new ObjectMapper();
            for (VariantIndex o : vrs) {
                byte[] json = new byte[0];
                try {
                    json = mapper.writeValueAsBytes(o);
                } catch (JsonProcessingException e) {
                    e.printStackTrace();
                }

                bulkProcessor.add(new IndexRequest(RgdIndex.getNewAlias()).source(json, XContentType.JSON));


            }

            try {
                bulkProcessor.awaitClose(10, TimeUnit.MINUTES);
                bulkProcessor.close();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }finally {
                bulkProcessor.close();
            }
            System.out.println("Indexed mapKey " + mapKey + ", chromosome: "+ chromosome+", Variant objects Size: " + vrs.size() + " Exiting thread.");
            System.out.println(Thread.currentThread().getName() + ": VariantThread" + mapKey +"\tSample: "+sampleId+"\tChromosome: "+chromosome+ " End " + new Date());
        }
    }
}
