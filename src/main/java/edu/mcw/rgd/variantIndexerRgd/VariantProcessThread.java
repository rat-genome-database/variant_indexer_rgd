package edu.mcw.rgd.variantIndexerRgd;

import com.fasterxml.jackson.databind.ObjectMapper;

import edu.mcw.rgd.variantIndexerRgd.model.*;
import edu.mcw.rgd.variantIndexerRgd.process.GeneCache;

import edu.mcw.rgd.variantIndexerRgd.service.ESClient;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;

import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;

import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.*;

import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;


/**
 * Created by jthota on 12/29/2019.
 */
public class VariantProcessThread implements Runnable {

/*  private List<String> lines;
    private GeneCache geneCache;
    private int strainCount;
    private String[] header;
    private String processName;*/
    private List<VariantIndexObject> indexObjects;
    int clusterCount;

    public VariantProcessThread(List<VariantIndexObject> indexObjects, int strainCount, String[] header, GeneCache geneCache, String processName, int clusterCount){
      //  this.lines=lines;
     /*   this.geneCache=geneCache;
        this.strainCount=strainCount;
        this.header=header;
        this.processName=processName;
        this.clusterCount=clusterCount;*/
        this.indexObjects=indexObjects;
    }

 //   public void run(GeneCache geneCache, File file, String processName) throws IOException {
    public void run(){
      System.out.println(Thread.currentThread().getName()  + " || LINE_CLUSTER : "+ clusterCount+" started .... " + new Date());

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
            for(VariantIndexObject obj:indexObjects){
                try {
                     String json =  mapper.writeValueAsString(obj);
                    bulkProcessor.add(new IndexRequest(RgdIndex.getNewAlias()).source(json, XContentType.JSON));

                } catch (IOException e) {
                    e.printStackTrace();
                }
            }


    //    reader.close();
        try {
            bulkProcessor.awaitClose(10, TimeUnit.MINUTES);
            bulkProcessor.close();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }finally {
            bulkProcessor.close();
        }
   //   System.out.println("***********"+Thread.currentThread().getName()+ "\tLINE_CLUSTER:"+clusterCount + "\tEND ...."+"\t"+ new Date()+"*********");


    }
}
