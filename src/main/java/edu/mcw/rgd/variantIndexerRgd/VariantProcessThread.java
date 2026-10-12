package edu.mcw.rgd.variantIndexerRgd;

import co.elastic.clients.elasticsearch._helpers.bulk.BulkIngester;
import edu.mcw.rgd.variantIndexerRgd.model.*;
import edu.mcw.rgd.variantIndexerRgd.newtablestructure.BulkIndexProcessor;
import edu.mcw.rgd.variantIndexerRgd.process.GeneCache;

import java.util.Date;
import java.util.List;


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

        BulkIngester<Void> bulkProcessor = BulkIndexProcessor.newIngester(1);

        try {
            for(VariantIndexObject obj:indexObjects){
                bulkProcessor.add(BulkIndexProcessor.indexOp(obj));
            }
        } finally {
            bulkProcessor.close();
        }
   //   System.out.println("***********"+Thread.currentThread().getName()+ "\tLINE_CLUSTER:"+clusterCount + "\tEND ...."+"\t"+ new Date()+"*********");


    }
}
