package edu.mcw.rgd.variantIndexerRgd;

import co.elastic.clients.elasticsearch._helpers.bulk.BulkIngester;
import edu.mcw.rgd.variantIndexerRgd.model.VariantIndex;
import edu.mcw.rgd.variantIndexerRgd.newtablestructure.BulkIndexProcessor;

import java.util.Date;
import java.util.List;

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
            BulkIngester<Void> bulkProcessor = BulkIndexProcessor.newIngester(1);

            try {
                for (VariantIndex o : vrs) {
                    bulkProcessor.add(BulkIndexProcessor.indexOp(o));
                }
            } finally {
                bulkProcessor.close();
            }
            System.out.println("Indexed mapKey " + mapKey + ", chromosome: "+ chromosome+", Variant objects Size: " + vrs.size() + " Exiting thread.");
            System.out.println(Thread.currentThread().getName() + ": VariantThread" + mapKey +"\tSample: "+sampleId+"\tChromosome: "+chromosome+ " End " + new Date());
        }
    }
}
