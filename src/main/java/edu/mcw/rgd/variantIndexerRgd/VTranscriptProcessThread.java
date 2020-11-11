package edu.mcw.rgd.variantIndexerRgd;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.mcw.rgd.process.FastaParser;
import edu.mcw.rgd.process.Utils;
import edu.mcw.rgd.variantIndexerRgd.dao.VariantLoad3;
import edu.mcw.rgd.variantIndexerRgd.model.CommonFormat2Line;
import edu.mcw.rgd.variantIndexerRgd.model.RgdIndex;
import edu.mcw.rgd.variantIndexerRgd.model.VariantTranscript;
import edu.mcw.rgd.variantIndexerRgd.process.GeneCache;
import edu.mcw.rgd.variantIndexerRgd.service.ESClient;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Created by jthota on 12/27/2019.
 */
public class VTranscriptProcessThread implements Runnable {
    private List<CommonFormat2Line> lines;
    private GeneCache geneCache;
    private int count;
    public VTranscriptProcessThread(List<CommonFormat2Line> lines, String index, GeneCache geneCache, int count){
        this.lines=lines;this.geneCache=geneCache;this.count=count;
    }
    @Override
    public void run() {
      //  System.out.println(Thread.currentThread().getName()  + " || LINE_CLUSTER: "+ count +"\tstarted .... " + new Date());
        VariantLoad3 loader=new VariantLoad3();

        FastaParser fastaFile= new FastaParser();
        //   fastaFile.setMapKey(mapKey);

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
        for(CommonFormat2Line line:lines){
            String chr = line.getChr();
            int position = line.getPos();
            String refNuc = line.getRefNuc(); // reference nucleotide for snvs (or ref sequence for indels)
            String varNuc = line.getVarNuc(); // variant nucleotide for snvs (or var sequence for indels)
            String rsId=line.getRsId();
            boolean isSnv = !Utils.isStringEmpty(refNuc) && !Utils.isStringEmpty(varNuc);

            fastaFile.setChr(chr);
      //   fastaFile.setMapKey(17, "C:/Apps/fasta/" );
         fastaFile.setMapKey(17, "/data/ref/fasta/hs37/");
            long endPos = 0;
            if (isSnv) {
                    endPos =position + 1;
                } else {
                    // insertions
                    if (Utils.isStringEmpty(refNuc)) {
                        endPos = position;
                    }
                    // deletions
                    else if (Utils.isStringEmpty(varNuc)) {
                        endPos = position + refNuc.length();
                    } else {
                        System.out.println("Unexpected var type");
                    }
                }
                if (refNuc!=null && !loader.alleleIsValid(refNuc) ) {
                 //   System.out.println(" *** Ref Nucleotides must be A,C,G,T,N");
                   continue;
                }
                if (varNuc!=null && !loader.alleleIsValid(varNuc) ) {
               //     System.out.println(" *** Var Nucleotides must be A,C,G,T,N");
                    continue;
                }

            try {
              //  loader.indexTranscripts(loader.processVariantTranscript(chr,position ,(int)endPos, refNuc, varNuc, 17, geneCache, rsId, fastaFile));
               List<VariantTranscript> vts=loader.processVariantTranscript(chr,position ,(int)endPos, refNuc, varNuc, 17, geneCache, rsId, fastaFile);
                for (VariantTranscript o : vts) {

                    ObjectMapper mapper = new ObjectMapper();
                    String json = new String();
                        json = mapper.writeValueAsString(o);
                        bulkProcessor.add(new IndexRequest(RgdIndex.getNewAlias()).source(json, XContentType.JSON));


                }

            } catch (Exception e) {
                e.printStackTrace();
            }

        }

        try {
            bulkProcessor.awaitClose(10, TimeUnit.MINUTES);
            bulkProcessor.close();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }finally {
            bulkProcessor.close();
        }
   //     System.out.println("***********"+Thread.currentThread().getName()  + " || LINE_CLUSTER: "+ count+"\tEND ....INDEXED TRANSCRIPTS:  "+"\t"+ new Date()+"*********");
    }
}
