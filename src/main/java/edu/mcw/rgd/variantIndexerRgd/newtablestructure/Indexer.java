package edu.mcw.rgd.variantIndexerRgd.newtablestructure;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.mcw.rgd.datamodel.variants.VariantTranscript;
import edu.mcw.rgd.variantIndexerRgd.model.RgdIndex;
import edu.mcw.rgd.variantIndexerRgd.model.VariantData;
import edu.mcw.rgd.variantIndexerRgd.model.VariantIndex;
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

import java.math.BigDecimal;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

public class Indexer implements Runnable{
    private List<VariantData> vrs;
    private Map<Long, List<String>> geneLoci;
    private int mapKey;
    //private int sampleId;
    String chromosome;
    public Indexer(){}
    public Indexer(List<VariantData> vrs,  Map<Long, List<String>> geneLoci, int mapKey,String chromosome){
        this.vrs=vrs;
        this.geneLoci=geneLoci;
        this.mapKey=mapKey;
        this.chromosome=chromosome;
      //  this.sampleId=sampleId;
    }
    @Override
    public void run() {
        Map<Integer, VariantIndex> processedMap=new HashMap<>();
        for(VariantData vd: vrs){
            VariantIndex v =null;
            if(processedMap.get(vd.getVariantRgdId())==null){
                v=mapVariant(vd);
                processedMap.put(vd.getVariantRgdId(), v);
            }else{
                v =processedMap.get(vd.getVariantRgdId());
                List<VariantTranscript> transcripts = new ArrayList<>(v.getVariantTranscripts());
                if(!transcriptExists( vd.getTranscriptRgdId(),v.getVariantTranscripts())){
                  VariantTranscript t=mapTranscriptObject(vd);
                  transcripts.add(t);
                    v.setVariantTranscripts(transcripts);
                    processedMap.put(vd.getVariantRgdId(), v);
                }
            }
        }
        System.out.println("VARIANTS PROCESSED:"+ processedMap.size());
        index(processedMap);
        System.out.println(Thread.currentThread().getName()+ "\tMapKey:"+mapKey + "\tchromosome:"+chromosome+  "\tEND ....");

    }
    void index(Map<Integer, VariantIndex>  processedMap){
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

                for(Entry entry:processedMap.entrySet()){
                    VariantIndex object= (VariantIndex) entry.getValue();
                   // System.out.println(object.getChromosome()+"\t"+object.getStartPos());
                    try {
                        ObjectMapper mapper=new ObjectMapper();
                        byte[] json =  mapper.writeValueAsBytes(object);
                        bulkProcessor.add(new IndexRequest(RgdIndex.getNewAlias()).source(json, XContentType.JSON));
                    } catch (JsonProcessingException e) {
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

    }
     VariantIndex mapVariant(VariantData vd){
        VariantIndex v=new VariantIndex();
        v.setVariant_id(vd.getVariantRgdId());
        v.setRefNuc(vd.getRefNuc());
        v.setVarNuc(vd.getVarNuc());
        v.setStartPos(vd.getStartPos());
        v.setEndPos(vd.getEndPos());
        v.setChromosome(vd.getChromosome());
        v.setZygosityStatus(vd.getZygosityStatus());
        v.setZygosityRefAllele(vd.getZygosityRefAllele());
        v.setZygosityPossError(vd.getZygosityPossError());
        v.setZygosityPercentRead(vd.getZygosityPercentRead());
        v.setZygosityNumAllele(vd.getZygosityNumAllele());
        v.setZygosityInPseudo(vd.getZygosityInPseudo());
        v.setVarFreq(vd.getVarFreq());
        v.setTotalDepth(vd.getTotatlDepth());
        v.setSampleId(vd.getSampleId());
        v.setQualityScore(vd.getQualityScore());
        v.setVariantType(vd.getVariantTpe());
        v.setPaddingBase(vd.getPaddingBase());
        v.setGenicStatus(vd.getGenicStatus());
        v.setMapKey(vd.getMapKey());
        v.setAnalysisName(vd.getLocationName());
        v.setGender(vd.getChromosome());
       // v.setHGVSNAME();
        v.setRsId(vd.getRsId());
        v.setClinvarId(vd.getClinvarId());
        List<BigDecimal> conScores= new ArrayList<>();
        conScores.add(vd.getConservationScore());
        if(conScores.size()>0)
        v.setConScores(conScores);
        v.setRegionName(getRegionNames(vd.getStartPos()));
        v.setRegionNameLc(getRegionNamesLC(vd.getStartPos()));
        VariantTranscript vt=mapTranscriptObject(vd);
        List<VariantTranscript> vts=new ArrayList<>(Arrays.asList(vt));
        if(vts.size()>0)
         v.setVariantTranscripts(vts);
       return v;
    }
    VariantTranscript mapTranscriptObject(VariantData vd){
        VariantTranscript t=new VariantTranscript();
        t.setPolyphenStatus(vd.getPolyphenPrediction());
        t.setSynStatus(vd.getSynStatus());
        t.setLocationName(vd.getLocationName());
        t.setFullRefAAPos(vd.getFulRefAAPos());
        t.setFullRefNucPos(vd.getFullRefNucPos());
        t.setFullRefNucSeqKey(vd.getFullRefNucSeqKey());
        t.setFullRefAASeqKey(vd.getFullRefAASeqKey());
        t.setTranscriptRgdId(vd.getTranscriptRgdId());
        t.setFrameShift(vd.getFrameShift());
        t.setTripletError(vd.getTripletError());
        t.setNearSpliceSite(vd.getNearSpliceSite());
        t.setVarAA(vd.getVarAA());
        t.setRefAA(vd.getRefAA());

        return t;
    }
    boolean transcriptExists(int transcriptRgdId, List<VariantTranscript> vts){
        boolean flag=false;
        for(VariantTranscript t:vts){
            if(transcriptRgdId==t.getTranscriptRgdId()){
                flag=true;
                break;
            }
        }
        return flag;
    }
   List<String> getRegionNames(long startPos){
        List<String> regionNames= new ArrayList<>();
               regionNames= geneLoci.get(startPos);

           return regionNames;

    }
     List<String> getRegionNamesLC(long startPos){
        List<String> regionNames=geneLoci.get(startPos);
        List<String> regionNameLC = new ArrayList<>();
        if(regionNames!=null && regionNames.size()!=0) {
            for (String name : regionNames) {
                regionNameLC.add(name.toLowerCase());
            }

        }
        return regionNameLC;
    }

}
