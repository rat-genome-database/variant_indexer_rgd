package edu.mcw.rgd.variantIndexerRgd.newtablestructure;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.mcw.rgd.dao.spring.ConservationScoreMapper;
import edu.mcw.rgd.dao.spring.variants.Polyphen;
import edu.mcw.rgd.datamodel.ConservationScore;
import edu.mcw.rgd.datamodel.prediction.PolyPhenPrediction;
import edu.mcw.rgd.datamodel.variants.VariantMapData;
import edu.mcw.rgd.datamodel.variants.VariantObject;
import edu.mcw.rgd.datamodel.variants.VariantSampleDetail;
import edu.mcw.rgd.datamodel.variants.VariantTranscript;
import edu.mcw.rgd.variantIndexerRgd.dao.VariantDao;
import edu.mcw.rgd.variantIndexerRgd.model.RgdIndex;
import edu.mcw.rgd.variantIndexerRgd.model.VariantIndex;
import edu.mcw.rgd.variantIndexerRgd.process.MyThreadPoolExecutor;
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

import java.io.IOException;
import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static edu.mcw.rgd.variantIndexerRgd.Manager.getGeneLociMap;

public class ProcessChromosome implements Runnable {
    private String chr;
    private int mapKey;
    private int speciesTypeKey;
    private Map<Long, List<String>> geneLociMap;
    private List<VariantIndex> vmdList;
    private VariantIndex vi;
    int sampleId;
    VariantDao dao=new VariantDao();
    public ProcessChromosome(){}
    public ProcessChromosome(String chr, int mapKey, int speciesTypeKey,  Map<Long, List<String>> geneLociMap, VariantIndex vi, int sampleId ){
        this.chr=chr;
        this.mapKey=mapKey;
        this.speciesTypeKey=speciesTypeKey;
        this.geneLociMap=geneLociMap;
        this.vi=vi;
        this.sampleId=sampleId;
    }
   @Override
    public void run() {

        System.out.println(Thread.currentThread().getName()+ "\tCHR:"+ chr +"\t started....");

        try {
          //  System.out.println("Variants of CHR-"+ chr+": Sample: "+ sampleId+": " +vmdList.size() );
          //  ExecutorService executor = new MyThreadPoolExecutor(10, 10, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
                mapRegion(vi, geneLociMap);
                mapTranscriptsNPolyphen(vi);
                addConservationScores(vi);

                    try {
                        ObjectMapper mapper = new ObjectMapper();
                        String json = mapper.writeValueAsString(vi);
                     //   BulkIndexProcessor.getBulkProcessor().add(new IndexRequest(RgdIndex.getNewAlias()).source(json, XContentType.JSON));
                         IndexRequest request=  new IndexRequest(RgdIndex.getNewAlias()).source(json, XContentType.JSON);
                           ESClient.getClient().index(request, RequestOptions.DEFAULT);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

        //    executor.shutdown();
        //    while (!executor.isTerminated()) {}
        } catch (Exception e) {
            e.printStackTrace();
        }

        System.out.println(Thread.currentThread().getName()+ "\tCHR:"+ chr +"\t END");

    }
 public  void mapRegion( VariantIndex vi,Map<Long, List<String>> geneLociMap){
       vi.setRegionName(geneLociMap.get(vi.getStartPos()));
        List<String> regionNameLc=new ArrayList<>();
       try {
           for (String s : geneLociMap.get(vi.getStartPos())) {
               regionNameLc.add(s.toLowerCase());
           }
       }catch (Exception e){
           System.out.println(vi.getStartPos());
           e.printStackTrace();
       }
        vi.setRegionNameLc(regionNameLc);
   }

   public void mapTranscriptsNPolyphen(VariantIndex vi) throws Exception {
       vi.setVariantTranscripts(dao.getVariantTranscripts(vi.getVariant_id()));

    }
   public VariantTranscript getTranscriptObject(VariantObject v){
        VariantTranscript t=new VariantTranscript();
        t.setFullRefAAPos(v.getFullRefAAPos());
        t.setFullRefNucPos(v.getFullRefNucPos());
        t.setTranscriptRgdId(v.getTranscriptRgdId());
        t.setRefAA(v.getRefAA());
        t.setVarAA(v.getVarAA());
        t.setLocationName(v.getLocationName());
        t.setSynStatus(v.getSynStatus());
        t.setNearSpliceSite(v.getNearSpliceSite());
        t.setFullRefNucSeqKey(v.getFullRefNucSeqKey());
        t.setFullRefAASeqKey(v.getFullRefAASeqKey());
        t.setTripletError(v.getTripletError());
        t.setFrameShift(v.getFrameShift());
        t.setPolyphenStatus(v.getPolyphenStatus());
        return t;
    }
   public void addConservationScores(VariantIndex vi) throws Exception {
    /* if(v.getConScore()!=null) {
         List<String> scores = new ArrayList<>();
         scores = vi.getConScores();
         if (scores != null && scores.size() > 0) {
             for (String s : scores) {
                 if (s != null && !s.equals(String.valueOf(v.getConScore()))) {
                     scores.add(String.valueOf(v.getConScore()));
                 }
             }
         } else {
             scores = new ArrayList<>();
             scores.add(String.valueOf(v.getConScore()));
         }
         vi.setConScores(scores);
    // }*/
       List<String> scores=new ArrayList<>();
       List<ConservationScore> conScores=   dao.getConservationScores(vi.getStartPos(),vi.getChromosome(), getConScoreTable(vi.getMapKey(),""));
       if (conScores != null && conScores.size() > 0) {
           for (ConservationScore s : conScores) {
               if (s != null ) {
                   scores.add(String.valueOf(s.getScore()));
               }
           }
           vi.setConScores(scores);
       }

    }
    public String getConScoreTable(int mapKey, String genicStatus ) {
        switch(mapKey) {
            case 17:
                return " B37_CONSCORE_PART_IOT ";
            case 38:
                return " CONSERVATION_SCORE_HG38 ";
            case 60:
                if (genicStatus.equalsIgnoreCase("GENIC")) {
                    return " CONSERVATION_SCORE_GENIC ";
                }

                return " CONSERVATION_SCORE ";
            case 70:
                return " CONSERVATION_SCORE_5 ";
            case 360:
                return " CONSERVATION_SCORE_6 ";
            default:
                return " CONSERVATION_SCORE_6 ";
        }
    }
   public void mapSampleDetails(VariantSampleDetail vsd, VariantIndex vi){
        vi.setSampleId(vsd.getSampleId());
        vi.setAnalysisName(vsd.getAnalysisName());
        vi.setZygosityInPseudo(vsd.getZygosityInPseudo());
        vi.setZygosityNumAllele(vsd.getZygosityNumberAllele());
        vi.setZygosityPercentRead(vsd.getZygosityPercentRead());
        vi.setZygosityPossError(vsd.getZygosityPossibleError());
        vi.setZygosityStatus(vsd.getZygosityStatus());
        vi.setQualityScore(vsd.getQualityScore());
        vi.setTotalDepth(vsd.getDepth());
        vi.setVarFreq(vsd.getVariantFrequency());

    }
}
