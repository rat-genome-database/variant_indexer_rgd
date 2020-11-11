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
    private List<VariantObject> vmdList;
    List<VariantSampleDetail> samples;
    VariantDao dao=new VariantDao();
    public ProcessChromosome(){}
    public ProcessChromosome(String chr, int mapKey, int speciesTypeKey,  Map<Long, List<String>> geneLociMap, List<VariantObject> vmdList ){
        this.chr=chr;
        this.mapKey=mapKey;
        this.speciesTypeKey=speciesTypeKey;
        this.geneLociMap=geneLociMap;
        this.vmdList=vmdList;
       // this.samples=samples;
    }
    @Override
    public void run() {
        System.out.println(Thread.currentThread().getName()+ "\tCHR:"+ chr +"\t started....");
        Set<Long> variantIds=new HashSet<>();
        List<VariantIndex> indexList=new ArrayList<>();
        try {
            System.out.println("Variants of CHR-"+ chr+":" +vmdList.size() );
            int count=0;
            ExecutorService executor = new MyThreadPoolExecutor(10, 10, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
            for (VariantObject vmd : vmdList) {
                if(!variantIds.contains(vmd.getId())) {
                    variantIds.add(vmd.getId());
                    VariantIndex vi =null;
                    boolean first=true;
                    for (VariantObject obj : vmdList) {
                        if (obj.getId() == vmd.getId()) {
                            if(first){
                               vi= new VariantIndex();
                                mapVariantDetials(vi, obj);
                                first=false;
                            }
                            mapTranscriptsNPolyphen(obj, vi);
                            addConservationScores(obj, vi);
                            mapRegion(vmd, vi, geneLociMap);
                        }
                    }
                    Runnable workerThread=new MapSamplesAndIndex(vmd,vi);
                    executor.execute(workerThread);
                         /*   indexList.add(vi);
                            if(indexList.size()>=1000){
                                 //   Runnable workerThread=new MapSamplesAndIndex(vmd,vi)
                                    Runnable workerThread=new MapSamplesAndIndex(indexList,variantIds);
                                    executor.execute(workerThread);
                                    variantIds=new HashSet<>();
                                    indexList=new ArrayList<>();

                                }
*/
                       /*  List<VariantSampleDetail> samples = dao.getSamples(vmd.getId());
                            for (VariantSampleDetail vsd : samples) {
                                if (vi != null) {
                                    mapSampleDetails(vsd, vi);

                                try {
                                //    System.out.println(vi.toString());
                                    ObjectMapper mapper=new ObjectMapper();
                                    String json =  mapper.writeValueAsString(vi);
                                 BulkIndexProcessor.getBulkProcessor().add(new IndexRequest(RgdIndex.getNewAlias()).source(json, XContentType.JSON));
                                // IndexRequest request=  new IndexRequest(RgdIndex.getNewAlias()).source(vi.toString(), XContentType.JSON);
                                //    ESClient.getClient().index(request, RequestOptions.DEFAULT);
                                } catch (Exception e) {
                                    e.printStackTrace();
                                }
                                }
                            }*/
                        }
                        count++;

                        }
         /* if(indexList.size()>0) {
                Runnable workerThread = new MapSamplesAndIndex(indexList, variantIds);
                executor.execute(workerThread);
                variantIds = new HashSet<>();
                indexList = new ArrayList<>();
            }*/
            executor.shutdown();
            while (!executor.isTerminated()) {}
        } catch (Exception e) {
            e.printStackTrace();
        }

        System.out.println(Thread.currentThread().getName()+ "\tCHR:"+ chr +"\t END");

    }
   void mapRegion(VariantObject vmd, VariantIndex vi,Map<Long, List<String>> geneLociMap){
       vi.setRegionName(geneLociMap.get(vmd.getStartPos()));
        List<String> regionNameLc=new ArrayList<>();
       try {
           for (String s : geneLociMap.get(vmd.getStartPos())) {
               regionNameLc.add(s.toLowerCase());
           }
       }catch (Exception e){
           System.out.println(vmd.getStartPos());
           e.printStackTrace();
       }
        vi.setRegionNameLc(regionNameLc);
   }
    void mapVariantDetials(VariantIndex vi, VariantObject vmd){
        vi.setVariant_id(vmd.getId());
        vi.setChromosome(vmd.getChromosome());
        vi.setStartPos(vmd.getStartPos());
        vi.setEndPos(vmd.getEndPos());
        vi.setRefNuc(vmd.getReferenceNucleotide());
        vi.setVarNuc(vmd.getVariantNucleotide());
        vi.setMapKey(vmd.getMapKey());
        vi.setClinvarId(vmd.getClinvarId());
        vi.setRsId(vmd.getRsId());
        vi.setGenicStatus(vmd.getGenicStatus());
        vi.setPaddingBase(vmd.getPaddingBase());
        vi.setVariantType(vmd.getVariantType());
    }
    void mapTranscriptsNPolyphen(VariantObject v, VariantIndex vi) throws Exception {
     /*  List<VariantTranscript> vts=  vdao.getVariantTranscripts(vmd.getId());
        List<Polyphen> pps= vdao.getPolyphen(vmd.getId());
        for(VariantTranscript vt:vts){
            for(Polyphen p:pps){
                if(p.getTranscriptRgdid()==vt.getTranscriptRgdId()){
                    vt.setPolyphenStatus(p.getPrediction());
                }
            }
        }*/
     List<VariantTranscript> vts=new ArrayList<>();
     List<VariantTranscript> transcripts=vi.getVariantTranscripts();
     if(transcripts!=null && transcripts.size()>0){
         vts.addAll(transcripts);
         for(VariantTranscript transcript:transcripts){
             if(transcript.getTranscriptRgdId()!=v.getTranscriptRgdId()){
                 vts.add(getTranscriptObject(v));
             }
         }
     }else {
         vts.add(getTranscriptObject(v));

     }
        vi.setVariantTranscripts(vts);
    }
    VariantTranscript getTranscriptObject(VariantObject v){
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
    void addConservationScores(VariantObject v, VariantIndex vi) throws Exception {
     /*   String tableName=getConScoreTable(mapKey, vmd.getGenicStatus());
        List<ConservationScore> conscores=vdao.getConservationScores(vmd.getStartPos(), chr,tableName);
        List<BigDecimal> scores= new ArrayList<>();
        for(ConservationScore cs:conscores){
            scores.add(cs.getScore());
        }*/
     if(v.getConScore()!=null) {
         List<BigDecimal> scores = new ArrayList<>();
         scores = vi.getConScores();
         if (scores != null && scores.size() > 0) {
             for (BigDecimal s : scores) {
                 if (s != null && !s.equals(v.getConScore())) {
                     scores.add(v.getConScore());
                 }
             }
         } else {
             scores = new ArrayList<>();
             scores.add(v.getConScore());
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
    void mapSampleDetails(VariantSampleDetail vsd, VariantIndex vi){
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
