package edu.mcw.rgd.variantIndexerRgd.newtablestructure;

/**
 * Created by jthota on 11/5/2020.
 */

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.mcw.rgd.datamodel.variants.VariantObject;
import edu.mcw.rgd.datamodel.variants.VariantSampleDetail;
import edu.mcw.rgd.datamodel.variants.VariantTranscript;
import edu.mcw.rgd.variantIndexerRgd.dao.VariantDao;
import edu.mcw.rgd.variantIndexerRgd.model.RgdIndex;
import edu.mcw.rgd.variantIndexerRgd.model.VariantIndex;
import edu.mcw.rgd.variantIndexerRgd.service.ESClient;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;


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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static edu.mcw.rgd.variantIndexerRgd.Manager.getGeneLociMap;

public class ProcessChromosomeBKUP  {
 /*   private String chr;
    private int mapKey;
    private int speciesTypeKey;
    VariantDao vdao=new VariantDao();
    private Map<Long, List<String>> geneLociMap;
    private List<VariantObject> vmdList;
    List<VariantSampleDetail> samples;
    public ProcessChromosomeBKUP(String chr, int mapKey, int speciesTypeKey,  Map<Long, List<String>> geneLociMap, List<VariantObject> vmdList,List<VariantSampleDetail> samples ){
        this.chr=chr;
        this.mapKey=mapKey;
        this.speciesTypeKey=speciesTypeKey;
        this.geneLociMap=geneLociMap;
        this.vmdList=vmdList;
        this.samples=samples;
    }
    @Override
    public void run() {
        System.out.println(Thread.currentThread().getName()+ "\tCHR:"+ chr +"\t started....");
        BulkRequest request = new BulkRequest();

        try {
            System.out.println("Variants of CHR-"+ chr+":" +vmdList.size()+"\tSamples:"+ samples.size() );
            VariantIndex vi = new VariantIndex();

            boolean first=true;
            for (VariantObject vmd : vmdList) {

                if(first){
                    mapVariantDetials(vi, vmd);
                    mapTranscriptsNPolyphen(vmd, vi);
                    addConservationScores(vmd, vi);
                    mapRegion(vmd, vi, geneLociMap);

                    first=false;
                }else{
                    mapTranscriptsNPolyphen(vmd, vi);
                    addConservationScores(vmd, vi);
                    mapRegion(vmd, vi, geneLociMap);
                }
            }
            for (VariantSampleDetail vsd : samples) {
                mapSampleDetails(vsd, vi);
                try {
                    ObjectMapper mapper = new ObjectMapper();
                    byte[] json = mapper.writeValueAsBytes(vi);
                    request.add(new IndexRequest(RgdIndex.getNewAlias()).source(json, XContentType.JSON));

                    //     bulkProcessor.add(new IndexRequest(RgdIndex.getNewAlias()).source(json, XContentType.JSON));
                } catch (JsonProcessingException e) {
                    e.printStackTrace();
                }
            }


            try {
                ESClient.getClient().bulk(request, RequestOptions.DEFAULT);
                request=new BulkRequest();
            } catch (IOException e) {
                e.printStackTrace();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println(Thread.currentThread().getName()+ "\tCHR:"+ chr +"\t END");

    }
    void mapRegion(VariantObject vmd, VariantIndex vi,Map<Long, List<String>> geneLociMap){
        vi.setRegionName(geneLociMap.get(vmd.getStartPos()));
        List<String> regionNameLc=new ArrayList<>();
        for(String s: geneLociMap.get(vmd.getStartPos())){
            regionNameLc.add(s.toLowerCase());
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
   /*     List<VariantTranscript> vts=new ArrayList<>();
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
  /*      if(v.getConScore()!=null) {
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

    }*/
}
