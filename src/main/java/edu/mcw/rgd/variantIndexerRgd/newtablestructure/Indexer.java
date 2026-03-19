package edu.mcw.rgd.variantIndexerRgd.newtablestructure;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.mcw.rgd.variantIndexerRgd.model.RgdIndex;
import edu.mcw.rgd.datamodel.variants.VariantTranscript;
import edu.mcw.rgd.variantIndexerRgd.model.VariantData;
import edu.mcw.rgd.variantIndexerRgd.model.VariantIndex;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.xcontent.XContentType;

import java.util.*;
import java.util.Map.Entry;

public class Indexer implements Runnable{
    private List<VariantData> vrs;
    private java.util.Map<Long, List<String>> geneLoci;
    private int mapKey;
    String chromosome;
    public Indexer(){}
    public Indexer(List<VariantData> vrs,  java.util.Map<Long, List<String>> geneLoci, int mapKey,String chromosome){
        this.vrs=vrs;
        this.geneLoci=geneLoci;
        this.mapKey=mapKey;
        this.chromosome=chromosome;
    }
    @Override
    public void run() {
        java.util.Map<Integer, VariantIndex> processedMap=new HashMap<>();
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
    void index(java.util.Map<Integer, VariantIndex>  processedMap){
        ObjectMapper mapper = new ObjectMapper();
        for(Entry entry:processedMap.entrySet()){
            VariantIndex object= (VariantIndex) entry.getValue();
            try {
                byte[] json =  mapper.writeValueAsBytes(object);
                String docId = object.getVariant_id() + "-" + object.getSampleId() + "-" + object.getMapKey();
                BulkIndexProcessor.bulkProcessor.add(new IndexRequest(RgdIndex.getInstance().getNewAlias()).id(docId).source(json, XContentType.JSON));
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }
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
        v.setRsId(vd.getRsId());
        v.setClinvarId(vd.getClinvarId());
        List<String> conScores= new ArrayList<>();
        conScores.add(String.valueOf(vd.getConservationScore()));
        if(!conScores.isEmpty())
        v.setConScores(conScores);
        v.setRegionName(getRegionNames(vd.getStartPos()));
        v.setRegionNameLc(getRegionNamesLC(vd.getStartPos()));
        VariantTranscript vt=mapTranscriptObject(vd);
        List<VariantTranscript> vts=new ArrayList<>(Arrays.asList(vt));
        if(!vts.isEmpty())
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
        for(VariantTranscript t:vts){
            if(transcriptRgdId==t.getTranscriptRgdId()){
                return true;
            }
        }
        return false;
    }
   List<String> getRegionNames(long startPos){
       return geneLoci.get(startPos);
    }
     List<String> getRegionNamesLC(long startPos){
        List<String> regionNames=geneLoci.get(startPos);
        List<String> regionNameLC = new ArrayList<>();
        if(regionNames!=null && !regionNames.isEmpty()) {
            for (String name : regionNames) {
                regionNameLC.add(name.toLowerCase());
            }
        }
        return regionNameLC;
    }

}
