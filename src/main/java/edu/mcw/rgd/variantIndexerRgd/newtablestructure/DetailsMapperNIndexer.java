package edu.mcw.rgd.variantIndexerRgd.newtablestructure;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.mcw.rgd.dao.spring.variants.Polyphen;
import edu.mcw.rgd.datamodel.ConservationScore;
import edu.mcw.rgd.datamodel.variants.VariantMapData;
import edu.mcw.rgd.datamodel.variants.VariantObject;
import edu.mcw.rgd.datamodel.variants.VariantSampleDetail;
import edu.mcw.rgd.datamodel.variants.VariantTranscript;
import edu.mcw.rgd.variantIndexerRgd.dao.VariantDao;
import edu.mcw.rgd.variantIndexerRgd.model.Json;
import edu.mcw.rgd.variantIndexerRgd.model.RgdIndex;
import edu.mcw.rgd.variantIndexerRgd.model.VariantIndex;
import edu.mcw.rgd.variantIndexerRgd.process.MyThreadPoolExecutor;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.xcontent.XContentType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class DetailsMapperNIndexer implements Runnable{
    private String chr;
    private int mapKey;
    private Map<Long, List<String>> geneLociMap;
    private VariantMapData vmd;
    VariantDao vdao=new VariantDao();
    List<VariantSampleDetail> samples;
    List<VariantTranscript> transcripts;

    public DetailsMapperNIndexer(String chr, int mapKey, Map<Long, List<String>> geneLociMap,VariantMapData vmd,List<VariantSampleDetail> samples,

                                 List<VariantTranscript> transcripts){
        this.chr=chr;
        this.mapKey=mapKey;
        this.geneLociMap=geneLociMap;
        this.vmd=vmd;
        this.samples=samples;

        this.transcripts=transcripts;
    }
    @Override
    public void run() {

        VariantIndex vi = new VariantIndex();
        mapVariantDetials(vi, vmd);
        try {
            mapTranscriptsNPolyphen(vmd, vi, transcripts);
        } catch (Exception exception) {
            exception.printStackTrace();
        }
        try {
            addConservationScores(vmd, vi);
        } catch (Exception exception) {
            exception.printStackTrace();
        }
        mapRegion(vmd, vi, geneLociMap);
        ExecutorService executor = new MyThreadPoolExecutor(10, 10, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
        Runnable workerThread =null;
        workerThread=new BulkIndexer(vi, vmd, samples);
        executor.execute(workerThread);
        executor.shutdown();
        while (!executor.isTerminated()) {}

     /*   List<VariantSampleDetail> variantSamplesDetails = getSamples(vmd.getId(),samples);
        for (VariantSampleDetail vsd : variantSamplesDetails) {
            mapSampleDetails(vsd, vi);
            try {
                byte[] json = Json.serializer().mapper().writeValueAsBytes(vi);
                BulkIndexProcessor.bulkProcessor.add(new IndexRequest(RgdIndex.getNewAlias()).source(json, XContentType.JSON));

                //     bulkProcessor.add(new IndexRequest(RgdIndex.getNewAlias()).source(json, XContentType.JSON));
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }
        }*/

    }

    List<VariantSampleDetail> getSamples(long variantRgdId, List<VariantSampleDetail> samples){
        List<VariantSampleDetail> sampleDetails=new ArrayList<>();
        for(VariantSampleDetail vsd:samples){
            try {
                if (vsd.getId() == variantRgdId) {
                    sampleDetails.add(vsd);
                }
            }catch (Exception e){
                e.printStackTrace();
            }
        }
        return sampleDetails;
    }
    void mapRegion(VariantMapData vmd, VariantIndex vi,Map<Long, List<String>> geneLociMap){
        vi.setRegionName(geneLociMap.get(vmd.getStartPos()));
        List<String> regionNameLc=new ArrayList<>();
        for(String s: geneLociMap.get(vmd.getStartPos())){
            regionNameLc.add(s.toLowerCase());
        }
        vi.setRegionNameLc(regionNameLc);
    }
    void mapVariantDetials(VariantIndex vi, VariantMapData vmd){
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
    void mapTranscriptsNPolyphen(VariantMapData vmd, VariantIndex vi, List<VariantTranscript> transcripts) throws Exception {
   /*     List<VariantTranscript> vts=  vdao.getVariantTranscripts(vmd.getId(), vmd.getMapKey());
        List<Polyphen> pps= vdao.getPolyphen(vmd.getId());
        for(VariantTranscript vt:vts){
            for(Polyphen p:pps){
                if(p.getTranscriptRgdid()==vt.getTranscriptRgdId()){
                    vt.setPolyphenStatus(p.getPrediction());
                }
            }
        }

*/
        List<VariantTranscript> vts=new ArrayList<>();
        for(VariantTranscript vt:transcripts){
           /* if(vt.getVariantId()== vmd.getId()){
                vts.add(vt);
            }*/
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
    void addConservationScores(VariantMapData vmd, VariantIndex vi) throws Exception {
       String tableName=getConScoreTable(mapKey, vmd.getGenicStatus());
        List<ConservationScore> conscores=vdao.getConservationScores(vmd.getStartPos(), chr,tableName);
        List<String> scores= new ArrayList<>();
        for(ConservationScore cs:conscores){
            scores.add(String.valueOf(cs.getScore()));
        }

        vi.setConScores(scores);


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
