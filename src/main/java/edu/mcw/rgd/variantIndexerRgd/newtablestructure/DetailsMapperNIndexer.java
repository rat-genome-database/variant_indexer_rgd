package edu.mcw.rgd.variantIndexerRgd.newtablestructure;

import edu.mcw.rgd.datamodel.variants.VariantMapData;
import edu.mcw.rgd.datamodel.variants.VariantSampleDetail;
import edu.mcw.rgd.datamodel.variants.VariantTranscript;
import edu.mcw.rgd.variantIndexerRgd.dao.VariantDao;

import edu.mcw.rgd.variantIndexerRgd.model.VariantIndex;

import java.util.List;
import java.util.Map;

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
        VariantIndexUtils.mapVariantDetails(vi, vmd);
        try {
            vi.setVariantTranscripts(vdao.getVariantTranscripts(vmd.getId(), vmd.getMapKey()));
        } catch (Exception exception) {
            exception.printStackTrace();
        }
        try {
            VariantIndexUtils.addConservationScores(vi);
        } catch (Exception exception) {
            exception.printStackTrace();
        }
        VariantIndexUtils.mapRegion(vmd, vi, geneLociMap);
        // Run BulkIndexer directly instead of wrapping in unnecessary thread pool
        new BulkIndexer(vi, vmd, samples).run();
    }
}
