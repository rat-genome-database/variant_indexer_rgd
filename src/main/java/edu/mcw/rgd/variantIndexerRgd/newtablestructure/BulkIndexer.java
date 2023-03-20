package edu.mcw.rgd.variantIndexerRgd.newtablestructure;

import com.fasterxml.jackson.core.JsonProcessingException;
import edu.mcw.rgd.datamodel.variants.VariantMapData;
import edu.mcw.rgd.datamodel.variants.VariantSampleDetail;
import edu.mcw.rgd.variantIndexerRgd.model.Json;
import edu.mcw.rgd.variantIndexerRgd.model.RgdIndex;
import edu.mcw.rgd.variantIndexerRgd.model.VariantIndex;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.xcontent.XContentType;


import java.util.ArrayList;
import java.util.List;

public class BulkIndexer implements Runnable {
    private VariantIndex vi;
    private VariantMapData vmd;
    List<VariantSampleDetail> samples;
    public BulkIndexer(VariantIndex vi, VariantMapData vamd, List<VariantSampleDetail> samples) {
      this.vi=vi;
      this.vmd=vamd;
      this.samples=samples;
    }

    @Override
    public void run() {
        List<VariantSampleDetail> variantSamplesDetails = getSamples(vmd.getId(),samples);
        for (VariantSampleDetail vsd : variantSamplesDetails) {
            mapSampleDetails(vsd, vi);
            try {
                byte[] json = Json.serializer().mapper().writeValueAsBytes(vi);
                BulkIndexProcessor.bulkProcessor.add(new IndexRequest(RgdIndex.getNewAlias()).source(json, XContentType.JSON));

                //     bulkProcessor.add(new IndexRequest(RgdIndex.getNewAlias()).source(json, XContentType.JSON));
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }
        }
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