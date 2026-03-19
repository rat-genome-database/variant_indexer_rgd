package edu.mcw.rgd.variantIndexerRgd.newtablestructure;

import com.fasterxml.jackson.databind.ObjectMapper;

import edu.mcw.rgd.datamodel.RgdIndex;
import edu.mcw.rgd.datamodel.variants.VariantTranscript;

import edu.mcw.rgd.variantIndexerRgd.dao.VariantDao;
import edu.mcw.rgd.variantIndexerRgd.model.VariantIndex;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.xcontent.XContentType;

import java.util.*;

public class ProcessVariant implements Runnable {

    private Map<Long, List<String>> geneLociMap;
    private VariantIndex vi;
    List<VariantTranscript> transcripts;
    BulkIndexProcessor bulkIndexProcessor;
    VariantDao dao=new VariantDao();
    public ProcessVariant(){}
    public ProcessVariant(Map<Long, List<String>> geneLociMap, VariantIndex vi,
                          List<VariantTranscript> transcripts,
                                  BulkIndexProcessor bulkIndexProcessor){

        this.geneLociMap=geneLociMap;
        this.vi=vi;
        this.bulkIndexProcessor=bulkIndexProcessor;
        this.transcripts=transcripts;

    }
   @Override
    public void run() {
        try {
                VariantIndexUtils.mapRegion(vi, geneLociMap);
                vi.setVariantTranscripts(dao.getVariantTranscripts(vi.getVariant_id(), vi.getMapKey()));
                VariantIndexUtils.addConservationScores(vi);

                    try {
                        ObjectMapper mapper = new ObjectMapper();
                        String json = mapper.writeValueAsString(vi);
                     bulkIndexProcessor.bulkProcessor.add(new IndexRequest(RgdIndex.getNewAlias()).source(json, XContentType.JSON));
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
