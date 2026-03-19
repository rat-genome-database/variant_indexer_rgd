package edu.mcw.rgd.variantIndexerRgd.newtablestructure;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.mcw.rgd.datamodel.RgdIndex;
import edu.mcw.rgd.datamodel.variants.VariantTranscript;
import edu.mcw.rgd.variantIndexerRgd.dao.VariantDao;
import edu.mcw.rgd.variantIndexerRgd.model.VariantIndex;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.xcontent.XContentType;

import java.util.List;
import java.util.Map;


public class ProcessSample  implements Runnable{
    private List<VariantIndex> indexList;
    private Map<Long, List<String>> geneLociMap;
    private String chr;
    private int sampleId;
    List<VariantTranscript> transcripts;
    BulkIndexProcessor bulkIndexProcessor;
    VariantDao dao=new VariantDao();

    public ProcessSample(List<VariantIndex> indexList, Map<Long, List<String>> geneLociMap,
                         String chr, int sampleId,
                         BulkIndexProcessor bulkIndexProcessor, List<VariantTranscript> transcripts){
       this.indexList=indexList;
       this.geneLociMap=geneLociMap;
       this.chr=chr;
       this.sampleId=sampleId;
       this.bulkIndexProcessor=bulkIndexProcessor;
       this.transcripts=transcripts;
    }
    @Override
    public void run() {
        System.out.println(Thread.currentThread().getName()+"\tVARIANTS OF CHR: "+ chr+ "\tSAMPLE-"+sampleId+":"+indexList.size());

        ObjectMapper mapper = new ObjectMapper();
        mapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        for(VariantIndex vi: indexList) {

            try {

                VariantIndexUtils.mapRegion(vi, geneLociMap);
                vi.setVariantTranscripts(dao.getVariantTranscripts(vi.getVariant_id(), vi.getMapKey()));
                VariantIndexUtils.addConservationScores(vi);

                try {
                    String json = mapper.writeValueAsString(vi);
                    String docId = vi.getVariant_id() + "-" + vi.getSampleId() + "-" + vi.getMapKey();
                    bulkIndexProcessor.bulkProcessor.add(new IndexRequest(RgdIndex.getNewAlias()).id(docId).source(json, XContentType.JSON));

                } catch (Exception e) {
                    e.printStackTrace();
                }


            } catch (Exception e) {
                e.printStackTrace();
            }


        }

        System.out.println(Thread.currentThread().getName()+"\tCHR: "+ chr+ "\tSAMPLE-"+sampleId +"\t END");

    }
}
