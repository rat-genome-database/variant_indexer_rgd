package edu.mcw.rgd.variantIndexerRgd.newtablestructure;

import edu.mcw.rgd.datamodel.RgdIndex;
import edu.mcw.rgd.variantIndexerRgd.model.Json;

import edu.mcw.rgd.variantIndexerRgd.model.VariantIndex;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.xcontent.XContentType;

import java.util.List;


public class ProcessPartChromosome implements  Runnable{
    private List<VariantIndex> indexList;
    public ProcessPartChromosome(List<VariantIndex> indexList){
        this.indexList=indexList;
    }
    @Override
    public void run() {
        System.out.println("INDEX LIST SIZE:"+indexList.size());

        for(VariantIndex vi:indexList) {
            try {
                String json = Json.serializer().mapper().writeValueAsString(vi);
                BulkIndexProcessor.bulkProcessor.add(new IndexRequest(RgdIndex.getNewAlias()).source(json, XContentType.JSON));

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
