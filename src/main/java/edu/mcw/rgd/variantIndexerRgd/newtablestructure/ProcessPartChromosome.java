package edu.mcw.rgd.variantIndexerRgd.newtablestructure;

import edu.mcw.rgd.variantIndexerRgd.Manager;
import edu.mcw.rgd.variantIndexerRgd.model.RgdIndex;
import edu.mcw.rgd.variantIndexerRgd.model.Json;

import edu.mcw.rgd.variantIndexerRgd.model.VariantIndex;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.xcontent.XContentType;

import java.util.List;

import static org.apache.logging.log4j.LogManager.getLogger;


public class ProcessPartChromosome implements  Runnable{
    private final List<VariantIndex> indexList;
    static Logger log=getLogger(Manager.class);

    public ProcessPartChromosome(List<VariantIndex> indexList){
        this.indexList=indexList;
    }
    @Override
    public void run() {
       log.info("INDEX LIST SIZE:"+indexList.size());
        String alias = RgdIndex.getInstance().getNewAlias();

        for(VariantIndex vi:indexList) {
            try {
                String json = Json.serializer().mapper().writeValueAsString(vi);
                BulkIndexProcessor.bulkProcessor.add(
                        new IndexRequest(alias).id(VariantIndexUtils.docId(vi)).source(json, XContentType.JSON));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
