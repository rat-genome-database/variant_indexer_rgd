package edu.mcw.rgd.variantIndexerRgd.newtablestructure;

import edu.mcw.rgd.variantIndexerRgd.model.RgdIndex;
import edu.mcw.rgd.variantIndexerRgd.model.Json;
import edu.mcw.rgd.variantIndexerRgd.model.VariantIndex;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.xcontent.XContentType;

import java.util.List;

public class ProcessPartChromosome implements Runnable {
    private static final Logger log = LogManager.getLogger(ProcessPartChromosome.class);

    private final List<VariantIndex> indexList;

    public ProcessPartChromosome(List<VariantIndex> indexList) {
        this.indexList = indexList;
    }

    @Override
    public void run() {
        log.info("INDEX LIST SIZE:" + indexList.size());
        String alias = RgdIndex.getInstance().getNewAlias();

        for (VariantIndex vi : indexList) {
            try {
                byte[] json = Json.serializer().mapper().writeValueAsBytes(vi);
                BulkIndexProcessor.bulkProcessor.add(
                        new IndexRequest(alias).id(VariantIndexUtils.docId(vi)).source(json, XContentType.JSON));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
