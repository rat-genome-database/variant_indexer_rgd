package edu.mcw.rgd.variantIndexerRgd.newtablestructure;

import edu.mcw.rgd.dao.impl.GeneLociDAO;
import edu.mcw.rgd.datamodel.GeneLoci;

import edu.mcw.rgd.variantIndexerRgd.dao.VariantDao;
import edu.mcw.rgd.variantIndexerRgd.model.Json;
import edu.mcw.rgd.variantIndexerRgd.model.RgdIndex;
import edu.mcw.rgd.variantIndexerRgd.model.VariantData;
import edu.mcw.rgd.variantIndexerRgd.model.VariantIndex;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.xcontent.XContentType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class ProcessPartChromosome implements  Runnable{
    private List<VariantIndex> indexList;
    public ProcessPartChromosome(List<VariantIndex> indexList){
        this.indexList=indexList;
    }
    @Override
    public void run() {

        System.out.println("INDEX LSIT SIZE:"+indexList.size());

        for(VariantIndex vi:indexList) {
            try {
                String json = Json.serializer().mapper().writeValueAsString(vi);
                BulkIndexProcessor.bulkProcessor.add(new IndexRequest(RgdIndex.getNewAlias()).source(json, XContentType.JSON));

            } catch (Exception e) {
                e.printStackTrace();
            }
        }


    }
    public static Map<Long, List<String>> getGeneLociMap(int mapKey, String chromosome) throws Exception {
        GeneLociDAO dao= new GeneLociDAO();
        List<GeneLoci> loci=dao.getGeneLociByMapKeyAndChr(mapKey, chromosome);
        Map<Long, List<String>> positionGeneMap=new HashMap<>();

        for(GeneLoci g: loci){
            List<String> list=new ArrayList<>();
            if(positionGeneMap.get(g.getPosition())!=null){
                list=positionGeneMap.get(g.getPosition());

            }
            list.add(g.getGeneSymbols());
            positionGeneMap.put(g.getPosition(), list);
        }
        System.out.println("GeneLoci Map size: "+ positionGeneMap.size());

        return positionGeneMap;
    }
}
