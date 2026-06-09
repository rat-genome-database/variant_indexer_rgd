package edu.mcw.rgd.variantIndexerRgd;


import co.elastic.clients.elasticsearch._helpers.bulk.BulkIngester;
import edu.mcw.rgd.dao.impl.GeneLociDAO;
import edu.mcw.rgd.datamodel.GeneLoci;
import edu.mcw.rgd.datamodel.RgdIndex;
import edu.mcw.rgd.datamodel.variants.VariantMapData;

import edu.mcw.rgd.services.ClientInit;
import edu.mcw.rgd.variantIndexerRgd.dao.VariantDao;
import edu.mcw.rgd.variantIndexerRgd.model.VariantData;
import edu.mcw.rgd.variantIndexerRgd.model.VariantIndexObject;
import edu.mcw.rgd.variantIndexerRgd.newtablestructure.BulkIndexProcessor;
import org.apache.logging.log4j.Logger;

import java.util.*;


public class RatIndexer implements Runnable {
    VariantDao variantDao=new VariantDao();

    private VariantMapData v;
    Map<Long, List<String>> geneLoci;
    Variants variants=new Variants();
    public RatIndexer(){}
    public RatIndexer(VariantMapData v, Map<Long, List<String>> geneLoci){
        this.v=v;
        this.geneLoci=geneLoci;

    }
        public void run() {
            Logger log=Manager.log;
            try {

                System.out.println(Thread.currentThread().getName()+"\tMAPKEY:"+ v.getMapKey() +"\tCHR: "+v.getChromosome() +"\tSTART POS: "+ v.getStartPos() );
                VariantIndexObject object = new VariantIndexObject();
                List<String> regionNames=geneLoci.get(v.getStartPos());
                if(regionNames!=null && regionNames.size()!=0) {
                    object.setRegionName(regionNames);
                    List<String> regionNameLC = new ArrayList<>();
                    for (String name : regionNames) {
                        regionNameLC.add(name.toLowerCase());
                    }
                    object.setRegionNameLc(regionNameLC);
                }

                object.setVariant(v);
                try {
                    List<VariantData> records = variants.getVariants(v.getSpeciesTypeKey(), v.getChromosome(), v.getMapKey(),(int) v.getId());
                    VariantIndexObject o=variantDao.mapSamplesNTranscripts(records, object);
                    object.setSamples(o.getSamples());
                    object.setVariantTranscripts(o.getVariantTranscripts());
                } catch (Exception e) {
                    e.printStackTrace();
                }
                ClientInit.getClient().index(i -> i.index(RgdIndex.getNewAlias()).document(object));
                log.info(Thread.currentThread().getName()+"\tMAPKEY:"+ v.getMapKey() +"\tCHR: "+v.getChromosome() +"\tSTART POS: "+ v.getStartPos() +"\tEND!!" );

            } catch (Exception e) {
                e.printStackTrace();
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


    public BulkIngester<Void> getBulkProcessor(){
         return BulkIndexProcessor.newIngester(1);
    }

}
