package edu.mcw.rgd.variantIndexerRgd.variantSearchIndexer;

import edu.mcw.rgd.dao.impl.MapDAO;

import edu.mcw.rgd.datamodel.GeneLoci;
import edu.mcw.rgd.datamodel.Map;

import edu.mcw.rgd.datamodel.search.elasticsearch.MapInfo;
import edu.mcw.rgd.datamodel.variants.VariantIndex;
import edu.mcw.rgd.datamodel.variants.VariantSampleDetail;
import edu.mcw.rgd.services.IndexDocument;
import edu.mcw.rgd.variantIndexerRgd.vvIndexer.VariantDetailsThread;


import java.util.*;



public class VariantSearchIndexer extends VariantDetailsThread {

    MapDAO mapDAO=new MapDAO();
    public VariantSearchIndexer(int mapKey, VariantIndex variant, List<GeneLoci> geneLoci) throws Exception {
        super(mapKey, variant, geneLoci);
    }
    @Override
    public void run(){
        VariantIndex variantIndex=new VariantIndex();
        mapVariant(variantIndex);
        mapTranscripts(variantIndex);
        mapConservationScore(variantIndex);
        mapGeneLoci(variantIndex);
        try {
            mapGeneDetails(variantIndex);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        try {
            mapMapData(variantIndex);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        setClinicalSignificance(variantIndex);
        Set<String> sampleNames=new HashSet<>();
        for(VariantSampleDetail vsd:sampleDetails){
          sampleNames.add(vsd.getAnalysisName());
        }
        variantIndex.setAnalysisName(String.join(",", sampleNames));
        try {
            variantIndex.setMapDataList(this.getMapData(variantIndex));
        } catch (Exception e) {
            e.printStackTrace();
        }
        IndexDocument.index(variantIndex);
    }

    public List<MapInfo> getMapData(VariantIndex vi) throws Exception {
        java.util.Map<Integer, Map> rgdMaps=new HashMap<>();
        List<MapInfo> mapList= new ArrayList<>();
        MapInfo map= new MapInfo();
        map.setChromosome(vi.getChromosome());
        map.setStartPos(vi.getStartPos());
        map.setStopPos(vi.getEndPos());
        edu.mcw.rgd.datamodel.Map m = mapDAO.getMapByKey(vi.getMapKey());
        rgdMaps.put(vi.getMapKey(), m);

        map.setMap(rgdMaps.get(vi.getMapKey()).getDescription());
        map.setRank(rgdMaps.get(vi.getMapKey()).getRank());

        mapList.add(map);

        return mapList;
    }
}
