package edu.mcw.rgd.variantIndexerRgd.vvIndexer;

import edu.mcw.rgd.dao.impl.variants.VariantDAO;
import edu.mcw.rgd.datamodel.GeneLoci;


import edu.mcw.rgd.datamodel.variants.VariantIndex;
import edu.mcw.rgd.variantIndexerRgd.utils.MyThreadPoolExecutor;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;


public class ProcessVariant implements  Runnable {
    private final List<VariantIndex> indexList;
    private final int mapKey;
    private final List<GeneLoci> geneLoci;
    private final String chromosome;
    VariantDAO variantDAO=new VariantDAO();

    public ProcessVariant(List<VariantIndex> indexList, int mapKey, List<GeneLoci> geneLoci, String chromosome) {
        this.indexList = indexList;
        this.mapKey = mapKey;
        this.geneLoci=geneLoci;
        this.chromosome=chromosome;
    }

    @Override
    public void run() {
        ExecutorService executor2 = new MyThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
        Runnable variantsNewTableThread= null;
        try {
//            for(VariantIndex v:indexList){
//                variantsNewTableThread=new VariantDetailsThread(mapKey, v,geneLoci);
//                executor2.execute(variantsNewTableThread);
//            }
           List<VariantIndex> variantDetails = variantDAO.getVariantsNewTableStructure(mapKey, new ArrayList<>( indexList.stream().map(v->(int)v.getVariant_id()).collect(Collectors.toSet())));

           variantsNewTableThread=new MapperThread(indexList, variantDetails, mapKey,geneLoci, chromosome);
              executor2.execute(variantsNewTableThread);

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        executor2.shutdown();
        while (!executor2.isTerminated()) {}
    }

}
