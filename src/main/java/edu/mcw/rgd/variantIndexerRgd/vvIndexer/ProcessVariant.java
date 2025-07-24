package edu.mcw.rgd.variantIndexerRgd.vvIndexer;

import edu.mcw.rgd.datamodel.GeneLoci;


import edu.mcw.rgd.datamodel.variants.VariantIndex;
import edu.mcw.rgd.variantIndexerRgd.utils.MyThreadPoolExecutor;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;



public class ProcessVariant implements  Runnable {
    private final List<VariantIndex> indexList;
    private final int mapKey;
    private List<GeneLoci> geneLoci;


    public ProcessVariant(List<VariantIndex> indexList, int mapKey, List<GeneLoci> geneLoci) {
        this.indexList = indexList;
        this.mapKey = mapKey;
        this.geneLoci=geneLoci;
    }

    @Override
    public void run() {
        ExecutorService executor2 = new MyThreadPoolExecutor(10, 10, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
        Runnable variantsNewTableThread= null;
        try {
            for(VariantIndex v:indexList){
                variantsNewTableThread=new VariantDetailsThread(mapKey, v,geneLoci);
                executor2.execute(variantsNewTableThread);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

}
