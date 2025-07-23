package edu.mcw.rgd.variantIndexerRgd.newtablestructure;

import edu.mcw.rgd.datamodel.GeneLoci;

import edu.mcw.rgd.variantIndexerRgd.dao.VariantDao;

import edu.mcw.rgd.variantIndexerRgd.model.VariantIndex;
import edu.mcw.rgd.variantIndexerRgd.process.MyThreadPoolExecutor;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;



public class ProcessVariant extends VariantDao implements  Runnable {
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
        ExecutorService executor2 = new MyThreadPoolExecutor(5, 5, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
        Runnable variantsNewTableThread= null;
        try {
            for(VariantIndex v:indexList){
                variantsNewTableThread=new VariantsNewTableThread(mapKey, v,geneLoci);
                executor2.execute(variantsNewTableThread);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

}
