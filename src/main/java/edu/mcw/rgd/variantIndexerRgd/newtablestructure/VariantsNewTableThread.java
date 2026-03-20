package edu.mcw.rgd.variantIndexerRgd.newtablestructure;

import edu.mcw.rgd.variantIndexerRgd.dao.VariantDao;
import edu.mcw.rgd.variantIndexerRgd.model.VariantIndex;

import java.util.ArrayList;
import java.util.List;

public class VariantsNewTableThread implements Runnable{
    private List<Integer> variantIds;
    private int mapKey;

    VariantDao variantDao=new VariantDao();
    public VariantsNewTableThread(int mapKey, List<Integer> variantIds){
        this.variantIds=variantIds;
        this.mapKey=mapKey;
    }
    @Override
    public void run() {
        List<VariantIndex> indexList = new ArrayList<>();
        try {
            indexList = variantDao.getVariantsForIndexing(mapKey, variantIds);
        } catch (Exception e) {
            e.printStackTrace();
        }
        if (!indexList.isEmpty()) {
            new ProcessPartChromosome(indexList).run();
        }
    }
}
