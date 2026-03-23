package edu.mcw.rgd.variantIndexerRgd.newtablestructure;

import edu.mcw.rgd.variantIndexerRgd.dao.VariantDao;
import edu.mcw.rgd.variantIndexerRgd.model.VariantIndex;

import java.util.ArrayList;
import java.util.List;

public class VariantsNewTableThread implements Runnable {
    private final int mapKey;
    private final List<Integer> variantIds;
    private final VariantDao variantDao;

    public VariantsNewTableThread(int mapKey, List<Integer> variantIds, VariantDao variantDao) {
        this.variantIds = variantIds;
        this.mapKey = mapKey;
        this.variantDao = variantDao;
    }

    @Override
    public void run() {
        List<VariantIndex> indexList;
        try {
            indexList = variantDao.getVariantsForIndexing(mapKey, variantIds);
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }
        if (!indexList.isEmpty()) {
            new ProcessPartChromosome(indexList).run();
        }
    }
}
