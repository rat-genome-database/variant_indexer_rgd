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
        try {

                Set<Long> ids=indexList.stream().map(VariantIndex::getVariant_id).collect(Collectors.toSet());
                if(ids.size()>0) {
                    List<VariantIndex> variantDetails = variantDAO.getVariantsNewTableStructure(mapKey, new ArrayList<>(indexList.stream().map(v -> (int) v.getVariant_id()).collect(Collectors.toSet())));

                   MapperThread detailsThread = new MapperThread(indexList, variantDetails, mapKey, geneLoci, chromosome);
                   detailsThread.run();
                }

            } catch (Exception e) {
                e.printStackTrace();
            }

    }

}
