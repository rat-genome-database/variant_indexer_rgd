package edu.mcw.rgd.variantIndexerRgd.vvIndexer;

import edu.mcw.rgd.dao.impl.VariantInfoDAO;
import edu.mcw.rgd.dao.impl.variants.VariantDAO;
import edu.mcw.rgd.datamodel.ConservationScore;
import edu.mcw.rgd.datamodel.GeneLoci;


import edu.mcw.rgd.datamodel.VariantSearchBean;
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
    private  Map<Integer, Set<String>> clinicalSignificance;
    private  List<ConservationScore> conservationScores;
    VariantDAO variantDAO=new VariantDAO();
    VariantInfoDAO variantInfoDAO=new VariantInfoDAO();

    public ProcessVariant(List<VariantIndex> indexList, int mapKey, List<GeneLoci> geneLoci, String chromosome) throws Exception {
        this.indexList = indexList;
        this.mapKey = mapKey;
        this.geneLoci=geneLoci;
        this.chromosome=chromosome;
        if(mapKey==38 || mapKey==17)
            setClinicalSignificance();
        if(mapKey==60 || mapKey==70 || mapKey==360 || mapKey==17 || mapKey==38)
            setConservationScore();
    }

    @Override
    public void run() {
        try {
            ExecutorService executor2 = new MyThreadPoolExecutor(10, 10, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());

                Set<Long> ids=indexList.stream().map(VariantIndex::getVariant_id).collect(Collectors.toSet());
                if(ids.size()>0) {
                    List<VariantIndex> variantDetails = variantDAO.getVariantsNewTableStructure(mapKey, new ArrayList<>(indexList.stream().map(v -> (int) v.getVariant_id()).collect(Collectors.toSet())));
                    Runnable detailsThread= null;
                    for (VariantIndex variant : indexList) {
                        detailsThread=new VariantDetailsThread(mapKey,variant,variantDetails,conservationScores,clinicalSignificance,geneLoci);
                        detailsThread.run();
                    }
                }
            executor2.shutdown();
            while (!executor2.isTerminated()) {}
            } catch (Exception e) {
                e.printStackTrace();
            }

    }
    public void setConservationScore() throws Exception {
        VariantSearchBean vsb=new VariantSearchBean(mapKey);
        String csTable=vsb.getConScoreTable();
        if(csTable!=null && !csTable.equals("")) {
            this.conservationScores = variantDAO.getConservationScoresByPositionsList(mapKey,chromosome , new ArrayList<>( indexList.stream().map(VariantIndex::getStartPos).collect(Collectors.toSet())));
        }
    }
    public void setClinicalSignificance() throws Exception {
        this.clinicalSignificance=variantInfoDAO.getClinicalSignificance(new ArrayList<>(indexList.stream().map(VariantIndex::getVariant_id).collect(Collectors.toSet())));
    }
}
