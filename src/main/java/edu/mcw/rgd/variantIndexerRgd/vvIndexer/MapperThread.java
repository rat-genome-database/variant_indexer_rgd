package edu.mcw.rgd.variantIndexerRgd.vvIndexer;

import edu.mcw.rgd.dao.impl.VariantInfoDAO;
import edu.mcw.rgd.dao.impl.variants.VariantDAO;
import edu.mcw.rgd.datamodel.ConservationScore;
import edu.mcw.rgd.datamodel.GeneLoci;
import edu.mcw.rgd.datamodel.VariantSearchBean;
import edu.mcw.rgd.datamodel.variants.VariantIndex;
import edu.mcw.rgd.datamodel.variants.VariantTranscript;
import edu.mcw.rgd.services.IndexDocument;
import edu.mcw.rgd.variantIndexerRgd.utils.MyThreadPoolExecutor;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;


public class MapperThread implements Runnable{
    private final List<VariantIndex> variants;
    private final List<VariantIndex> variantDetails;
    private final int mapKey;
    private  Map<Integer, Set<String>> clinicalSignificance;
    private  List<ConservationScore> conservationScores;
    private final String chromosome;

    private final List<GeneLoci> geneLoci;

    VariantDAO variantDAO=new VariantDAO();
    VariantInfoDAO variantInfoDAO=new VariantInfoDAO();
    public MapperThread(List<VariantIndex> variants, List<VariantIndex> variantDetails, int mapKey, List<GeneLoci> geneLoci, String chromosome) throws Exception {
        this.variants = variants;
        this.variantDetails = variantDetails;
        this.mapKey = mapKey;
        if(mapKey==38 || mapKey==17)
            setClinicalSignificance();
        if(mapKey==60 || mapKey==70 || mapKey==360 || mapKey==17 || mapKey==38)
            setConservationScore();
        this.geneLoci = geneLoci;
        this.chromosome=chromosome;
    }

    @Override
    public void run() {

        try {
            sortVariants();
        } catch (Exception e) {
           e.printStackTrace();
        }
    }
    public void sortVariants() throws Exception {
        ExecutorService executor2 = new MyThreadPoolExecutor(10, 10, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
        Runnable detailsThread= null;
        for (VariantIndex variant : variants) {
            detailsThread=new VariantDetailsThread(mapKey,variant,variantDetails,conservationScores,clinicalSignificance,geneLoci);
            detailsThread.run();
        }
        executor2.shutdown();
        while (!executor2.isTerminated()) {}

    }
    public void setConservationScore() throws Exception {
        VariantSearchBean vsb=new VariantSearchBean(mapKey);
        String csTable=vsb.getConScoreTable();
        if(csTable!=null && !csTable.equals("")) {
            this.conservationScores = variantDAO.getConservationScoresByPositionsList(mapKey,chromosome , new ArrayList<>( variants.stream().map(VariantIndex::getStartPos).collect(Collectors.toSet())));
        }
    }
    public void setClinicalSignificance() throws Exception {
        this.clinicalSignificance=variantInfoDAO.getClinicalSignificance(new ArrayList<>(variants.stream().map(VariantIndex::getVariant_id).collect(Collectors.toSet())));
    }


}
