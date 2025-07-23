package edu.mcw.rgd.variantIndexerRgd.newtablestructure;

import edu.mcw.rgd.datamodel.GeneLoci;
import edu.mcw.rgd.datamodel.Variant;

import edu.mcw.rgd.variantIndexerRgd.dao.VariantDao;

import edu.mcw.rgd.variantIndexerRgd.model.VariantIndex;
import edu.mcw.rgd.variantIndexerRgd.process.MyThreadPoolExecutor;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;



public class ProcessPartChromosome extends VariantDao implements  Runnable {
    private final List<VariantIndex> indexList;
    private final int mapKey;
    private List<GeneLoci> geneLoci;


    public ProcessPartChromosome(List<VariantIndex> indexList, int mapKey, List<GeneLoci> geneLoci) {
        this.indexList = indexList;
        this.mapKey = mapKey;
        this.geneLoci=geneLoci;
    }

    @Override
    public void run() {

        System.out.println("INDEX LSIT SIZE:" + indexList.size());
        ExecutorService executor2 = new MyThreadPoolExecutor(5, 5, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
        Runnable variantsNewTableThread= null;
        try {
            for(VariantIndex v:indexList){
                variantsNewTableThread=new VariantsNewTableThread(mapKey, v,geneLoci);
                executor2.execute(variantsNewTableThread);
            }
          //  sortVariants();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    //    public VariantIndex cloneObject(VariantIndex variantIndex){
//        VariantIndex index=new VariantIndex();
//        index.setVariant_id(variantIndex.getVariant_id());
//        index.setChromosome(variantIndex.getChromosome());
//        index.setAnalysisName(variantIndex.getAnalysisName());
//        index.setClinicalSignificance(variantIndex.getClinicalSignificance());
//        index.setConScores(variantIndex.getConScores());
//        index.setCategory(variantIndex.getCategory());
//        index.setClinvarId(variantIndex.getClinvarId());
//        index.setDbsSnpName(variantIndex.getDbsSnpName());
//        index.setEndPos(variantIndex.getEndPos());
//        index.setGeneRgdId(variantIndex.getGeneRgdId());
//        index.setGeneSymbol(variantIndex.getGeneSymbol());
//        index.setGenicStatus(variantIndex.getGenicStatus());
//        index.setHGVSNAME(variantIndex.getHGVSNAME());
//        index.setMapKey(variantIndex.getMapKey());
//
//        return index;
//    }
//    public void sortVariants() throws Exception {
//
//        Set<Long> variantIdsWithTrancripts = new HashSet<>();
//        Set<Long> uniqueVariantIds = indexList.stream().map(VariantIndex::getVariant_id).collect(Collectors.toSet());
//        Set<Integer> sampleIds = indexList.stream().map(VariantIndex::getSampleId).collect(Collectors.toSet());
//        for (long variantId : uniqueVariantIds) {
//
//            for (int sampleId : sampleIds) {
//                boolean first = true;
//                VariantIndex indexDoc = null;
//                for (VariantIndex variant : indexList) {
//                    if (variant.getSampleId() == sampleId && variant.getVariant_id() == variantId) {
//                        if (first) {
//                            indexDoc = variant;
//                            first = false;
//                        }
//                        variantIdsWithTrancripts.add(variant.getVariant_id());
////                        String key = variant.getVariant_id() + "-" + variant.getSampleId() + "-" + variant.getMapKey();
//                        if (mapKey == 38 || mapKey == 17) {
//                            try {
//                                String clinvarSignificance = getClinvarInfo((int) variant.getVariant_id());
//                                if (clinvarSignificance != null && !clinvarSignificance.equals(""))
//                                    indexDoc.setClinicalSignificance(clinvarSignificance);
//                            } catch (Exception e) {
//                                System.out.println("NO CLINICAL SIGNIFICACE SAMPLE_ID:" + variant.getSampleId() + " RGD_ID:" + variant.getVariant_id());
//                            }
//                        }
//
//                        List<VariantTranscript> vTranscripts = new ArrayList<>();
//                        if (indexDoc.getVariantTranscripts() != null) {
//                            vTranscripts.addAll(indexDoc.getVariantTranscripts());
//                        }
//                        boolean exists = false;
//                        if (variant.getVariantTranscripts() != null) {
//                            for (VariantTranscript transcript : variant.getVariantTranscripts()) {
//                                for (VariantTranscript variantTranscript : vTranscripts) {
//                                    if (transcript.getTranscriptRgdId() == variantTranscript.getTranscriptRgdId()) {
//                                        exists = true;
//                                        break;
//                                    }
//                                }
//                                if (!exists) {
//                                    vTranscripts.add(transcript);
//                                    indexDoc.setVariantTranscripts(vTranscripts);
//                                }
//
//                            }
//                        }
//
//                    }
//                }
//                IndexDocument.index(indexDoc);
//            }
//        }
//
//    }
}
