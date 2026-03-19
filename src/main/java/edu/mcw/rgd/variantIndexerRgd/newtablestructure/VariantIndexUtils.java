package edu.mcw.rgd.variantIndexerRgd.newtablestructure;

import edu.mcw.rgd.dao.impl.GeneLociDAO;
import edu.mcw.rgd.datamodel.ConservationScore;
import edu.mcw.rgd.datamodel.GeneLoci;
import edu.mcw.rgd.datamodel.variants.VariantMapData;
import edu.mcw.rgd.datamodel.variants.VariantObject;
import edu.mcw.rgd.datamodel.variants.VariantSampleDetail;
import edu.mcw.rgd.datamodel.variants.VariantTranscript;
import edu.mcw.rgd.variantIndexerRgd.dao.VariantDao;
import edu.mcw.rgd.variantIndexerRgd.model.VariantIndex;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

public class VariantIndexUtils {

    private static final VariantDao dao = new VariantDao();

    public static String getConScoreTable(int mapKey, String genicStatus) {
        switch (mapKey) {
            case 17:
                return " B37_CONSCORE_PART_IOT ";
            case 38:
                return " CONSERVATION_SCORE_HG38 ";
            case 60:
                if (genicStatus != null && genicStatus.equalsIgnoreCase("GENIC")) {
                    return " CONSERVATION_SCORE_GENIC ";
                }
                return " CONSERVATION_SCORE ";
            case 70:
                return " CONSERVATION_SCORE_5 ";
            case 360:
                return " CONSERVATION_SCORE_6 ";
            default:
                return " CONSERVATION_SCORE_6 ";
        }
    }

    public static void mapSampleDetails(VariantSampleDetail vsd, VariantIndex vi) {
        vi.setSampleId(vsd.getSampleId());
        vi.setAnalysisName(vsd.getAnalysisName());
        vi.setZygosityInPseudo(vsd.getZygosityInPseudo());
        vi.setZygosityNumAllele(vsd.getZygosityNumberAllele());
        vi.setZygosityPercentRead(vsd.getZygosityPercentRead());
        vi.setZygosityPossError(vsd.getZygosityPossibleError());
        vi.setZygosityStatus(vsd.getZygosityStatus());
        vi.setQualityScore(vsd.getQualityScore());
        vi.setTotalDepth(vsd.getDepth());
        vi.setVarFreq(vsd.getVariantFrequency());
    }

    public static void mapRegion(VariantIndex vi, Map<Long, List<String>> geneLociMap) {
        List<String> regionNames = geneLociMap.get(vi.getStartPos());
        if (regionNames == null || regionNames.isEmpty()) {
            return;
        }
        vi.setRegionName(regionNames);
        List<String> regionNameLc = new ArrayList<>();
        for (String s : regionNames) {
            regionNameLc.add(s.toLowerCase());
        }
        vi.setRegionNameLc(regionNameLc);
    }

    public static void mapRegion(VariantMapData vmd, VariantIndex vi, Map<Long, List<String>> geneLociMap) {
        List<String> regionNames = geneLociMap.get(vmd.getStartPos());
        if (regionNames == null || regionNames.isEmpty()) {
            return;
        }
        vi.setRegionName(regionNames);
        List<String> regionNameLc = new ArrayList<>();
        for (String s : regionNames) {
            regionNameLc.add(s.toLowerCase());
        }
        vi.setRegionNameLc(regionNameLc);
    }

    public static Map<Long, List<String>> getGeneLociMap(int mapKey, String chromosome) throws Exception {
        GeneLociDAO dao = new GeneLociDAO();
        List<GeneLoci> loci = dao.getGeneLociByMapKeyAndChr(mapKey, chromosome);
        Map<Long, List<String>> positionGeneMap = new HashMap<>();

        for (GeneLoci g : loci) {
            List<String> list = positionGeneMap.get(g.getPosition());
            if (list == null) {
                list = new ArrayList<>();
                positionGeneMap.put(g.getPosition(), list);
            }
            list.add(g.getGeneSymbols());
        }
        System.out.println("GeneLoci Map size of CHR-:" + chromosome + "\t" + positionGeneMap.size());

        return positionGeneMap;
    }

    public static Collection[] split(List<Integer> rgdids, int size) {
        int numOfBatches = rgdids.size() / size + 1;
        Collection[] batches = new Collection[numOfBatches];

        for (int index = 0; index < numOfBatches; ++index) {
            int count = index + 1;
            int fromIndex = Math.max((count - 1) * size, 0);
            int toIndex = Math.min(count * size, rgdids.size());
            batches[index] = rgdids.subList(fromIndex, toIndex);
        }

        return batches;
    }

    public static VariantTranscript getTranscriptObject(VariantObject v) {
        VariantTranscript t = new VariantTranscript();
        t.setFullRefAAPos(v.getFullRefAAPos());
        t.setFullRefNucPos(v.getFullRefNucPos());
        t.setTranscriptRgdId(v.getTranscriptRgdId());
        t.setRefAA(v.getRefAA());
        t.setVarAA(v.getVarAA());
        t.setLocationName(v.getLocationName());
        t.setSynStatus(v.getSynStatus());
        t.setNearSpliceSite(v.getNearSpliceSite());
        t.setFullRefNucSeqKey(v.getFullRefNucSeqKey());
        t.setFullRefAASeqKey(v.getFullRefAASeqKey());
        t.setTripletError(v.getTripletError());
        t.setFrameShift(v.getFrameShift());
        t.setPolyphenStatus(v.getPolyphenStatus());
        return t;
    }

    public static void addConservationScores(VariantIndex vi) throws Exception {
        List<String> scores = new ArrayList<>();
        List<ConservationScore> conScores = dao.getConservationScores(
                vi.getStartPos(), vi.getChromosome(), getConScoreTable(vi.getMapKey(), ""));
        if (conScores != null && !conScores.isEmpty()) {
            for (ConservationScore s : conScores) {
                if (s != null) {
                    scores.add(String.valueOf(s.getScore()));
                }
            }
            vi.setConScores(scores);
        }
    }

    public static void mapVariantDetails(VariantIndex vi, VariantMapData vmd) {
        vi.setVariant_id(vmd.getId());
        vi.setChromosome(vmd.getChromosome());
        vi.setStartPos(vmd.getStartPos());
        vi.setEndPos(vmd.getEndPos());
        vi.setRefNuc(vmd.getReferenceNucleotide());
        vi.setVarNuc(vmd.getVariantNucleotide());
        vi.setMapKey(vmd.getMapKey());
        vi.setClinvarId(vmd.getClinvarId());
        vi.setRsId(vmd.getRsId());
        vi.setGenicStatus(vmd.getGenicStatus());
        vi.setPaddingBase(vmd.getPaddingBase());
        vi.setVariantType(vmd.getVariantType());
    }

    public static List<VariantSampleDetail> getSamplesForVariant(long variantRgdId, List<VariantSampleDetail> samples) {
        List<VariantSampleDetail> sampleDetails = new ArrayList<>();
        for (VariantSampleDetail vsd : samples) {
            if (vsd.getId() == variantRgdId) {
                sampleDetails.add(vsd);
            }
        }
        return sampleDetails;
    }

    public static void awaitTermination(ExecutorService executor) {
        executor.shutdown();
        try {
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            e.printStackTrace();
        }
    }
}
