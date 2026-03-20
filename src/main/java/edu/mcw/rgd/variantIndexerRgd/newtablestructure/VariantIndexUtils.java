package edu.mcw.rgd.variantIndexerRgd.newtablestructure;

import edu.mcw.rgd.dao.impl.GeneLociDAO;
import edu.mcw.rgd.datamodel.GeneLoci;


import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

public class VariantIndexUtils {


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
