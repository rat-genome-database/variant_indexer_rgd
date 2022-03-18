package edu.mcw.rgd.variantIndexerRgd.newtablestructure;

import edu.mcw.rgd.variantIndexerRgd.dao.VariantDao;
import edu.mcw.rgd.variantIndexerRgd.process.MyThreadPoolExecutor;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class ChromosomeThread  implements Runnable{
    private String chr;
    private int mapKey;
    private int speciesTypeKey;
    VariantDao variantDao=new VariantDao();
    public ChromosomeThread(String chr, int mapKey, int speciesTypeKey){
        this.chr=chr;
        this.mapKey=mapKey;
        this.speciesTypeKey=speciesTypeKey;
    }
    @Override
    public void run() {
        ExecutorService executor2 = new MyThreadPoolExecutor(5, 5, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
        Runnable variantsNewTableThread= null;
        List<Integer> variantIds = null;
        try {
            variantIds = variantDao.getUniqueVariantsIds(chr, mapKey, speciesTypeKey);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("UNIQUE VAIANTS SIZE of CHR:" + chr + ":\t" + variantIds.size());
        Collection[] collections = new Collection[0];
        try {
            collections = split(variantIds, 1000);
        } catch (Exception e) {
            e.printStackTrace();
        }
        for (int i = 0; i < collections.length; i++) {
            variantsNewTableThread=new VariantsNewTableThread(mapKey, (List<Integer>) collections[i]);
            executor2.execute(variantsNewTableThread);
        }
        executor2.shutdown();
        while (!executor2.isTerminated()) {}
    }
    public Collection[] split(List<Integer> rgdids, int size) throws Exception {
        int numOfBatches = rgdids.size() / size + 1;
        Collection[] batches = new Collection[numOfBatches];

        for(int index = 0; index < numOfBatches; ++index) {
            int count = index + 1;
            int fromIndex = Math.max((count - 1) * size, 0);
            int toIndex = Math.min(count * size, rgdids.size());
            batches[index] = rgdids.subList(fromIndex, toIndex);
        }

        return batches;
    }
}
