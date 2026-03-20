package edu.mcw.rgd.variantIndexerRgd.newtablestructure;

import edu.mcw.rgd.variantIndexerRgd.dao.VariantDao;
import edu.mcw.rgd.variantIndexerRgd.process.MyThreadPoolExecutor;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class ChromosomeThread  implements Runnable{
    private final String chr;
    private final int mapKey;
    private final int speciesTypeKey;
    VariantDao variantDao=new VariantDao();
    public ChromosomeThread(String chr, int mapKey, int speciesTypeKey){
        this.chr=chr;
        this.mapKey=mapKey;
        this.speciesTypeKey=speciesTypeKey;
    }
    @Override
    public void run() {
        ExecutorService executor2 = new MyThreadPoolExecutor(5, 5, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
        List<Integer> variantIds = null;
        try {
            variantIds = variantDao.getUniqueVariantsIds(chr, mapKey, speciesTypeKey);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("UNIQUE VARIANTS SIZE of CHR:" + chr + ":\t" + variantIds.size());
        Collection[] collections = VariantIndexUtils.split(variantIds, 1000);
        for (int i = 0; i < collections.length; i++) {
            Runnable variantsNewTableThread=new VariantsNewTableThread(mapKey, (List<Integer>) collections[i]);
            executor2.execute(variantsNewTableThread);
        }
        VariantIndexUtils.awaitTermination(executor2);
    }
}
