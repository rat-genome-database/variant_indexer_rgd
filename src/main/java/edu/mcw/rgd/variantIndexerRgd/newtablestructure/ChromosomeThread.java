package edu.mcw.rgd.variantIndexerRgd.newtablestructure;

import edu.mcw.rgd.variantIndexerRgd.Manager;
import edu.mcw.rgd.variantIndexerRgd.dao.VariantDao;
import edu.mcw.rgd.variantIndexerRgd.process.MyThreadPoolExecutor;
import org.apache.logging.log4j.Logger;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.apache.logging.log4j.LogManager.getLogger;

public class ChromosomeThread  implements Runnable{
    private final String chr;
    private final int mapKey;
    private final int speciesTypeKey;
    static Logger log=getLogger(Manager.class);

    public ChromosomeThread(String chr, int mapKey, int speciesTypeKey){
        this.chr=chr;
        this.mapKey=mapKey;
        this.speciesTypeKey=speciesTypeKey;
    }
    @Override
    public void run() {
        log.info("########### Started Chromosome:"+chr);
        VariantDao variantDao = new VariantDao();
        List<Integer> variantIds;
        try {
            variantIds = variantDao.getUniqueVariantsIds(chr, mapKey, speciesTypeKey);
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }
        log.info("UNIQUE VARIANTS SIZE of CHR:" + chr + ":\t" + variantIds.size());
        ExecutorService executor = new MyThreadPoolExecutor(10, 10, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
        Collection[] collections = VariantIndexUtils.split(variantIds, 1000);
        for (int i = 0; i < collections.length; i++) {
            Runnable variantsNewTableThread=new VariantsNewTableThread(mapKey, (List<Integer>) collections[i]);
            executor.execute(variantsNewTableThread);
        }
        log.info("############# END Chromosome:"+ chr);
        VariantIndexUtils.awaitTermination(executor);
    }
}
