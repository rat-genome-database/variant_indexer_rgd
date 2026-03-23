package edu.mcw.rgd.variantIndexerRgd.newtablestructure;

import edu.mcw.rgd.variantIndexerRgd.dao.VariantDao;
import edu.mcw.rgd.variantIndexerRgd.process.MyThreadPoolExecutor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class ChromosomeThread implements Runnable {
    private static final Logger log = LogManager.getLogger(ChromosomeThread.class);

    private final String chr;
    private final int mapKey;
    private final int speciesTypeKey;
    private final VariantDao variantDao;

    public ChromosomeThread(String chr, int mapKey, int speciesTypeKey, VariantDao variantDao) {
        this.chr = chr;
        this.mapKey = mapKey;
        this.speciesTypeKey = speciesTypeKey;
        this.variantDao = variantDao;
    }

    @Override
    public void run() {
        log.info("Started Chromosome:" + chr);

        // Bounded queue of 20 batches — blocks the DB cursor when 20 batches are queued,
        // preventing memory from growing unbounded while workers catch up
        ExecutorService executor = new MyThreadPoolExecutor(10, 10, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(20));
        AtomicInteger totalVariants = new AtomicInteger(0);
        AtomicInteger batchCount = new AtomicInteger(0);

        try {
            // Stream pages of 1000 variant IDs directly from DB cursor — no upfront load of all IDs
            variantDao.processVariantIdsByPage(chr, mapKey, speciesTypeKey, 1000, (List<Integer> page) -> {
                totalVariants.addAndGet(page.size());
                int batch = batchCount.incrementAndGet();

                // Submit to thread pool; if queue is full, retry until accepted
                Runnable task = new VariantsNewTableThread(mapKey, page, variantDao);
                boolean submitted = false;
                while (!submitted) {
                    try {
                        executor.execute(task);
                        submitted = true;
                    } catch (java.util.concurrent.RejectedExecutionException e) {
                        try { Thread.sleep(100); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); return; }
                    }
                }

                if (batch % 50 == 0) {
                    log.info("CHR:" + chr + " submitted batch " + batch + " (" + totalVariants.get() + " variants so far)");
                }
            });
        } catch (Exception e) {
            log.error("Error processing chromosome " + chr, e);
        }

        log.info("CHR:" + chr + " all " + batchCount.get() + " batches submitted (" + totalVariants.get() + " total variants)");
        VariantIndexUtils.awaitTermination(executor);
        log.info("Finished Chromosome:" + chr);
    }
}
