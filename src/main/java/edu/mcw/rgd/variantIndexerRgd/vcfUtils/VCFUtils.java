package edu.mcw.rgd.variantIndexerRgd.vcfUtils;

import edu.mcw.rgd.variantIndexerRgd.model.CommonFormat2Line;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;

public class VCFUtils {
    public void parse(String fileName) throws Exception{
        System.out.println("FILE NAME:"+ fileName);
        File file = new File(fileName);
            BufferedReader reader;
            if (file.getName().endsWith(".txt.gz") || file.getName().endsWith(".vcf.gz")) {
                reader = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(file))));
            } else {
                // System.out.println("FILE: "+ file);
                reader = new BufferedReader(new FileReader(file));
            }
            String line;
            int lineCount = 0;
            String[] header = null;
            int strainCount = 0;
            List<CommonFormat2Line> lines = new ArrayList<>();
            int clusterCount = 0;
            File out=new File("data/outFile.txt");
            FileWriter writer=new FileWriter(out);
        //    ExecutorService executor= new MyThreadPoolExecutor(10,10,0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
            while ((line = reader.readLine()) != null) {
                // skip comment line
                if (line.startsWith("#")) {
                    header = line.substring(1).split("[\\t]", -1);
                    strainCount = header.length - 9;
                    writer.write(line);
                } else {
                    if(line.contains("CSQ"))
                        writer.write(line);
                    lineCount=lineCount+1;

                }
            }
            writer.close();
            reader.close();

    }
}
