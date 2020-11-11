package edu.mcw.rgd.variantIndexerRgd;

import htsjdk.samtools.util.CloseableIterator;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFFileReader;

import java.io.File;

/**
 * Created by jthota on 12/19/2019.
 */

/**
 * HTSProcess is a High Sequence Troughput processing pipeline, reads VCF file
 * and processes the variants.
 */
public class HTSProcess {
    public static void main(String[] args){
        VCFFileReader r= new VCFFileReader(new File(args[0]), false);
        CloseableIterator<VariantContext> t=r.iterator();
        System.out.println("ID"+"\t"+"CHR"+"\t"+"START_POS"+"\t"+"REFNUC"+
                "\t"+"VARNUC"+"\t"+"ALLELES"+"\t"+ "QUAL"+"\t"+ "NA18537"+
                "\t"+ "ZYGOSITY_TYPE_NA18537"+
        "\t"+ "HG00096" +"\t"+"ZYGOSITY_TYPE_HG00096");
        while(t.hasNext()){
            VariantContext ctx=t.next();
            System.out.println(ctx.getID()+"\t"+ctx.getContig()+"\t"+ctx.getStart()+"\t"+ctx.getReference()+
                    "\t"+ctx.getAlternateAlleles()+"\t"+ctx.getAlleles()+"\t"+  ctx.getCommonInfo().getPhredScaledQual() +
                  //  "\t"+ctx.getGenotype("NA18537")+
                    "\t"+ ctx.getGenotype("NA18537").getGenotypeString()+
                    "\t"+ ctx.getGenotype("NA18537").getType()+
                  //  "\t"+ctx.getGenotype("HG00096") +
                    "\t"+ctx.getGenotype("HG00096").getGenotypeString()+
                    "\t"+ ctx.getGenotype("HG00096").getType());
        }
        t.close();
        r.close();
        System.out.println("Done!!");
    }

}
