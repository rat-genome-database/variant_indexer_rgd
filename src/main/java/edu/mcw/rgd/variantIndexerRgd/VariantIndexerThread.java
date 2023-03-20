package edu.mcw.rgd.variantIndexerRgd;

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.mcw.rgd.dao.impl.MapDAO;
import edu.mcw.rgd.datamodel.Chromosome;
import edu.mcw.rgd.datamodel.Sample;
import edu.mcw.rgd.variantIndexerRgd.model.CommonFormat2Line;
import edu.mcw.rgd.variantIndexerRgd.process.GeneCache;

import java.util.*;

/**
 * Created by jthota on 11/15/2019.
 */
public class VariantIndexerThread{
        //implements Runnable {
 /*  private Thread t;
    private String objectType;
    private String index;
    private String line;
    private int lineCount;
    private String[] header;*/
   boolean processLinesWithMissingADDP=true;
    boolean processVariantsSameAsRef=false ;
 //  private int strainCount;
 //   private GeneCache geneCache;
    public static Map<String, Sample> sampleIdMap;
 //   public static Map<Long, List<String>> geneLociMap;
 //   private String processType;
//   public static ObjectMapper mapper;
    public VariantIndexerThread(){}
 /* public VariantIndexerThread(String line, int strainCount, String[] header, GeneCache geneCache, String processType, int lineCount){


        this.lineCount=lineCount;
        processLinesWithMissingADDP = true;
        processVariantsSameAsRef = false;
        this.line=line;
        this.strainCount=strainCount;
        this.header=header;
        this.geneCache=geneCache;
        this.processType=processType;
    }*/
// public void run() {
        public  List<CommonFormat2Line> run(String line, int strainCount, String[] header, GeneCache geneCache, String processType) {

        //    System.out.println(Thread.currentThread().getName()  + " ||  "+ processType+"started .... " + new Date());

        String[] v = line.split("[\\t]", -1);

        if (v.length == 0 || v[0].length() == 0 || v[0].charAt(0) == '#')
            //skip lines with "#"
            return null;

        // validate chromosome
        String chr = null;
        try {
            chr = getChromosome(v[0]);
        } catch (Exception e) {
            e.printStackTrace();

        }
        if( chr==null || chr.length()>2 ) {
            return null;
        }

        // variant pos
        int pos = Integer.parseInt(v[1]);
        String rsId=v[2];
        String refNuc = v[3];
        String alleles = v[4];
        //   log.info(Thread.currentThread().getName() + ":rsId " + rsId + " started " + new Date());
        // get index of GQ - genotype quality
        String[] format = v[8].split(":");
        int ADindex = readADindex(format);
        int DPindex = readDPindex(format);
        if( ADindex < 0 || DPindex<0 ) {
            if( !processLinesWithMissingADDP ) {
                return null;
            }
        }

        // rgdid and hgvs name
        Integer rgdId = null;
        String hgvsName = null;
        String id = v[2];
            List<CommonFormat2Line> lines= new ArrayList<>();
            if(processType.equalsIgnoreCase("variants")) {
                List<String> strains = new ArrayList<>();
               // System.out.println("SAMPLES SIZE :"+ VariantIndexerThread.sampleIdMap.size());
                for (int i = 9; i < 9 + strainCount && i < v.length; i++) {
                    //     System.out.println(i);
                    String strain = header[i];
                    String genotype = new String();

                    try {
                        if(v[i].length()>3)
                           genotype= v[i].substring(0, 3); // GT for diploid
                        else
                        genotype= v[i]; // GT for haploid
                        if (!genotype.equals("./.") && !genotype.equals("0/0") && !genotype.trim().equals("") && !genotype.trim().equals(".") && !genotype.contains("/")) {
                       //     System.out.println(strain);
                            if (VariantIndexerThread.sampleIdMap.get(strain) != null) {
                                strains.add(strain);

                            }
                        }

                    } catch (Exception e) {
                        System.out.println("POSITION: " + pos + "\tGENOTYPE:" + genotype);
                        e.printStackTrace();
                    }


                }
            //    System.out.println("strain COUNT: "+ strains.size());
                try {
                    // if(pos!=9411264)

                    lines.addAll(processStrains(chr, pos, refNuc, alleles, rgdId, hgvsName,rsId, strains,geneCache,processType ));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }else{
                if(processType.equalsIgnoreCase("transcripts")) {
                    try {
                        // if(pos!=9411264)

                        lines.addAll(processStrains(chr, pos, refNuc, alleles, rgdId, hgvsName, rsId, null, geneCache, processType));
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
          //  VariantProcessThread vp=new VariantProcessThread();

         /*   if(lines.size()>0){
               vp.run(lines, geneCache, lineCount);
            }*/
     //       System.out.println(Thread.currentThread().getName() +"\tCHR: "+chr+"\tPOS: "+ pos +"\t COMPLETED" );
        return lines;
    }




    List<CommonFormat2Line> processStrains(String chr, int pos, String refNuc, String alleleString, Integer rgdId, String hgvsName, String rsId, List<String> strains, GeneCache geneCache, String processType) throws Exception {


        // read counts for all alleles, as determined by genotype
        int[] readCount = null;
        // format is in 0/1:470,63:533:99:507,0,3909
        int readDepth = 0;

      if( processLinesWithMissingADDP ) {
            readDepth = 9;
            readCount = new int[] {9, 9, 9, 9, 9, 9, 9, 9};
        }


        int totalDepth = 0;

       if( processLinesWithMissingADDP ) {
            totalDepth = 9;
       }


        // for the single Reference to multiple Variants
        int alleleCount = getAlleleCount(alleleString);
        String[] alleles = (refNuc+","+alleleString).split(",");



        List<CommonFormat2Line> lines= new ArrayList<>();
        // for every allele, including refNuc
        for (String allele: alleles ) {
            // skip the line variant if it is the same with reference (unless an override is specified)
            if( !processVariantsSameAsRef && refNuc.equals(allele) ) {
                continue;
            }

            CommonFormat2Line line = new CommonFormat2Line();
            line.setChr(chr);
            line.setPos(pos);
            line.setRefNuc(refNuc);
            line.setVarNuc(allele);
            line.setRsId(rsId);
            line.setCountA(getReadCountForAllele("A", alleles, readCount));
            line.setCountC(getReadCountForAllele("C", alleles, readCount));
            line.setCountG(getReadCountForAllele("G", alleles, readCount));
            line.setCountT(getReadCountForAllele("T", alleles, readCount));
            if( totalDepth>0 )
                line.setTotalDepth(totalDepth);
            line.setAlleleDepth(getReadCountForAllele(allele, alleles, readCount));
            line.setAlleleCount(alleleCount);
            line.setReadDepth(readDepth);
            line.setRgdId(rgdId);
            line.setHgvsName(hgvsName);
            //    line.setStrains(sb.toString());
            if(strains!=null)
            line.setStrainList(strains);

            // insertion
            if( refNuc.length()==1 && allele.length()>1 ) {
                // varNuc first base must be the same as refNuc
                if( refNuc.charAt(0)!=allele.charAt(0) ) {
                //    System.out.println("Unexpected: insertion: missing padding base");
                  continue;
                }
                // handle padding base
                line.setPaddingBase(refNuc);
                line.setRefNuc(null);
                line.setVarNuc(allele.substring(1));

                pos++;
                line.setPos(pos);

            }
            // deletion
            if( refNuc.length()>1 && allele.length()==1 ) {
                // varNuc first base must be the same as refNuc
                if( refNuc.charAt(0)!=allele.charAt(0) ) {
                //    System.out.println("Unexpected: deletion: missing padding base");
                   continue;
                }
                // handle padding base
                line.setPaddingBase(allele);
                line.setVarNuc(null);
                line.setRefNuc(refNuc.substring(1));
                pos++;
                line.setPos(pos);

            }
            lines.add(line);

        }
        return lines;
    }
    int getReadCountForAllele(String allele, String[] alleles, int[] readCount) {

        for( int i=0; i<alleles.length; i++ ) {
            if( alleles[i].equals(allele) )

                    return readCount[0];

        }
        return 0;
    }

    int getAlleleCount(String s) {
        int alleleCount = 1;
        for( int i=0; i<s.length(); i++ ) {
            if( s.charAt(i)==',' )
                alleleCount++;
        }
        return alleleCount;
    }

    int readADindex(String[] format) {

        // format : "GT:AD:DP:GQ:PL"
        for( int i=0; i<format.length; i++ ) {
            if( format[i].equals("AD") ) {
                return i;
            }
        }

        // try CLCAD2
        for( int i=0; i<format.length; i++ ) {
            if( format[i].equals("CLCAD2") ) {
                return i;
            }
        }
        return -1;
    }

    int readDPindex(String[] format) {

        // format : "GT:AD:DP:GQ:PL"
        // determine which position separated by ':' occupies
        for( int i=0; i<format.length; i++ ) {
            if( format[i].equals("DP") ) {
                return i;
            }
        }
        return -1;
    }
    String getChromosome(String chr) throws Exception {

        String c = getChromosomeImpl(chr);
        if( c!=null && c.equals("M") ) {
            c = "MT";
        }
        return c;
    }
    String getChromosomeImpl(String chr) throws Exception {
        // chromosomes could be provided as 'NC_005100.4'
        if( chr.startsWith("NC_") ) {
            return getChromosomeFromDb(chr);
        }

        chr = chr.replace("chr", "").replace("c", "");
        // skip lines with invalid chromosomes (chromosome length must be 1 or 2
        if( chr.length()>2 || chr.contains("r") || chr.equals("Un") ) {
            return null;
        }
        return chr;
    }
    String getChromosomeFromDb(String refseqNuc) throws Exception {
        String chr = _chromosomeMap.get(refseqNuc);
        if( chr==null ) {
            MapDAO dao = new MapDAO();
            Chromosome c = dao.getChromosome(refseqNuc);
            if( c!=null ) {
                chr = c.getChromosome();
                _chromosomeMap.put(refseqNuc, chr);
            }
        }
        return chr==null ? null : chr;
    }
    Map<String,String> _chromosomeMap = new HashMap<>();


}
