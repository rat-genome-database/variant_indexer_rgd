package edu.mcw.rgd.variantIndexerRgd.process;

/**
 * Created by jthota on 1/9/2020.
 */
public class IndexObjectProcess  {
  /*  VariantLoad3 loader=new VariantLoad3();
    Zygosity zygosity=new Zygosity();
    public List<VariantIndex> getIndexObjects(List<CommonFormat2Line> list, GeneCache geneCache){
        List<VariantIndex> objects= new ArrayList<>();
        for (CommonFormat2Line line : list) {

            boolean isSnv = !Utils.isStringEmpty(line.getRefNuc()) && !Utils.isStringEmpty(line.getVarNuc());
            long endPos = 0;
            if (isSnv) {
                endPos = line.getPos() + 1;
            } else {
                // insertions
                if (Utils.isStringEmpty(line.getRefNuc())) {
                    endPos = line.getPos();
                }
                // deletions
                else if (Utils.isStringEmpty(line.getVarNuc())) {
                    endPos = line.getPos() + line.getRefNuc().length();
                } else {
                    System.out.println("Unexpected var type");
                }
            }
            if (!loader.alleleIsValid(line.getRefNuc())) {
                //   System.out.println(" *** Ref Nucleotides must be A,C,G,T,N");
                continue;
            }
            if (!loader.alleleIsValid(line.getVarNuc())) {
                //     System.out.println(" *** Var Nucleotides must be A,C,G,T,N");
                continue;
            }


            List<BasicTranscriptData> variantTranscripts = new ArrayList<>();

            try {
                variantTranscripts.addAll(loader.getVariantTranscripts(line.getPos(), line.getChr()));

            } catch (IOException e) {
                e.printStackTrace();
            }

            // NOTE: for snvs, only ACGT counts are provided
            //    for indels, only allele count is provided
            int alleleDepth = line.getAlleleDepth(); // from AD field: how many times allele was called
            int readDepth = line.getReadDepth(); // from AD field: how many times all alleles were called
            int readCountA = line.getCountA();
            int readCountC = line.getCountC();
            int readCountG = line.getCountG();
            int readCountT = line.getCountT();

            int totalDepth = 0;
            String totalDepthStr = line.getTotalDepth().toString();
            if (totalDepthStr == null || totalDepthStr.isEmpty()) {
                if (isSnv)
                    totalDepth = readCountA + readCountC + readCountG + readCountT;
                else
                    totalDepth = readDepth;
            } else
                totalDepth = Integer.parseInt(totalDepthStr);

            // total reads called (AD field) vs total reads analyzed (DP field): 100*readDepth/totalDepth
            int qualityScore = 0;
            if (totalDepth > 0) {
                qualityScore = (100 * readDepth + totalDepth / 2) / totalDepth;
            }



            List<Integer> geneRgdIds = geneCache.getGeneRgdIds(line.getPos());
            String genicStatus = !geneRgdIds.isEmpty() ? "GENIC" : "INTERGENIC";
            for (String s : line.getStrainList()) {
                Sample sample = VariantIndexerThread.sampleIdMap.get(s);

                VariantIndex v = new VariantIndex();
                v.setChromosome(line.getChr());
                v.setRefNuc(line.getRefNuc());
                v.setStartPos(line.getPos());
                v.setTotalDepth(line.getTotalDepth());
                v.setVarFreq(line.getAlleleDepth());
                v.setVarNuc(line.getVarNuc());
                v.setSampleId(sample.getId());
                v.setAnalysisName(sample.getAnalysisName());
                v.setPatientId(sample.getPatientId());
                v.setGender(sample.getGender());
                v.setQualityScore(qualityScore);
                v.setHGVSNAME(line.getHgvsName());
                v.setStrainRgdId(sample.getStrainRgdId());
                v.setVariantType(loader.determineVariantType(line.getRefNuc(), line.getVarNuc()));

                v.setGenicStatus(genicStatus);
                if (!isSnv) {
                    v.setPaddingBase(line.getPaddingBase());
                }

                // Determine the ending position

                v.setEndPos(endPos);

                int score = 0;
                if (isSnv) {
                    score = zygosity.computeVariant(readCountA, readCountC, readCountG, readCountT, sample.getGender(), v);
                } else {
                    //         // parameter tweaking for indels
                    zygosity.computeZygosityStatus(alleleDepth, readDepth, sample.getGender(), v);
//
                    // compute zygosity ref allele, if possible
                    if (line.getRefNuc().equals("A")) {
                        v.setZygosityRefAllele(readCountA > 0 ? "Y" : "N");
                    } else if (line.getRefNuc().equals("C")) {
                        v.setZygosityRefAllele(readCountC > 0 ? "Y" : "N");
                    } else if (line.getRefNuc().equals("G")) {
                        v.setZygosityRefAllele(readCountG > 0 ? "Y" : "N");
                    } else if (line.getRefNuc().equals("T")) {
                        v.setZygosityRefAllele(readCountT > 0 ? "Y" : "N");
                    }

                    if (alleleDepth == 0)
                        score = 0;
                    else
                        score = (int) v.getZygosityPercentRead();
                }
                if (score == 0) {
                    continue;
                }


                v.setRsId(line.getRsId());
                List<String> regionNames=geneLociMap.get(v.getStartPos());
                if(regionNames!=null && regionNames.size()!=0) {
                    v.setRegionName(regionNames);
                    List<String> regionNameLC = new ArrayList<>();
                    for (String name : regionNames) {
                        regionNameLC.add(name);
                    }
                    v.setRegionNameLc(regionNameLC);
                }
                if(variantTranscripts!=null && variantTranscripts.size()!=0)
                    v.setVariantTranscripts(variantTranscripts);
                objects.add(v);
            }

        }
        return objects;
        //  return null;
    }*/
}
