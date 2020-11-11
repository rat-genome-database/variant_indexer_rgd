package edu.mcw.rgd.variantIndexerRgd.process;

/**
 * Created by jthota on 1/7/2020.
 */


import edu.mcw.rgd.datamodel.variants.VariantMapData;
import edu.mcw.rgd.datamodel.variants.VariantSampleDetail;
import edu.mcw.rgd.util.PseudoAutosomalRegion;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


public class Zygosity {
    public static String HOMOZYGOUS = "homozygous";
    public static String HETEROZYGOUS = "heterozygous";
    public static String POSSIBLY_HOMOZYGOUS = "possibly homozygous";
    public static String HEMIZYGOUS = "hemizygous";
    public static String PROBABLY_HEMIZYGOUS = "probably hemizygous";
    public static String POSSIBLY_HEMIZYGOUS = "possibly hemizygous";
    public static String TRUE = "Y";
    public static String FALSE = "N";
    public static int POSSIBLE_ERROR_PERCENT = 15;
    public static int POSSIBLY_HOMOZYGOUS_PERCENT = 85;
    public static int PROBABLY_HEMIZYGOUSE_PERCENT = 85;
    public static int HOMOZYGOUS_PERCENT = 100;
    protected final Log logger = LogFactory.getLog(this.getClass());

    public Zygosity() {
    }

    /** @deprecated */
    @Deprecated
 /*   public List<Variant> computeVariants(int scoreA, int scoreC, int scoreG, int scoreT, String gender, VariantIndex originalVariant) {
        float totalDepth = (float)(scoreA + scoreC + scoreG + scoreT);
        ArrayList returnList = new ArrayList();
        String origVarNuc = originalVariant.getVarNuc();
        float scoreAPerc = (float)(scoreA * 100) / totalDepth;
        float scoreCPerc = (float)(scoreC * 100) / totalDepth;
        float scoreGPerc = (float)(scoreG * 100) / totalDepth;
        float scoreTPerc = (float)(scoreT * 100) / totalDepth;
        String refNucl = originalVariant.getRefNuc();
        String chr = originalVariant.getChromosome();
        if(scoreAPerc > 0.0F && !refNucl.equals("A")) {
            VariantIndex variantA = originalVariant;
            if(!origVarNuc.equals("A")) {
               // variantA.setId(0L);
            }

            variantA.setVarNuc("A");
            this.computeZygosity(scoreAPerc, scoreAPerc, scoreCPerc, scoreGPerc, scoreTPerc, chr, gender, variantA);
            returnList.add(variantA);
        }

        if(scoreCPerc > 0.0F && !refNucl.equals("C")) {
            VariantIndex variantC = originalVariant;
            if(!origVarNuc.equals("C")) {
            //    variantC.setId(0L);
            }

            variantC.setVarNuc("C");
            this.computeZygosity(scoreCPerc, scoreAPerc, scoreCPerc, scoreGPerc, scoreTPerc, chr, gender, variantC);
            returnList.add(variantC);
        }

        if(scoreGPerc > 0.0F && !refNucl.equals("G")) {
            VariantIndex variantG = originalVariant;
            if(!origVarNuc.equals("G")) {
             //   variantG.setId(0L);
            }

            variantG.setVarNuc("G");
            this.computeZygosity(scoreGPerc, scoreAPerc, scoreCPerc, scoreGPerc, scoreTPerc, chr, gender, variantG);
            returnList.add(variantG);
        }

        if(scoreTPerc > 0.0F && !refNucl.equals("T")) {
            VariantIndex variantT = originalVariant;
            if(!origVarNuc.equals("T")) {
           //     variantT.setId(0L);
            }

            variantT.setVarNuc("T");
            this.computeZygosity(scoreTPerc, scoreAPerc, scoreCPerc, scoreGPerc, scoreTPerc, chr, gender, variantT);
            returnList.add(variantT);
        }

        return returnList.size() > 0?returnList:null;
    }*/

    public int computeVariant(int scoreA, int scoreC, int scoreG, int scoreT, String gender, VariantMapData md, VariantSampleDetail sd) {
        float totalDepth = (float)(scoreA + scoreC + scoreG + scoreT);
        String varNuc = md.getVariantNucleotide();
        float scoreAPerc = (float)(scoreA * 100) / totalDepth;
        float scoreCPerc = (float)(scoreC * 100) / totalDepth;
        float scoreGPerc = (float)(scoreG * 100) / totalDepth;
        float scoreTPerc = (float)(scoreT * 100) / totalDepth;
        String chr = md.getChromosome();
        if(varNuc.equals("A")) {
            this.computeZygosity(scoreAPerc, scoreAPerc, scoreCPerc, scoreGPerc, scoreTPerc, chr, gender, md, sd);
            return scoreA;
        } else if(varNuc.equals("C")) {
            this.computeZygosity(scoreCPerc, scoreAPerc, scoreCPerc, scoreGPerc, scoreTPerc, chr, gender, md,sd);
            return scoreC;
        } else if(varNuc.equals("G")) {
            this.computeZygosity(scoreGPerc, scoreAPerc, scoreCPerc, scoreGPerc, scoreTPerc, chr, gender, md,sd);
            return scoreG;
        } else if(varNuc.equals("T")) {
            this.computeZygosity(scoreTPerc, scoreAPerc, scoreCPerc, scoreGPerc, scoreTPerc, chr, gender, md,sd);
            return scoreT;
        } else {
            return 0;
        }
    }

    public void computeZygosityStatus(int score, int depth, String gender, VariantMapData md, VariantSampleDetail sd) {
        float scorePerc = depth == 0?0.0F:(float)score * 100.0F / (float)depth;
        sd.setZygosityPercentRead((int)(scorePerc + 0.5F));
        this.computeZygosityStatusPseudoError(scorePerc, md.getChromosome(), gender, md,sd);
    }

    private void computeZygosity(float myScore, float scoreAPerc, float scoreCPerc, float scoreGPerc, float scoreTPerc, String chr, String gender, VariantMapData md, VariantSampleDetail sd) {
        String refNuc = md.getReferenceNucleotide();
        String varNuc = md.getVariantNucleotide();
        this.computeZygosityStatusPseudoError(myScore, chr, gender, md, sd);
        int scoreCount = 0;
        if(scoreAPerc > 0.0F) {
            ++scoreCount;
        }

        if(scoreCPerc > 0.0F) {
            ++scoreCount;
        }

        if(scoreGPerc > 0.0F) {
            ++scoreCount;
        }

        if(scoreTPerc > 0.0F) {
            ++scoreCount;
        }

        sd.setZygosityNumberAllele(scoreCount);
        sd.setZygosityRefAllele(FALSE);
        if(refNuc.equals("A") && scoreAPerc > 0.0F) {
            sd.setZygosityRefAllele(TRUE);
        }

        if(refNuc.equals("C") && scoreCPerc > 0.0F) {
            sd.setZygosityRefAllele(TRUE);
        }

        if(refNuc.equals("G") && scoreGPerc > 0.0F) {
            sd.setZygosityRefAllele(TRUE);
        }

        if(refNuc.equals("T") && scoreTPerc > 0.0F) {
            sd.setZygosityRefAllele(TRUE);
        }

        if(varNuc.equals("A")) {
            sd.setZygosityPercentRead((int)(scoreAPerc + 0.5F));
        }

        if(varNuc.equals("C")) {
            sd.setZygosityPercentRead((int)(scoreCPerc + 0.5F));
        }

        if(varNuc.equals("G")) {
            sd.setZygosityPercentRead((int)(scoreGPerc + 0.5F));
        }

        if(varNuc.equals("T")) {
            sd.setZygosityPercentRead((int)(scoreTPerc + 0.5F));
        }

    }

    private void computeZygosityStatusPseudoError(float myScore, String chr, String gender, VariantMapData md,VariantSampleDetail sd) {
        if((gender.equals("M") || gender.equals("P")) && (chr.equals("X") || chr.equals("Y"))) {
            PseudoAutosomalRegion pseudo = new PseudoAutosomalRegion();
            if(pseudo.inPAR(md.getChromosome(), Long.valueOf(md.getStartPos()))) {
                if(myScore == (float)HOMOZYGOUS_PERCENT) {
                    sd.setZygosityStatus(HOMOZYGOUS);
                } else if(myScore >= (float)POSSIBLY_HOMOZYGOUS_PERCENT) {
                    sd.setZygosityStatus(POSSIBLY_HOMOZYGOUS);
                } else {
                    sd.setZygosityStatus(HETEROZYGOUS);
                }

                sd.setZygosityInPseudo(TRUE);
            } else {
                if(myScore == (float)HOMOZYGOUS_PERCENT) {
                    sd.setZygosityStatus(HEMIZYGOUS);
                } else if(myScore >= (float)PROBABLY_HEMIZYGOUSE_PERCENT) {
                    sd.setZygosityStatus(PROBABLY_HEMIZYGOUS);
                } else {
                    sd.setZygosityStatus(POSSIBLY_HEMIZYGOUS);
                }

                sd.setZygosityInPseudo(FALSE);
            }
        } else {
            if(myScore == (float)HOMOZYGOUS_PERCENT) {
                sd.setZygosityStatus(HOMOZYGOUS);
            } else if(myScore >= (float)POSSIBLY_HOMOZYGOUS_PERCENT) {
                sd.setZygosityStatus(POSSIBLY_HOMOZYGOUS);
            } else {
                sd.setZygosityStatus(HETEROZYGOUS);
            }

            sd.setZygosityInPseudo(FALSE);
        }

        if(myScore <= (float)POSSIBLE_ERROR_PERCENT) {
            sd.setZygosityPossibleError(TRUE);
        } else {
            sd.setZygosityPossibleError(FALSE);
        }

    }
}

