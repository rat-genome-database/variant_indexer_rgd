package edu.mcw.rgd.variantIndexerRgd.model;

import edu.mcw.rgd.datamodel.variants.VariantTranscript;

import java.math.BigDecimal;
import java.util.List;

/**
 * Created by jthota on 11/15/2019.
 */
public class VariantIndex {
    private long variant_id;
    private String rsId;
    private String clinvarId;
    private String chromosome;
    private String  paddingBase;
    private long endPos;
    private String refNuc;
    private int sampleId;
    private long startPos;
    private int totalDepth;
    private int varFreq;
    private String variantType;
    private String varNuc;
    private String zygosityStatus;
    private String genicStatus;
    private double zygosityPercentRead;
    private String zygosityPossError;
    private String zygosityRefAllele;
    private int zygosityNumAllele;
    private String zygosityInPseudo;
    private int qualityScore;
    private String HGVSNAME;
    private List<String> regionName;
    private List<String> regionNameLc;
    /*****************Sample******************/
   private String  analysisName;
   private int mapKey;
   private List<VariantTranscript> variantTranscripts;
   private List<String> conScores;
   private String dbsSnpName;

    public String getClinvarId() {
        return clinvarId;
    }

    public void setClinvarId(String clinvarId) {
        this.clinvarId = clinvarId;
    }

    public List<String> getRegionName() {
        return regionName;
    }

    public void setRegionName(List<String> regionName) {
        this.regionName = regionName;
    }

    public List<String> getRegionNameLc() {
        return regionNameLc;
    }

    public void setRegionNameLc(List<String> regionNameLc) {
        this.regionNameLc = regionNameLc;
    }

    public String getRsId() {
        return rsId;
    }

    public void setRsId(String rsId) {
        this.rsId = rsId;
    }

    public String getDbsSnpName() {
        return dbsSnpName;
    }

    public void setDbsSnpName(String dbsSnpName) {
        this.dbsSnpName = dbsSnpName;
    }

    public long getVariant_id() {
        return variant_id;
    }

    public void setVariant_id(long variant_id) {
        this.variant_id = variant_id;
    }

    public String getChromosome() {
        return chromosome;
    }

    public void setChromosome(String chromosome) {
        this.chromosome = chromosome;
    }

    public String getPaddingBase() {
        return paddingBase;
    }

    public void setPaddingBase(String paddingBase) {
        this.paddingBase = paddingBase;
    }

    public long getEndPos() {
        return endPos;
    }

    public void setEndPos(long endPos) {
        this.endPos = endPos;
    }

    public String getRefNuc() {
        return refNuc;
    }

    public void setRefNuc(String refNuc) {
        this.refNuc = refNuc;
    }

    public int getSampleId() {
        return sampleId;
    }

    public void setSampleId(int sampleId) {
        this.sampleId = sampleId;
    }

    public long getStartPos() {
        return startPos;
    }

    public void setStartPos(long startPos) {
        this.startPos = startPos;
    }

    public int getTotalDepth() {
        return totalDepth;
    }

    public void setTotalDepth(int totalDepth) {
        this.totalDepth = totalDepth;
    }

    public int getVarFreq() {
        return varFreq;
    }

    public void setVarFreq(int varFreq) {
        this.varFreq = varFreq;
    }



    public String getVariantType() {
        return variantType;
    }

    public void setVariantType(String variantType) {
        this.variantType = variantType;
    }

    public String getVarNuc() {
        return varNuc;
    }

    public void setVarNuc(String varNuc) {
        this.varNuc = varNuc;
    }

    public String getZygosityStatus() {
        return zygosityStatus;
    }

    public void setZygosityStatus(String zygosityStatus) {
        this.zygosityStatus = zygosityStatus;
    }

    public String getGenicStatus() {
        return genicStatus;
    }

    public void setGenicStatus(String genicStatus) {
        this.genicStatus = genicStatus;
    }

    public double getZygosityPercentRead() {
        return zygosityPercentRead;
    }

    public void setZygosityPercentRead(double zygosityPercentRead) {
        this.zygosityPercentRead = zygosityPercentRead;
    }

    public String getZygosityPossError() {
        return zygosityPossError;
    }

    public void setZygosityPossError(String zygosityPossError) {
        this.zygosityPossError = zygosityPossError;
    }

    public String getZygosityRefAllele() {
        return zygosityRefAllele;
    }

    public void setZygosityRefAllele(String zygosityRefAllele) {
        this.zygosityRefAllele = zygosityRefAllele;
    }

    public int getZygosityNumAllele() {
        return zygosityNumAllele;
    }

    public void setZygosityNumAllele(int zygosityNumAllele) {
        this.zygosityNumAllele = zygosityNumAllele;
    }

    public String getZygosityInPseudo() {
        return zygosityInPseudo;
    }

    public void setZygosityInPseudo(String zygosityInPseudo) {
        this.zygosityInPseudo = zygosityInPseudo;
    }

    public int getQualityScore() {
        return qualityScore;
    }

    public void setQualityScore(int qualityScore) {
        this.qualityScore = qualityScore;
    }

    public String getHGVSNAME() {
        return HGVSNAME;
    }

    public void setHGVSNAME(String HGVSNAME) {
        this.HGVSNAME = HGVSNAME;
    }

    public String getAnalysisName() {
        return analysisName;
    }

    public void setAnalysisName(String analysisName) {
        this.analysisName = analysisName;
    }

    public int getMapKey() {
        return mapKey;
    }

    public void setMapKey(int mapKey) {
        this.mapKey = mapKey;
    }

    public List<VariantTranscript> getVariantTranscripts() {
        return variantTranscripts;
    }

    public void setVariantTranscripts(List<VariantTranscript> variantTranscripts) {
        this.variantTranscripts = variantTranscripts;
    }

    public List<String> getConScores() {
        return conScores;
    }

    public void setConScores(List<String> conScores) {
        this.conScores = conScores;
    }

 /*   public String toString(){
        StringBuilder sb=new StringBuilder();
        sb.append("{");
        if(variant_id!=0){
            sb.append("\"variant_id\":").append(variant_id);
        }
        if(rsId!=null){
            sb.append(", \"rsId\":\"").append(rsId).append("\"");
        }
        if(clinvarId!=null){
            sb.append(", \"clinvarId\":\"").append(clinvarId).append("\"");
        }
        if(chromosome!=null){
            sb.append(", \"chromosome\":\"").append(chromosome).append("\"");
        }
        if(paddingBase!=null){
            sb.append(", \"paddingBase\":\"").append(paddingBase).append("\"");
        }
        if(endPos>-1){
            sb.append(", \"endPos\":").append(endPos);
        }
        if(refNuc!=null){
            sb.append(", \"refNuc\":\"").append(refNuc).append("\"");
        }
        if(sampleId>0){
            sb.append(", \"sampleId\":").append(sampleId);
        }
        if(startPos>-1){
            sb.append(", \"startPos\":").append(startPos);
        }
        if(analystFlag!=null){
            sb.append(", \"analystFlag\":\"").append(analystFlag).append("\"");
        }
        if(variantType!=null){
            sb.append(", \"variantType\":\"").append(variantType).append("\"");
        }
        if(varNuc!=null){
            sb.append(", \"varNuc\":\"").append(varNuc).append("\"");
        }
        if(zygosityStatus!=null){
            sb.append(", \"zygosityStatus\":\"").append(zygosityStatus).append("\"");
        } if(genicStatus!=null){
            sb.append(", \"genicStatus\":\"").append(genicStatus).append("\"");
        } if(zygosityPossError!=null){
            sb.append(", \"zygosityPossError\":\"").append(zygosityPossError).append("\"");
        }
        if(zygosityRefAllele!=null){
            sb.append(", \"zygosityRefAllele\":\"").append(zygosityRefAllele).append("\"");
        }
        if(zygosityInPseudo!=null){
            sb.append(", \"zygosityInPseudo\":\"").append(zygosityInPseudo).append("\"");
        }
        if(HGVSNAME!=null){
            sb.append(", \"HGVSNAME\":\"").append(HGVSNAME).append("\"");
        }
        if(analysisName!=null){
            sb.append(", \"analysisName\":\"").append(analysisName).append("\"");
        }
        if(gender!=null){
            sb.append(", \"gender\":\"").append(gender).append("\"");
        }
        if(dbsSnpName!=null){
            sb.append(", \"dbsSnpName\":\"").append(dbsSnpName).append("\"");
        }
        if(totalDepth>0){
            sb.append(", \"totalDepth\":").append(totalDepth);
        }
        if(varFreq>0){
            sb.append(", \"varFreq\":").append(varFreq);
        }
        if(zygosityPercentRead>0){
            sb.append(", \"zygosityPercentRead\":").append(zygosityPercentRead);
        }
        if(zygosityNumAllele>0){
            sb.append(", \"zygosityNumAllele\":").append(zygosityNumAllele);
        }
        if(qualityScore>0){
            sb.append(", \"qualityScore\":").append(qualityScore);
        }
        if(patientId>0){
            sb.append(", \"patientId\":").append(patientId);
        }
        if(mapKey>0){
            sb.append(", \"mapKey\":").append(mapKey);
        }
        if(strainRgdId>0){
            sb.append(", \"strainRgdId\":").append(strainRgdId);
        }
        if(regionName!=null && regionName.size()>0){
            sb.append(", \"regionName\":[");
            boolean first=true;
            for(String s:regionName){
                if(first) {
                    sb.append("\""+s+"\"");
                    first=false;
                }else {
                    sb.append(",\""+s+"\"");
                }
            }
            sb.append("]");
        }
        if(regionNameLc!=null && regionNameLc.size()>0){
            sb.append(", \"regionNameLc\":[");
            boolean first=true;
            for(String s:regionNameLc){
                if(first) {
                    sb.append("\""+s+"\"");
                    first=false;
                }else {
                    sb.append(",\""+s+"\"");
                }
            }
            sb.append("]");
        }
        if(variantTranscripts!=null && variantTranscripts.size()>0){
            sb.append(", \"variantTranscripts\":[");
            boolean first=true;
            for(VariantTranscript t: variantTranscripts){
                if(first) {
                    sb.append("{");
                    first=false;
                }else {
                    sb.append(",{");
                }
                if(t.getTranscriptRgdId()>0)
                sb.append("\"transcriptRgdId\":"+t.getTranscriptRgdId());
                if(t.getRefAA()!=null){
                    sb.append(",\"refAA\":\""+t.getRefAA()+"\"");

                }
                if(t.getVarAA()!=null){
                    sb.append(",\"varAA\":\""+t.getVarAA()+"\"");

                }
                if(t.getGenespliceStatus()!=null){
                    sb.append(",\"genespliceStatus\":\""+t.getGenespliceStatus()+"\"");

                }
                if(t.getPolyphenStatus()!=null){
                    sb.append(",\"polyphenStatus\":\""+t.getPolyphenStatus()+"\"");

                }
                if(t.getSynStatus()!=null){
                    sb.append(",\"synStatus\":\""+t.getSynStatus()+"\"");

                }
                if(t.getLocationName()!=null){
                    sb.append(",\"locationName\":\""+t.getLocationName()+"\"");

                }
                if(t.getNearSpliceSite()!=null){
                    sb.append(",\"nearSpliceSite\":\""+t.getNearSpliceSite()+"\"");

                }
                if(t.getTripletError()!=null){
                    sb.append(",\"tripletError\":\""+t.getTripletError()+"\"");

                }
                if(t.getFrameShift()!=null){
                    sb.append(",\"frameShift\":\""+t.getFrameShift()+"\"");

                }
                if(t.getFullRefNucSeqKey()>0){
                    sb.append(",\"fullRefNucSeqKey\":"+t.getFullRefNucSeqKey());

                }
                if(t.getFullRefNucPos()>0){
                    sb.append(",\"fullRefNucPos\":"+t.getFullRefNucPos());

                }
                if(t.getFullRefAASeqKey()>0){
                    sb.append(",\"fullRefAASeqKey\":"+t.getFullRefAASeqKey());

                }
                if(t.getFullRefAAPos()>0){
                    sb.append(",\"fullRefAAPos\":"+t.getFullRefAAPos());

                }

                    sb.append("}");

            }
            sb.append("]");
        }
        if(conScores!=null && conScores.size()>0){
            sb.append(", \"conScores\":[");
            boolean first=true;
            for(String s:conScores){
                if(first) {
                    sb.append(s);
                    first=false;
                }else {
                    sb.append(","+s);
                }
            }
            sb.append("]");
        }
        sb.append("}");
        return sb.toString();
    }*/
}
