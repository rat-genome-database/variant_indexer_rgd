package edu.mcw.rgd.variantIndexerRgd.model;

import java.math.BigDecimal;

public class VariantData {
   private int variantRgdId ;
   private String varNuc;
   private String refNuc;
   private String variantTpe;
   private String rsId;
    private String clinvarId;
   private String chromosome;
   private int startPos;
   private int endPos;
   private String genicStatus;
   private String paddingBase;
   private int mapKey;
   private int sampleId;
   private int totatlDepth;
   private String source;
   private int varFreq;
   private String zygosityStatus;
   private int zygosityPercentRead;
   private String zygosityPossError;
   private String zygosityRefAllele;
   private int zygosityNumAllele;
   private String zygosityInPseudo;
   private int qualityScore;
    private int transcriptRgdId;
    private String refAA;
    private String varAA;
    private String synStatus;
    private String nearSpliceSite;
    private int fullRefNucPos;
    private int fulRefAAPos;
    private String locationName;
    private int fullRefNucSeqKey;
    private int fullRefAASeqKey;
    private String tripletError;
    private String frameShift;
    private String polyphenPrediction;
    private String dbsnpName;
    private BigDecimal conservationScore;

    public BigDecimal getConservationScore() {
        return conservationScore;
    }

    public void setConservationScore(BigDecimal conservationScore) {
        this.conservationScore = conservationScore;
    }

    public String getClinvarId() {
        return clinvarId;
    }

    public void setClinvarId(String clinvarId) {
        this.clinvarId = clinvarId;
    }

    public String getDbsnpName() {
        return dbsnpName;
    }

    public void setDbsnpName(String dbsnpName) {
        this.dbsnpName = dbsnpName;
    }

    public String getPolyphenPrediction() {
        return polyphenPrediction;
    }

    public void setPolyphenPrediction(String polyphenPrediction) {
        this.polyphenPrediction = polyphenPrediction;
    }

    public String getVarAA() {
        return varAA;
    }

    public void setVarAA(String varAA) {
        this.varAA = varAA;
    }

    public int getVariantRgdId() {
        return variantRgdId;
    }

    public void setVariantRgdId(int variantRgdId) {
        this.variantRgdId = variantRgdId;
    }

    public int getStartPos() {
        return startPos;
    }

    public void setStartPos(int startPos) {
        this.startPos = startPos;
    }

    public int getEndPos() {
        return endPos;
    }

    public void setEndPos(int endPos) {
        this.endPos = endPos;
    }

    public String getVarNuc() {
        return varNuc;
    }

    public void setVarNuc(String varNuc) {
        this.varNuc = varNuc;
    }

    public String getRefNuc() {
        return refNuc;
    }

    public void setRefNuc(String refNuc) {
        this.refNuc = refNuc;
    }

    public String getVariantTpe() {
        return variantTpe;
    }

    public void setVariantTpe(String variantTpe) {
        this.variantTpe = variantTpe;
    }

    public String getRsId() {
        return rsId;
    }

    public void setRsId(String rsId) {
        this.rsId = rsId;
    }

    public String getChromosome() {
        return chromosome;
    }

    public void setChromosome(String chromosome) {
        this.chromosome = chromosome;
    }



    public String getGenicStatus() {
        return genicStatus;
    }

    public void setGenicStatus(String genicStatus) {
        this.genicStatus = genicStatus;
    }

    public String getPaddingBase() {
        return paddingBase;
    }

    public void setPaddingBase(String paddingBase) {
        this.paddingBase = paddingBase;
    }

    public int getMapKey() {
        return mapKey;
    }

    public void setMapKey(int mapKey) {
        this.mapKey = mapKey;
    }

    public int getSampleId() {
        return sampleId;
    }

    public void setSampleId(int sampleId) {
        this.sampleId = sampleId;
    }

    public int getTotatlDepth() {
        return totatlDepth;
    }

    public void setTotatlDepth(int totatlDepth) {
        this.totatlDepth = totatlDepth;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public int getVarFreq() {
        return varFreq;
    }

    public void setVarFreq(int varFreq) {
        this.varFreq = varFreq;
    }

    public String getZygosityStatus() {
        return zygosityStatus;
    }

    public void setZygosityStatus(String zygosityStatus) {
        this.zygosityStatus = zygosityStatus;
    }

    public int getZygosityPercentRead() {
        return zygosityPercentRead;
    }

    public void setZygosityPercentRead(int zygosityPercentRead) {
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

    public int getTranscriptRgdId() {
        return transcriptRgdId;
    }

    public void setTranscriptRgdId(int transcriptRgdId) {
        this.transcriptRgdId = transcriptRgdId;
    }

    public String getRefAA() {
        return refAA;
    }

    public void setRefAA(String refAA) {
        this.refAA = refAA;
    }

    public String getSynStatus() {
        return synStatus;
    }

    public void setSynStatus(String synStatus) {
        this.synStatus = synStatus;
    }

    public String getNearSpliceSite() {
        return nearSpliceSite;
    }

    public void setNearSpliceSite(String nearSpliceSite) {
        this.nearSpliceSite = nearSpliceSite;
    }

    public int getFullRefNucPos() {
        return fullRefNucPos;
    }

    public void setFullRefNucPos(int fullRefNucPos) {
        this.fullRefNucPos = fullRefNucPos;
    }

    public int getFulRefAAPos() {
        return fulRefAAPos;
    }

    public void setFulRefAAPos(int fulRefAAPos) {
        this.fulRefAAPos = fulRefAAPos;
    }

    public String getLocationName() {
        return locationName;
    }

    public void setLocationName(String locationName) {
        this.locationName = locationName;
    }

    public int getFullRefNucSeqKey() {
        return fullRefNucSeqKey;
    }

    public void setFullRefNucSeqKey(int fullRefNucSeqKey) {
        this.fullRefNucSeqKey = fullRefNucSeqKey;
    }

    public int getFullRefAASeqKey() {
        return fullRefAASeqKey;
    }

    public void setFullRefAASeqKey(int fullRefAASeqKey) {
        this.fullRefAASeqKey = fullRefAASeqKey;
    }

    public String getTripletError() {
        return tripletError;
    }

    public void setTripletError(String tripletError) {
        this.tripletError = tripletError;
    }

    public String getFrameShift() {
        return frameShift;
    }

    public void setFrameShift(String frameShift) {
        this.frameShift = frameShift;
    }
}
