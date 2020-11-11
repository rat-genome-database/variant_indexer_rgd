package edu.mcw.rgd.variantIndexerRgd.model;

/**
 * Created by jthota on 11/15/2019.
 */
public class VariantTranscript {
    private long id;
    private long variantId;
    private int startPos;
    private int stopPos;
    private String chromosome;
    private String refNuc;
    private String varNuc;
    private int transcriptRgdId;
    private String refAA;
    private String varAA;
    private String genespliceStatus;
    private String polyphenStatus;
    private String synStatus;
    private String locationName;
    private String nearSpliceSite;
    private String fullRefNuc;
    private Integer fullRefNucPos;
    private String fullRefAA;
    private Integer fullRefAAPos;
    private String uniprotId;
    private String proteinId;
    private String tripletError;
    private String frameShift;
    private String rsId;

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public long getVariantId() {
        return variantId;
    }

    public void setVariantId(long variantId) {
        this.variantId = variantId;
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

    public String getVarAA() {
        return varAA;
    }

    public void setVarAA(String varAA) {
        this.varAA = varAA;
    }

    public String getGenespliceStatus() {
        return genespliceStatus;
    }

    public void setGenespliceStatus(String genespliceStatus) {
        this.genespliceStatus = genespliceStatus;
    }

    public String getPolyphenStatus() {
        return polyphenStatus;
    }

    public void setPolyphenStatus(String polyphenStatus) {
        this.polyphenStatus = polyphenStatus;
    }

    public String getSynStatus() {
        return synStatus;
    }

    public void setSynStatus(String synStatus) {
        this.synStatus = synStatus;
    }

    public String getLocationName() {
        return locationName;
    }

    public void setLocationName(String locationName) {
        this.locationName = locationName;
    }

    public String getNearSpliceSite() {
        return nearSpliceSite;
    }

    public void setNearSpliceSite(String nearSpliceSite) {
        this.nearSpliceSite = nearSpliceSite;
    }

    public String getFullRefNuc() {
        return fullRefNuc;
    }

    public void setFullRefNuc(String fullRefNuc) {
        this.fullRefNuc = fullRefNuc;
    }

    public Integer getFullRefNucPos() {
        return fullRefNucPos;
    }

    public void setFullRefNucPos(Integer fullRefNucPos) {
        this.fullRefNucPos = fullRefNucPos;
    }

    public String getFullRefAA() {
        return fullRefAA;
    }

    public void setFullRefAA(String fullRefAA) {
        this.fullRefAA = fullRefAA;
    }

    public Integer getFullRefAAPos() {
        return fullRefAAPos;
    }

    public void setFullRefAAPos(Integer fullRefAAPos) {
        this.fullRefAAPos = fullRefAAPos;
    }

    public String getUniprotId() {
        return uniprotId;
    }

    public void setUniprotId(String uniprotId) {
        this.uniprotId = uniprotId;
    }

    public String getProteinId() {
        return proteinId;
    }

    public void setProteinId(String proteinId) {
        this.proteinId = proteinId;
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

    public int getStartPos() {
        return startPos;
    }

    public void setStartPos(int startPos) {
        this.startPos = startPos;
    }

    public int getStopPos() {
        return stopPos;
    }

    public void setStopPos(int stopPos) {
        this.stopPos = stopPos;
    }

    public String getChromosome() {
        return chromosome;
    }

    public void setChromosome(String chromosome) {
        this.chromosome = chromosome;
    }

    public String getRefNuc() {
        return refNuc;
    }

    public void setRefNuc(String refNuc) {
        this.refNuc = refNuc;
    }

    public String getVarNuc() {
        return varNuc;
    }

    public void setVarNuc(String varNuc) {
        this.varNuc = varNuc;
    }

    public String getRsId() {
        return rsId;
    }

    public void setRsId(String rsId) {
        this.rsId = rsId;
    }
}
