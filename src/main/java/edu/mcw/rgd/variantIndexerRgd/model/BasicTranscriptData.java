package edu.mcw.rgd.variantIndexerRgd.model;

/**
 * Created by jthota on 12/20/2019.
 */
public class BasicTranscriptData {
    private int transcriptRgdId;
    private String genespliceStatus;
    private String nearSliceSite;
    private String tripleError;
    private String frameShift;
    private String synStatus;
    private String polyphenStatus;
    private String locationName;
    private String refAA;
    private String varAA;

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

    public String getLocationName() {
        return locationName;
    }

    public void setLocationName(String locationName) {
        this.locationName = locationName;
    }

    public int getTranscriptRgdId() {
        return transcriptRgdId;
    }

    public void setTranscriptRgdId(int transcriptRgdId) {
        this.transcriptRgdId = transcriptRgdId;
    }

    public String getGenespliceStatus() {
        return genespliceStatus;
    }

    public void setGenespliceStatus(String genespliceStatus) {
        this.genespliceStatus = genespliceStatus;
    }

    public String getNearSliceSite() {
        return nearSliceSite;
    }

    public void setNearSliceSite(String nearSliceSite) {
        this.nearSliceSite = nearSliceSite;
    }

    public String getTripleError() {
        return tripleError;
    }

    public void setTripleError(String tripleError) {
        this.tripleError = tripleError;
    }

    public String getFrameShift() {
        return frameShift;
    }

    public void setFrameShift(String frameShift) {
        this.frameShift = frameShift;
    }

    public String getSynStatus() {
        return synStatus;
    }

    public void setSynStatus(String synStatus) {
        this.synStatus = synStatus;
    }

    public String getPolyphenStatus() {
        return polyphenStatus;
    }

    public void setPolyphenStatus(String polyphenStatus) {
        this.polyphenStatus = polyphenStatus;
    }


}
