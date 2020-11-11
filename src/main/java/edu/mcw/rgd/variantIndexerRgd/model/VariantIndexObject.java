package edu.mcw.rgd.variantIndexerRgd.model;

import edu.mcw.rgd.datamodel.prediction.PolyPhenPrediction;
import edu.mcw.rgd.datamodel.variants.VariantMapData;
import edu.mcw.rgd.datamodel.variants.VariantSampleDetail;
import edu.mcw.rgd.datamodel.variants.VariantTranscript;

import java.util.List;

public class VariantIndexObject {
    private VariantMapData variant;
    private List<VariantSampleDetail> samples;
    private List<VariantTranscript> variantTranscripts;
    private List<PolyPhenPrediction> polyPhenPredictions;
    private List<String> regionName;
    private List<String> regionNameLc;

    public List<String> getRegionNameLc() {
        return regionNameLc;
    }

    public void setRegionNameLc(List<String> regionNameLc) {
        this.regionNameLc = regionNameLc;
    }

    public List<String> getRegionName() {
        return regionName;
    }

    public void setRegionName(List<String> regionName) {
        this.regionName = regionName;
    }

    public List<PolyPhenPrediction> getPolyPhenPredictions() {
        return polyPhenPredictions;
    }

    public void setPolyPhenPredictions(List<PolyPhenPrediction> polyPhenPredictions) {
        this.polyPhenPredictions = polyPhenPredictions;
    }

    public VariantMapData getVariant() {
        return variant;
    }

    public void setVariant(VariantMapData variant) {
        this.variant = variant;
    }

    public List<VariantSampleDetail> getSamples() {
        return samples;
    }

    public void setSamples(List<VariantSampleDetail> samples) {
        this.samples = samples;
    }

    public List<VariantTranscript> getVariantTranscripts() {
        return variantTranscripts;
    }

    public void setVariantTranscripts(List<VariantTranscript> variantTranscripts) {
        this.variantTranscripts = variantTranscripts;
    }
}
