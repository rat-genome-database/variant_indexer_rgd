package edu.mcw.rgd.variantIndexerRgd.model;

import java.util.ArrayList;

/**
 * Created by jthota on 11/15/2019.
 */
public class TranscriptFlags {
    public String strand = null;
    public Feature threeUtr = null;
    public Feature fiveUtr = null;
    public ArrayList<Feature> exomsArray = new ArrayList<Feature>();
    // process all transcripts 3UTS and 5UTR are top of the list of results but don't count on getting
    // them every time in results
    public String nearSpliceSite = "F";
    public String transcriptLocation = null;
    public boolean inExon = false;
}
