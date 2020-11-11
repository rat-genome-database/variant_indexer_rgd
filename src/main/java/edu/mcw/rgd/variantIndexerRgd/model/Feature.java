package edu.mcw.rgd.variantIndexerRgd.model;

/**
 * Created by jthota on 11/15/2019.
 */
public class Feature {
    public int start;
    public int stop;
    public String location; // 3UTRS, 5UTRS, EXONS

    public Feature(int start, int stop, String location) {
        this.start = start;
        this.stop = stop;
        this.location = location;
    }
}
