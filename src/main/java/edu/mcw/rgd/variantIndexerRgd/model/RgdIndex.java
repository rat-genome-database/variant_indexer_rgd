package edu.mcw.rgd.variantIndexerRgd.model;

import java.util.List;

/**
 * Created by jthota on 11/15/2019.
 */
public class RgdIndex {
    private static String index;
    private static String oldAlias;
    private static String newAlias;
    private static List<String> indices;


    public static String getOldAlias() {
        return oldAlias;
    }

    public static void setOldAlias(String oldAlias) {
        RgdIndex.oldAlias = oldAlias;
    }

    public static String getNewAlias() {
        return newAlias;
    }

    public static void setNewAlias(String newAlias) {
        RgdIndex.newAlias = newAlias;
    }


    public void setIndex(String index) {
        RgdIndex.index = index;
    }

    public String getIndex() {
        return index;
    }

    public  List<String> getIndices() {
        return indices;
    }

    public void setIndices(List<String> indices) {
        RgdIndex.indices = indices;
    }
}
