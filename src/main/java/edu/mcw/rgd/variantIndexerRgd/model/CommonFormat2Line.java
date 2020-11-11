package edu.mcw.rgd.variantIndexerRgd.model;

import java.util.List;

/**
 * Created by jthota on 11/15/2019.
 */
public class CommonFormat2Line {
    private String chr;
    private int pos;
    private String refNuc;
    private String varNuc;

    private Integer countA;
    private Integer countC;
    private Integer countG;
    private Integer countT;
    private Integer totalDepth;
    private String hgvsName;
    private Integer rgdId;
    private Integer alleleDepth;
    private Integer alleleCount;
    private Integer readDepth;
    private String paddingBase;
    private String strains;
    private String rsId;
    private List<String> strainList;

    public List<String> getStrainList() {
        return strainList;
    }

    public void setStrainList(List<String> strainList) {
        this.strainList = strainList;
    }

    public String getStrains() {
        return strains;
    }

    public void setStrains(String strains) {
        this.strains = strains;
    }

    // return true if adjustement was successful
    public boolean adjustForIndels() {
        // snv
        if( refNuc.length()==1 && varNuc.length()==1 ) {
            return true;
        }
        // insertion
        if( refNuc.length()==1 && varNuc.length()>1 ) {
            // varNuc first base must be the same as refNuc
            if( refNuc.charAt(0)!=varNuc.charAt(0) ) {
                //      System.out.println("Unexpected: insertion: missing padding base");
                return false;
            }
            // handle padding base
            setPaddingBase(refNuc);
            setRefNuc(null);
            setVarNuc(varNuc.substring(1));
            pos++;
            return true;
        }
        // deletion
        if( refNuc.length()>1 && varNuc.length()==1 ) {
            // varNuc first base must be the same as refNuc
            if( refNuc.charAt(0)!=varNuc.charAt(0) ) {
                //         System.out.println("Unexpected: deletion: missing padding base");
                return false;
            }
            // handle padding base
            setPaddingBase(varNuc);
            setVarNuc(null);
            setRefNuc(refNuc.substring(1));
            pos++;
            return true;
        }
        // unhandled case
        //   System.out.println("TODO");
        return false;
    }

    public boolean old_adjustForIndels() {
        while( refNuc.length()>1 && varNuc.length()>0 ) {
            if( varNuc.length()>refNuc.length() ) {
                // strip same nucleotides from the end
                //if( refNuc.charAt(refNuc.length()-1)==varNuc.charAt(varNuc.length()-1) ) {
                //    refNuc = refNuc.substring(0, refNuc.length()-1);
                //    varNuc = varNuc.substring(0, varNuc.length()-1);
                //}
                //else
                if( refNuc.charAt(0)==varNuc.charAt(0) ) {
                    // strip first nucleotide from refNuc and varNuc
                    refNuc = refNuc.substring(1);
                    varNuc = varNuc.substring(1);
                    pos++;
                } else {
                    System.out.println("unhandled insertion");
                    return false;
                }
            } else {
                // deletion
                while( !varNuc.isEmpty() ) {
                    if( refNuc.charAt(0)==varNuc.charAt(0) ) {
                        // strip first nucleotide from refNuc and varNuc
                        refNuc = refNuc.substring(1);
                        varNuc = varNuc.substring(1);
                        pos++;
                        //} else if( refNuc.charAt(refNuc.length()-1)==varNuc.charAt(varNuc.length()-1) ) {
                        //    // strip last nucleotide from refNuc and varNuc
                        //    refNuc = refNuc.substring(0, refNuc.length()-1);
                        //    varNuc = varNuc.substring(0, varNuc.length()-1);
                    } else {
                        // irregular deletion, f.e. AAG -> T
                        while( refNuc.length()>varNuc.length() ) {
                            varNuc += "-";
                        }
                        return true;
                    }
                }
            }
        }

        // deletion final
        while( refNuc.length()>varNuc.length() ) {
            varNuc += "-";
        }
        return true;
    }

    public String getChr() {
        return chr;
    }

    public void setChr(String chr) {
        this.chr = chr;
    }

    public int getPos() {
        return pos;
    }

    public void setPos(int pos) {
        this.pos = pos;
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

    public Integer getCountA() {
        return countA;
    }

    public void setCountA(Integer countA) {
        this.countA = countA;
    }

    public Integer getCountC() {
        return countC;
    }

    public void setCountC(Integer countC) {
        this.countC = countC;
    }

    public Integer getCountG() {
        return countG;
    }

    public void setCountG(Integer countG) {
        this.countG = countG;
    }

    public Integer getCountT() {
        return countT;
    }

    public void setCountT(Integer countT) {
        this.countT = countT;
    }

    public Integer getTotalDepth() {
        return totalDepth;
    }

    public void setTotalDepth(Integer totalDepth) {
        this.totalDepth = totalDepth;
    }

    public String getHgvsName() {
        return hgvsName;
    }

    public void setHgvsName(String hgvsName) {
        this.hgvsName = hgvsName;
    }

    public Integer getRgdId() {
        return rgdId;
    }

    public void setRgdId(Integer rgdId) {
        this.rgdId = rgdId;
    }

    public Integer getAlleleDepth() {
        return alleleDepth;
    }

    public void setAlleleDepth(Integer alleleDepth) {
        this.alleleDepth = alleleDepth;
    }

    public Integer getAlleleCount() {
        return alleleCount;
    }

    public void setAlleleCount(Integer alleleCount) {
        this.alleleCount = alleleCount;
    }

    public Integer getReadDepth() {
        return readDepth;
    }

    public void setReadDepth(Integer readDepth) {
        this.readDepth = readDepth;
    }

    public String getPaddingBase() {
        return paddingBase;
    }

    public void setPaddingBase(String paddingBase) {
        this.paddingBase = paddingBase;
    }
}
