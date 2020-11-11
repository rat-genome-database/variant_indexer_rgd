package edu.mcw.rgd.variantIndexerRgd.dao;

import edu.mcw.rgd.dao.AbstractDAO;
import edu.mcw.rgd.dao.spring.*;
import edu.mcw.rgd.datamodel.Transcript;
import edu.mcw.rgd.process.FastaParser;
import edu.mcw.rgd.variantIndexerRgd.model.Feature;
import edu.mcw.rgd.variantIndexerRgd.model.TranscriptFeatures;
import edu.mcw.rgd.variantIndexerRgd.model.TranscriptFlags;
import edu.mcw.rgd.variantIndexerRgd.model.VariantTranscript;
import org.springframework.jdbc.core.SqlParameter;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.*;

/**
 * Created by jthota on 11/15/2019.
 */
public class VariantTranscriptDao extends AbstractDAO{


    public Map<Integer, String> getTranscriptsResultSet(int geneRgdId, int mapKey) throws Exception {
        String  sql = "SELECT transcript_rgd_id,is_non_coding_ind FROM transcripts WHERE gene_rgd_id=? "+
                "AND EXISTS(SELECT 1 FROM maps_data md WHERE md.rgd_id=transcript_rgd_id AND md.map_key=?)";
        Map<Integer, String> transcriptResult=new HashMap<>();
        try(Connection conn= this.getDataSource().getConnection();
                PreparedStatement stmt=conn.prepareStatement(sql)){
            stmt.setInt(1,geneRgdId);
            stmt.setInt(2, mapKey);
            ResultSet rs= stmt.executeQuery();
            while (rs.next()){
                transcriptResult.put(rs.getInt("transcript_rgd_id"), rs.getString("is_non_coding_ind"));
            }
            rs.close();
            stmt.close();
            conn.close();
        }
    return transcriptResult;
    }
    public List<Transcript> getTranscriptsResult(int geneRgdId, int mapKey) throws Exception {
        String  sql = "SELECT * FROM transcripts WHERE gene_rgd_id=? "+
                " AND EXISTS(SELECT 1 FROM maps_data md WHERE md.rgd_id=transcript_rgd_id AND md.map_key=?)";
        TranscriptQuery query=new TranscriptQuery(this.getDataSource(), sql);
        return execute(query, geneRgdId, mapKey);

    }
    public  int getExonCount(int transcriptRgdId, String chr, int mapKey) throws Exception {

        String  sql = "SELECT count(*) "+
                "FROM TRANSCRIPT_FEATURES tf, rgd_ids r, maps_data md "+
                "WHERE "+
                " tf.FEATURE_RGD_ID = r.rgd_id "+
                " AND tf.FEATURE_RGD_ID = md.RGD_ID "+
                " AND tf.TRANSCRIPT_RGD_ID=?"+
                " AND md.MAP_KEY=?"+
                " AND md.CHROMOSOME=?"+
                " AND r.OBJECT_KEY=15"; // "EXONS"

        return getCount(sql,new Object[]{transcriptRgdId, mapKey, chr});
     /*  try(Connection conn=this.getDataSource().getConnection();
           PreparedStatement psExonCount=conn.prepareStatement(sql)) {
           psExonCount.setInt(1, transcriptRgdId);
           psExonCount.setInt(2, mapKey);
           psExonCount.setString(3, chr);
           ResultSet rs = psExonCount.executeQuery();
           rs.next();
           int exonCount = rs.getInt(1);
           rs.close();
           psExonCount.close();
           conn.close();
           return exonCount;*/

    }
    public  List<TranscriptFeatures> getTranscriptFeaturesResultSet(int transcriptRgdId, String chr, int mapKey) throws Exception {
        String  sql = "SELECT" +
                "    ro.OBJECT_NAME, " +
                "    md.STRAND, " +
                "    md.CHROMOSOME, " +
                "    md.START_POS, " +
                "    md.STOP_POS  " +
                "FROM " +
                "    TRANSCRIPT_FEATURES tf , " +
                "    rgd_ids r, " +
                "    maps_data md, " +
                "    rgd_objects ro " +
                "WHERE " +
                "    tf.FEATURE_RGD_ID = r.rgd_id " +
                "AND tf.FEATURE_RGD_ID = md.RGD_ID " +
                "AND tf.TRANSCRIPT_RGD_ID = ? " +
                "AND md.MAP_KEY = ? " +
                "AND md.CHROMOSOME = ? " +
                "AND r.OBJECT_KEY = ro.OBJECT_KEY order by OBJECT_NAME, START_POS, STOP_POS";
        List<TranscriptFeatures> features=new ArrayList<>();


    try(Connection conn= this.getDataSource().getConnection();
        PreparedStatement psTranscriptFeatures =conn.prepareStatement(sql)){
        psTranscriptFeatures.setInt(1, transcriptRgdId);
        psTranscriptFeatures.setInt(2, mapKey);
        psTranscriptFeatures.setString(3, chr);
        ResultSet rs=psTranscriptFeatures.executeQuery();

        while(rs.next()){
            TranscriptFeatures tf= new TranscriptFeatures();
            tf.setStrand(rs.getString("STRAND"));
            tf.setStartPos(rs.getInt("START_POS"));
            tf.setStopPos(rs.getInt("STOP_POS"));
            tf.setObjectName(rs.getString("OBJECT_NAME"));
            features.add(tf);
        }

            rs.close();
            psTranscriptFeatures.close();
            conn.close();


    }

     // System.out.print("\t"+features.size()+"\t");
        return  features;

    }
    public  void processFeatures(int transcriptRgdId, String chr, int mapKey, TranscriptFlags tflags, int varStart, int varStop, int totalExonCount) throws Exception {

        // Get all Transcript features for this transcript. These are Exoms, 3primeUTRs and 5PrimeUTRs
       List<TranscriptFeatures> features = getTranscriptFeaturesResultSet(transcriptRgdId, chr, mapKey);

       for(TranscriptFeatures f:features) {

            // Assume all rows have the same strand
            tflags.strand = f.getStrand();
            int transStart = f.getStartPos();
            int transStop = f.getStopPos();
            String objectName = f.getObjectName();


            if (objectName.equals("3UTRS") ) {
                tflags.threeUtr = new Feature(transStart, transStop, objectName);
            }
            if (objectName.equals("5UTRS") ) {
                tflags.fiveUtr = new Feature(transStart, transStop, objectName);
            }
            if (objectName.equals("EXONS") ) {
                tflags.exomsArray.add(new Feature(transStart, transStop, objectName));

                // 1. Determine splice site is wihtin 10 BP of start / stop of any EXON
                // 2. Check the start position unless it is the start of the first EXON
                if (tflags.exomsArray.size() != 1) {
                    // If the transcript start falls within 10 bp of the variant
                    if ((transStart - 10 <= varStart) && (transStart + 10 >= varStop)) {
                        //     getLogWriter().write("nearSpliceSite found for : transcriptRGDId : " + transcriptRgdId + " , variantStart: " + varStart + ",  transStart: ${transStart} , threeUtr.start: ${threeUtr?.start} threeUtr.stop: ${threeUtr?.stop} fiveUtr.start : ${fiveUtr?.start}  fiveUtr.stop : ${fiveUtr?.stop}\n");
                        tflags.nearSpliceSite = "T";
                    }
                }

                // 3. Check the stop position unless it is the stop position of  last exon
                if (tflags.exomsArray.size() != totalExonCount) {
                    // If the transcript stop falls within 10 bp of the variant stop
                    if ((transStop - 10 <= varStart) && (transStop + 10 >= varStop)) {
                        //      getLogWriter().write("nearSpliceSite found for : transcriptRGDId : " + transcriptRgdId + " ,variantStart: " + varStart + " ,  transStart: " + transStart + " , threeUtr.start: ${threeUtr?.start} threeUtr.stop: ${threeUtr?.stop} fiveUtr.start : ${fiveUtr?.start}  fiveUtr.stop : ${fiveUtr?.stop}\n");
                        tflags.nearSpliceSite = "T";
                    }
                }
            }

            // See if our variant falls into this particular feature , we grad the first feature as the 3Prime and 5prime come first
            // and skip the EXONS as they would also match which we don't want
            //

            // Determine up the transcipt Location  , we want one of these strings:
            // "3UTR,EXON" or "3UTR,INTRON" or "EXON" or "INTRON" or "5UTR,EXON" or "5UTR,INTRON"

            if( transStart <= varStart && transStop >= varStop ) {
                //  getLogWriter().write("	Object found " + objectName + "\n");

                if ((objectName.equals("5UTRS")) || (objectName.equals("3UTRS"))) {
                    if (tflags.transcriptLocation != null) {
                        tflags.transcriptLocation += "," + objectName;
                    } else {
                        tflags.transcriptLocation = objectName;
                    }
                }
                // Add only one EXON using inExon to not do this again
                if (objectName.equals("EXONS") && (!tflags.inExon)) {
                    if (tflags.transcriptLocation != null) {
                        tflags.transcriptLocation += ",EXON";
                    } else {
                        tflags.transcriptLocation = "EXON";
                    }
                    tflags.inExon = true;
                }

            }
        }


    }

    public VariantTranscript processTranscript(TranscriptFlags tflags, int transcriptRgdId, FastaParser fastaFile,
                                      int varStart, int varStop, long variantId, String refNuc, String varNuc) throws Exception {

   //   System.out.print(transcriptRgdId+"\t"+varStart+"\t"+varStop);
        if (tflags.strand != null && tflags.strand.equals("-") ) {
            //      getLogWriter().write("Switching UTrs as we're dealing with - strand ... \n");
            Feature temp = tflags.threeUtr;
            tflags.threeUtr = tflags.fiveUtr;
            tflags.fiveUtr = temp;
        }

        // OK we' have a variant in an Exom  for only plus stranded genes !!!!!!
        handleUTRs(tflags.exomsArray, tflags.threeUtr, tflags.fiveUtr);

        //  getLogWriter().write("		        Variant : " + varStart + " " + varStop + "\n");
        //    getLogWriter().write(" 		Processed exons :\n");
        int fcount = 1;
        int variantRelPos = 0; // relative position of the variant in the entire combined exome sequence
        boolean foundInExon = false;
        // Determine the relative position the variant occurs at
        for (Feature feature: tflags.exomsArray) {
            //     getLogWriter().write(" 		EXON start :" + feature.start + " stop " + feature.stop + "\n");
            // See if feature was skipped entirely by removal from 5PrimteUTR
            if (feature.start != -1) {
                if (feature.start <= varStart && feature.stop > varStop) {
                    //            getLogWriter().write(" 		DNA :" + getDnaChunk(fastaFile, feature.start, feature.stop) + "\n");
                    foundInExon = true;
                    //            getLogWriter().write("Variant found in feature # " + fcount + "\n");
                    variantRelPos += (varStart - (feature.start - 1)); // add length of partial feature
                    //           getLogWriter().write("Relative variant position found as " + variantRelPos + "\n");
                    break;
                } else {
                    variantRelPos += (feature.stop - feature.start) + 1;  // add length of entire feature
                }
            }
            fcount++;
        }
        //    getLogWriter().write("			variantRelPos = " + variantRelPos + "\n");
        if (foundInExon) {
         //     System.out.println("************************* Variant in Exome region ************************\n");
            StringBuffer refDna = new StringBuffer();
            StringBuffer varDna = new StringBuffer();

            // Build up DNA Sequence from features
            for (Feature feature: tflags.exomsArray) {
                // Skip those exons that have been removed. These have had their start / stop marked as -1
                if (feature.start != -1) {

                    String dnaChunk = getDnaChunk(fastaFile, feature.start, feature.stop);
              //    System.out.println(("Building dna adding : (" + feature.start + ", " + feature.stop + ") " + dnaChunk + " length : " + dnaChunk.length() + "\n"));
                    refDna.append(dnaChunk);
                    varDna.append(dnaChunk);
                }
            }
            varDna = new StringBuffer(varDna.toString().toLowerCase());

            // handle deletion
            if( varNuc!=null && varNuc.contains("-") ) {
                int deletionLength = varNuc.length();
                varDna.replace(variantRelPos-1, variantRelPos-1+deletionLength, "");
            }
            // handle insertion
            else if( refNuc!=null &&refNuc.contains("-") ) {
                varDna.insert(variantRelPos-1, varNuc);
            }
            // handle insertion
            else if( refNuc.length()==1 && varNuc.length()>1 ) {
                varDna.insert(variantRelPos, varNuc.substring(1));
            }
            else if( refNuc.length()!=1 || varNuc.length()!=1 ) {
                int deletionLength = varStop-varStart;
                varDna.replace(variantRelPos-1, variantRelPos-1+deletionLength, varNuc);
            }
            else {
                if(!varDna.toString().equals(""))
                varDna.setCharAt(variantRelPos-1, varNuc.charAt(0));
            }

            refDna = new StringBuffer(refDna.toString().toLowerCase());

            //   getLogWriter().write(" RefDna length =  : " + refDna.length() + " mod " + (refDna.length() % 3) + "\n");
            //   getLogWriter().write(" ENTIRE reference DNA :\n" + refDna.toString() + "\n");


            if (tflags.strand.equals("-")) {
                //  getLogWriter().write("		Negative Strand found reverseCompliment dna \n");
                //  getLogWriter().write("		variantRelPos before neg stand switch is : " + variantRelPos + "\n");
                variantRelPos = refDna.length() - variantRelPos + 1;
                // getLogWriter().write("		variantRelPos set now set to : " + variantRelPos + "\n");
                // Dealing with "-" strand , reverse the DNA
                refDna = reverseComplement(refDna);
                varDna = reverseComplement(varDna);
            } else {
                // getLogWriter().write("		Positive Strand found " + "\n");
            }

            // Check for rna evenly divisable by 3 or log as error
            String transcriptErrorFound = "F";
            if (refDna.length() % 3 != 0) {

                transcriptErrorFound = "T";
            }
            // make it divisible by 3
            if (refDna.length() % 3 != 0) {
                refDna.replace(0, refDna.length(), refDna.substring(0, refDna.length() - (refDna.length() % 3)));

            }
            if (varDna.length() % 3 != 0) {
                varDna.replace(0, varDna.length(), varDna.substring(0, varDna.length() - (varDna.length() % 3)));

            }

            // Now test to see if the variant was in an area eliminated by the divisable by 3 truncation process
            if ( variantRelPos < 1 ) {
                //        writeError(variantId+":"+transcriptRgdId+":"+refDna.length()+":"+new Date().toString()+":SKIPPED\n", sampleId);
                //       getLogWriter().write("************************* Error in transcript variant in trimmed area : skipping see error_rga.txt file  ************************\n");
            //    return false; // return false to insert new row into VARIANT_TRANSCRIPT: at least variant location will be available
                return null;

            }


            //     getLogWriter().write("ENTIRE Ref DNA :\n" + refDna.toString() + "\n");

            // replace variant , ok as the variant is always on the plus strand
            //    getLogWriter().write("variant rel pos = " + variantRelPos + "\n");

            //     getLogWriter().write(" ENTIRE variant DNA ( change in upper case ):\n" + varDna.toString() + "\n");


            return handleTranslatedProtein(refDna, varDna, variantRelPos, variantId,
                    transcriptRgdId, tflags.transcriptLocation, tflags.nearSpliceSite, transcriptErrorFound);
        } else {
            //variant lies within an exon but the part of the exon where it lies is not protein coding
            // so the variant lies within an exon that is part of a UTR
            //      getLogWriter().write("************************* Variant in Non-protein coding exon region ************************\n");
          return  insertVariantTranscript(variantId, transcriptRgdId, tflags.transcriptLocation, tflags.nearSpliceSite);
         //   return true; // true denotes successful insert into VARIANT_TRANSCRIPT
        }

    }

  public VariantTranscript handleTranslatedProtein(StringBuffer refDna, StringBuffer varDna, int variantRelPos, long variantId,
                                    int transcriptRgdId, String transcriptLocation, String nearSpliceSite, String transcriptErrorFound) throws Exception {

        String rnaRefTranslated = translate(refDna);
        String rnaVarTranslated = translate(varDna);

        //    getLogWriter().write("RNA REF  \n" + rnaRefTranslated + "\n");
        //    getLogWriter().write("RNA VAR  \n" + rnaVarTranslated + "\n");
        //    getLogWriter().write("variantRelPos = " + variantRelPos + "\n");

        // Determine AA Symbol
        int pos = 1 + (variantRelPos - 1) / 3;
        //     getLogWriter().write("Position were looking for: " + pos + " rnaRefTranslate.length : " + rnaRefTranslated.length() + "\n");

        // Check if the variant still falls in the transcript
        if (pos>0 && pos <= rnaRefTranslated.length() && pos <= rnaVarTranslated.length()) {
            String LRef = rnaRefTranslated.substring(pos-1, pos);
            String LVar = rnaVarTranslated.substring(pos-1, pos);

            //    getLogWriter().write("Calculated  Ref AA = " + LRef + " Var AA = " + LVar + "\n");
            String synStatus = LRef.equals(LVar) ? "synonymous" : "nonsynonymous";
            if( LRef.equals("X") || LVar.equals("X") ) {
                synStatus = "unassignable";
            }

            // compute frameshift
            // compute length difference between reference and variant nucleotides
            int lenDiffRefVar = Math.abs(refDna.length() - varDna.length());
            int reminder = lenDiffRefVar%3;
            String isFrameShift = reminder!=0 ? "T" : "F";

          return   insertVariantTranscript(variantId, transcriptRgdId, LRef, LVar,
                    synStatus, transcriptLocation, nearSpliceSite, pos, variantRelPos, transcriptErrorFound,
                    rnaRefTranslated, refDna.toString(), isFrameShift);

         //   return true; // true denotes successful insert into VARIANT_TRANSCRIPT
        } else {
            //   getLogWriter().write("Variant skipped because it is no longer in the truncated transcript.");
          //  return false; // return false to insert new row into VARIANT_TRANSCRIPT: at least variant location will be available
        }
      return null;
    }

    void handleUTRs(ArrayList<Feature> exomsArray, Feature threeUtr, Feature fiveUtr) throws Exception {
        for (Feature feature: exomsArray) {

            // if we have a 3'utr to deal with  at end
            if (threeUtr != null) {
                //  println  "comparing 3prime: " <<  threeUtr.start << "," << threeUtr.stop << " feature: " <<  feature.start << ", " << feature.stop
                if (feature.stop < threeUtr.start) {
                    // use entire feature
                } else if (feature.start < threeUtr.start) {
                    //     getLogWriter().write("feature start reset to " + (threeUtr.start - 1) + "\n");
                    feature.stop = threeUtr.start - 1;
                } else {
                    // remove all of exome
                    //    getLogWriter().write("3Prime removed  feature \n");
                    feature.start = -1;
                    feature.stop = -1;
                }
            }
            // if we have a 5' utr to deal with at start
            if (fiveUtr != null) {
                // println  "comparing 5prime: " <<  fiveUtr.start << "," << fiveUtr.stop << " feature: " <<  feature.start << ", " << feature.stop
                if (feature.start > fiveUtr.stop) {
                    // use entire feature
                } else if (feature.stop > fiveUtr.stop) {
                    //   getLogWriter().write("feature stop reset to " + (fiveUtr.stop + 1) + "\n");
                    feature.start = fiveUtr.stop + 1;
                } else {
                    // remove all of exome
                    //  getLogWriter().write("5Prime removed feature" + "\n");
                    feature.start = -1;
                    feature.stop = -1;
                }
            }
        }
    }

    static public StringBuffer reverseComplement(CharSequence dna) throws Exception {

        StringBuffer buf = new StringBuffer(dna.length());
        for( int i=dna.length()-1; i>=0; i-- ) {
            char ch = dna.charAt(i);
            if( ch=='A' || ch=='a' ) {
                buf.append('T');
            } else if( ch=='C' || ch=='c' ) {
                buf.append('G');
            } else if( ch=='G' || ch=='g' ) {
                buf.append('C');
            } else if( ch=='T' || ch=='t' ) {
                buf.append('A');
            } else if( ch=='N' || ch=='n' ) {
                buf.append('N');
            }
            else throw new Exception("reverseComplement: unexpected nucleotide ["+ch+"]");
        }
        return buf;
    }

    static
    String translate(StringBuffer dna) {

        StringBuilder out = new StringBuilder(dna.length() / 3);
        for( int i=0; i<dna.length(); i+=3 ) {
            char c1 = Character.toUpperCase(dna.charAt(i + 0));
            char c2 = Character.toUpperCase(dna.charAt(i + 1));
            char c3 = Character.toUpperCase(dna.charAt(i + 2));

            if( c1=='C') { // QUARTER C
                if( c2=='A' ) {
                    if( c3=='T' || c3=='C' ) {
                        out.append("H"); // histodine
                    }
                    else if( c3=='A' || c3=='G' ) {
                        out.append("Q"); // glutamine
                    }
                    else
                        out.append("X"); // unknown
                }
                else if( c2=='C' ) {
                    out.append("P"); // proline
                }
                else if( c2=='G' ) {
                    out.append("R"); // arginine
                }
                else if( c2=='T' ) {
                    out.append("L"); // leucine
                }
                else
                    out.append("X"); // unknown
            }

            else if( c1=='G' ) { // QUARTER G
                if( c2=='A' ) {
                    if( c3=='T' || c3=='C' ) {
                        out.append("D"); // aspartic acid
                    }
                    else if( c3=='A' || c3=='G' ) {
                        out.append("E"); // glutamic acid
                    }
                    else
                        out.append("X"); // unknown
                }
                else if( c2=='C' ) {
                    out.append("A"); // alanine
                }
                else if( c2=='G' ) {
                    out.append("G"); // glycine
                }
                else if( c2=='T' ) {
                    out.append("V"); // valine
                }
                else
                    out.append("X"); // unknown
            }

            else if( c1=='A' ) { // QUARTER A
                if( c2=='A' ) {
                    if( c3=='T' || c3=='C' ) {
                        out.append("N"); // asparagine
                    }
                    else if( c3=='A' || c3=='G' ) {
                        out.append("K"); // lysine
                    }
                    else
                        out.append("X"); // unknown
                }
                else if( c2=='C' ) {
                    out.append("T"); // threonine
                }
                else if( c2=='G' ) {
                    if( c3=='T' || c3=='C' ) {
                        out.append("S"); // serine
                    }
                    else if( c3=='A' || c3=='G' ) {
                        out.append("R"); // arginine
                    }
                    else
                        out.append("X"); // unknown
                }
                else if( c2=='T' ) {
                    if( c3=='T' || c3=='C' || c3=='A' ) {
                        out.append("I"); // isoleucine
                    }
                    else if( c3=='G' ) {
                        out.append("M"); // methionine
                    }
                    else
                        out.append("X"); // unknown
                }
                else
                    out.append("X"); // unknown
            }

            else if( c1=='T' ) { // QUARTER T
                if( c2=='A' ) {
                    if( c3=='T' || c3=='C' ) {
                        out.append("Y"); // tyrosine
                    }
                    else if( c3=='A' || c3=='G' ) {
                        out.append("*"); // STOP
                    }
                    else
                        out.append("X"); // unknown
                }
                else if( c2=='C' ) {
                    out.append("S"); // serine
                }
                else if( c2=='G' ) {
                    if( c3=='T' || c3=='C' ) {
                        out.append("C"); // cysteine
                    }
                    else if( c3=='A' ) {
                        out.append("*"); // STOP
                    }
                    else if( c3=='G' ) {
                        out.append("W"); // tryptophan
                    }
                    else
                        out.append("X"); // unknown
                }
                else if( c2=='T' ) {
                    if( c3=='T' || c3=='C' ) {
                        out.append("F"); // phenylalanine
                    }
                    else if( c3=='A' || c3=='G' ) {
                        out.append("L"); // leucine
                    }
                    else
                        out.append("X"); // unknown
                }
                else
                    out.append("X"); // unknown
            }

            else { // QUARTER N
                out.append("X"); // unknown
            }
        }

        return out.toString();
    }

    // **************
    // ***DAO*** code

    public VariantTranscript insertVariantTranscript(long variantId, int transcriptRgdId, String transcriptLocation, String nearSpliceSite) throws Exception {
      return   insertVariantTranscript(variantId, transcriptRgdId, null, null, null, transcriptLocation, nearSpliceSite, null,
                null, null, null, null, null);
        //  getLogWriter().write("		Found variant at Location  " + transcriptLocation + " found for " + variantId
        //          + ", " + transcriptRgdId + " \n");
    }

  public  VariantTranscript insertVariantTranscript(long variantId, int transcriptRgdId, String refAA, String varAA, String synStatus,
                                 String transcriptLocation, String nearSpliceSite, Integer fullRefAaPos, Integer fullRefNucPos,
                                 String tripletError, String fullRefAA, String fullRefNuc, String frameShift) throws Exception {

        VariantTranscript vt = new VariantTranscript();
        vt.setVariantId(variantId);
        vt.setTranscriptRgdId(transcriptRgdId);
        vt.setRefAA(refAA);
        vt.setVarAA(varAA);
        vt.setSynStatus(synStatus);
        vt.setLocationName(transcriptLocation);
        vt.setNearSpliceSite(nearSpliceSite);
        vt.setFullRefAAPos(fullRefAaPos);
        vt.setFullRefNucPos(fullRefNucPos);
        vt.setTripletError(tripletError);
        vt.setFullRefAA(fullRefAA);
        vt.setFullRefNuc(fullRefNuc);
        vt.setFrameShift(frameShift);

        return vt;
        //   batch.addToBatch(vt);
    }
    public String getDnaChunk(FastaParser parser, int start, int stop) throws Exception {
        String key = start+"+"+stop;
        String dna = dnaCache.get(key);
        if( dna==null ) {
            dna = getDnaChunkFromFastaFile(parser, start, stop);
            if(!Objects.equals(dna, ""))
            dnaCache.put(key, dna);
        }
        return dna;
    }


    public String getDnaChunkFromFastaFile(FastaParser parser, int start, int stop) throws Exception {
        String fasta = parser.getSequence(start, stop);
        if(fasta!=null)
        return fasta.replaceAll("\\s+", "");
        else return "";
    }

    Map<String,String> dnaCache = new HashMap<>();

    public void initGene(int geneRgdId) {
        // the idea is to keep in the cache positions for gene exons
        // the more transcripts a gene has, the bigger benefits of this cache
        dnaCache.clear();
    }

  /*  public static void main(String[] args) throws Exception {
        VariantTranscriptDao dao= new VariantTranscriptDao();
        dao.getConservationScores("1", 404395 );
        System.out.println("DONE!!");
    }
*/

}
