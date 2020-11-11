package edu.mcw.rgd.variantIndexerRgd.polyphen;

import edu.mcw.rgd.dao.AbstractDAO;
import edu.mcw.rgd.dao.impl.SequenceDAO;
import edu.mcw.rgd.datamodel.Sequence;
import edu.mcw.rgd.variantIndexerRgd.dao.VariantLoad3;
import edu.mcw.rgd.variantIndexerRgd.model.VariantTranscript;
import org.springframework.beans.factory.xml.XmlBeanFactory;
import org.springframework.core.io.FileSystemResource;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by jthota on 2/25/2020.
 */
public class Polyphen extends AbstractDAO{
    private String version;

    private String WORKING_DIR = "data/polyphen/output";
    private BufferedWriter errorFile;
    private BufferedWriter polyphenFile;
    private BufferedWriter polyphenFileInfo;
    private BufferedWriter fastaFile;

    SequenceDAO sequenceDAO = new SequenceDAO();
    boolean simpleProteinQC = false;
 //   boolean createFastaFile = false;
    boolean createFastaFile = true;

    public Polyphen() throws Exception {}
    public static void main(String[] args) throws Exception {
        Polyphen p= new Polyphen();
        p.run("21", "human", 17);

        System.out.println("DONE!!");
    }
    public void run(String chr, String species, int mapKey) throws Exception {

        String varTable = "VARIANT";
        if(!species.equals("rat")){
            varTable=varTable+"_"+species;
        }
        simpleProteinQC = !varTable.equals("VARIANT");

       String polyphenFileName = WORKING_DIR + "/chr" +chr+".PolyPhenInput";

        polyphenFile = new BufferedWriter(new FileWriter(polyphenFileName));
        polyphenFileInfo = new BufferedWriter(new FileWriter(polyphenFileName+".info"));
        polyphenFileInfo.append("#Note: if STRAND is '-', then inverted NUC_VAR is AA_REF\n");
        polyphenFileInfo.append("LOCUS_NAME\tPROTEIN_ACC_ID\tRELATIVE_VAR_POS\tREF_AA\tVAR_AA\tSTRAND\tTRANSCRIPT_RGD_ID\n");


        if( createFastaFile ) {
            // create the fasta file
            String fastaFileName = WORKING_DIR + "/chr" +  chr + ".PolyPhenInput.fasta";
            fastaFile = new BufferedWriter(new FileWriter(fastaFileName));
        }

        runChromosome(chr,  mapKey);

        polyphenFileInfo.close();
        polyphenFile.close();

        if( fastaFile!=null ) {
            fastaFile.close();
        }

        System.out.println("finishing "+polyphenFileName+"\n\n\n");
    }

    public void runChromosome(String chr, int mapKey) throws Exception {
        int variantsProcessed = 0;
        int refSeqProteinLengthErrors = 0;
        int refSeqProteinLeftPartMismatch = 0;
        int refSeqProteinRightPartMismatch = 0;
        int proteinRefSeqNotInRgd = 0;
        int stopCodonsInProtein = 0;

        // /*+ INDEX(vt) */
        // this query hint forces oracle to use indexes on VARIANT_TRANSCRIPT table
        // (many times it was using full scans for unknown reasons)
        VariantLoad3 loader= new VariantLoad3();
        List<VariantTranscript> transcripts=loader.getVariantTranscriptsByChromosome(chr);
        String line;
        System.out.println("TRANSCRIPTS SIZE: "+ transcripts.size());
        for(VariantTranscript t:transcripts) {
        try {
            int startPos = t.getStartPos();


            String refAA = t.getRefAA();
            String varAA = t.getVarAA();
            String fullRefAA = t.getFullRefAA();
            Integer fullRefAaaPos = t.getFullRefAAPos();
            int transcriptRgdId = t.getTranscriptRgdId();
            Map<String, String> geneProteinMap = this.getGeneProteinMap(transcriptRgdId);
            String regionName = geneProteinMap.get("gene");
            String proteinAccId = geneProteinMap.get("protein");
            String strand = getStrand(transcriptRgdId, chr, startPos, mapKey);


            if (simpleProteinQC) {

                // simple protein QC:
                // there must not be any stop codons in the middle of the protein
                // only at the end, if any
                int stopCodonFirstPos = fullRefAA.indexOf('*');
                if (stopCodonFirstPos < fullRefAA.length() - 1) {
                    // stop codons in the middle of the protein

                    // exception: if stop codons are 10 AAs after variant pos, that's OK
                    if (stopCodonFirstPos <= fullRefAaaPos + 10) {
                        line = "stop codons in the middle of the protein sequence!\n" +
                                "    transcript_rgd_id = " + transcriptRgdId + "\n" +
                                "    protein_acc_id = " + proteinAccId + "\n" +
                                "    ref_aa_pos = " + fullRefAaaPos + "\n" +
                                "    RefSeq protein length = " + fullRefAA.length() + "\n";

                        stopCodonsInProtein++;
                  //      System.out.println("***STOP CODONS IN PROTEIN***\n" + line);
                        continue;
                    }
                }

                variantsProcessed++;

                String translatedLeftPart = fullRefAA.substring(0, fullRefAaaPos - 1);

                // RefSeq protein part to the right of the mutation point must match the translated part
                String translatedRightPart = fullRefAA.substring(fullRefAaaPos);
                if (translatedRightPart.endsWith("*"))
                    translatedRightPart = translatedRightPart.substring(0, translatedRightPart.length() - 1);

                System.out.println("***MATCH***\n" +
                        "  proteinLeftPart\n" + translatedLeftPart + "\n" +
                        "  proteinRightPart\n" + translatedRightPart + "\n");

                // write polyphen input file
                // PROTEIN_ACC_ID POS REF_AA VAR_AA
                line = proteinAccId + " " + fullRefAaaPos + " " + refAA + " " + varAA + "\n";
                polyphenFile.write(line);

                // write polyphen input info file
                //#VARIANT_ID\tVARIANT_TRANSCRIPT_ID\tLOCUS_NAME\tPROTEIN_ACC_ID\tRELATIVE_VAR_POS\tREF_AA\tVAR_AA\tSTRAND\tTRANSCRIPT_RGD_ID");
                line = regionName + "\t" + proteinAccId + "\t" + fullRefAaaPos + "\t" + refAA + "\t" + varAA + "\t" + strand + "\t" + transcriptRgdId + "\n";
                polyphenFileInfo.write(line);

                writeFastaFile(proteinAccId, fullRefAA);
                continue;
            }

            // retrieve protein sequence from RGD
            List<Sequence> seqsInRgd = getProteinSequences(transcriptRgdId);
            if (seqsInRgd == null || seqsInRgd.isEmpty()) {
                proteinRefSeqNotInRgd++;
                System.out.println("***PROTEIN REFSEQ NOT IN RGD***\n");
            } else {
                for (Sequence seq : seqsInRgd) {
                    variantsProcessed++;

                    // RefSeq protein part to the left of the mutation point must match the translated part
                    String refSeqLeftPart;
                    String translatedLeftPart = fullRefAA.substring(0, fullRefAaaPos - 1);
                    try {
                        refSeqLeftPart = seq.getSeqData().substring(0, fullRefAaaPos - 1);
                    } catch (IndexOutOfBoundsException e) {
                        line = "RefSeq protein shorter than REF_AA_POS!\n" +
                                "    transcript_rgd_id = " + transcriptRgdId + "\n" +
                                "    protein_acc_id = " + proteinAccId + "\n" +
                                "    ref_aa_pos = " + fullRefAaaPos + "\n" +
                                "    RefSeq protein length = " + seq.getSeqData().length() + "\n";
                        errorFile.append(line);
                        refSeqProteinLengthErrors++;
                        System.out.println("***LEFT FLANK LENGTH ERROR***\n" + line);
                        continue;
                    }

                    if (!refSeqLeftPart.equalsIgnoreCase(translatedLeftPart)) {
                        line = "Left flank not the same!\n" +
                                "    transcript_rgd_id = " + transcriptRgdId + "\n" +
                                "    protein_acc_id = " + proteinAccId + "\n" +
                                "    RefSeq left part         " + refSeqLeftPart + "\n" +
                                "    translated ref left part " + translatedLeftPart + "\n";
                        errorFile.append(line);
                        refSeqProteinLeftPartMismatch++;
                        System.out.println("***LEFT FLANK ERROR***\n" + line);
                        continue;
                    }


                    // RefSeq protein part to the right of the mutation point must match the translated part
                    String refSeqRightPart;
                    String translatedRightPart = fullRefAA.substring(fullRefAaaPos);
                    if (translatedRightPart.endsWith("*"))
                        translatedRightPart = translatedRightPart.substring(0, translatedRightPart.length() - 1);
                    try {
                        refSeqRightPart = seq.getSeqData().substring(fullRefAaaPos);
                    } catch (IndexOutOfBoundsException e) {
                        line = "RefSeq protein shorter than REF_AA_POS!\n" +
                                "    transcript_rgd_id = " + transcriptRgdId + "\n" +
                                "    protein_acc_id = " + proteinAccId + "\n" +
                                "    ref_aa_pos = " + fullRefAaaPos + "\n" +
                                "    RefSeq protein length = " + seq.getSeqData().length() + "\n";
                        errorFile.append(line);
                        refSeqProteinLengthErrors++;
                        System.out.println("***RIGHT FLANK LENGTH ERROR***\n" + line);
                        continue;
                    }

                    if (!refSeqRightPart.equalsIgnoreCase(translatedRightPart)) {
                        line = "Right flank not the same!\n" +
                                "    transcript_rgd_id = " + transcriptRgdId + "\n" +
                                "    protein_acc_id = " + proteinAccId + "\n" +
                                "    RefSeq left part         " + refSeqRightPart + "\n" +
                                "    translated ref right part " + translatedRightPart + "\n";
                        errorFile.append(line);
                        refSeqProteinRightPartMismatch++;
                        System.out.println("***RIGHT FLANK ERROR***\n" + line);
                        continue;
                    }

                    System.out.println("***MATCH***\n" +
                            "  proteinLeftPart\n" + refSeqLeftPart + "\n" +
                            "  proteinRightPart\n" + refSeqRightPart + "\n");

                    // write polyphen input file
                    // PROTEIN_ACC_ID POS REF_AA VAR_AA
                    line = proteinAccId + " " + fullRefAaaPos + " " + refAA + " " + varAA + "\n";
                    polyphenFile.write(line);

                    // write polyphen input info file
                    //#VARIANT_ID\tVARIANT_TRANSCRIPT_ID\tLOCUS_NAME\tPROTEIN_ACC_ID\tRELATIVE_VAR_POS\tREF_AA\tVAR_AA\tSTRAND\tTRANSCRIPT_RGD_ID");
                    line = regionName + "\t" + proteinAccId + "\t" + fullRefAaaPos + "\t" + refAA + "\t" + varAA + "\t" + strand + "\t" + transcriptRgdId + "\n";
                    polyphenFileInfo.write(line);

                    writeFastaFile(proteinAccId, fullRefAA);
                }
            }
        }catch (Exception e){
            System.out.println("**********"+ t.getTranscriptRgdId()+"\t"+t.getFullRefAA()+"\t"+t.getStartPos());
            e.printStackTrace();
        }
        }



        System.out.println("\nPROCESSING SUMMARY:");
        System.out.println("\n  variantsProcessed = "+variantsProcessed);
        System.out.println("\n  refSeqProteinLengthErrors = "+refSeqProteinLengthErrors);
        System.out.println("\n  refSeqProteinLeftPartMismatch = "+refSeqProteinLeftPartMismatch);
        System.out.println("\n  refSeqProteinRightPartMismatch = "+refSeqProteinRightPartMismatch);
        System.out.println("\n  proteinRefSeqNotInRgd = "+proteinRefSeqNotInRgd);
        System.out.println("\n  stopCodonsInProtein = "+stopCodonsInProtein);

    }

    public Map<String, String> getGeneProteinMap(int transcriptRgdId) throws Exception {
        String sql="SELECT GENE_RGD_ID, PROTEIN_ACC_ID FROM TRANSCRIPTS WHERE TRANSCRIPT_RGD_ID=?";
        Connection conn= this.getDataSource().getConnection();
        PreparedStatement stmt= conn.prepareStatement(sql);
        stmt.setInt(1, transcriptRgdId);
        ResultSet rs= stmt.executeQuery();
        Map<String, String> geneProteinMap= new HashMap<>();
        while (rs.next()){
            geneProteinMap.put("gene", rs.getString("gene_rgd_id"));
            geneProteinMap.put("protein", rs.getString("protein_acc_id"));
        }

        rs.close();
        stmt.close();
        conn.close();
        return geneProteinMap;
    }
    void writeFastaFile(String proteinAccId, String fullRefAA) throws IOException {
        if( fastaFile!=null ) {
            fastaFile.write(">"+proteinAccId);
            fastaFile.newLine();

            // write protein sequence, up to 70 characters per line
            for( int i=0; i<fullRefAA.length(); i+=70 ) {
                int chunkEndPos = i+70;
                if( chunkEndPos > fullRefAA.length() )
                    chunkEndPos = fullRefAA.length();
                String chunk = fullRefAA.substring(i, chunkEndPos);
                fastaFile.write(chunk);
                fastaFile.newLine();
            }
        }
    }

    List<Sequence> getProteinSequences(int transcriptRgdId) throws Exception {
        return sequenceDAO.getObjectSequences(transcriptRgdId, "ncbi_protein");
    }

    String getStrand(int rgdId, String chr, int pos, int mapKey) throws Exception {

        String strands = "";

        String sql = "SELECT DISTINCT strand FROM maps_data md "+
                "WHERE md.rgd_id=? AND map_key=? AND chromosome=? AND start_pos<=? AND stop_pos>=?";

        Connection conn = this.getDataSource().getConnection();
        PreparedStatement ps = conn.prepareStatement(sql);
        ps.setInt(1, rgdId);
        ps.setInt(2, mapKey);
        ps.setString(3, chr);
        ps.setInt(4, pos);
        ps.setInt(5, pos);
        ResultSet rs = ps.executeQuery();
        while( rs.next() ) {
            String strand = rs.getString(1);
            if( strand != null )
                strands += strand;
        }

        conn.close();
        return strands;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public String getVersion() {
        return version;
    }
}
