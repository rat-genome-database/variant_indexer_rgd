package edu.mcw.rgd.variantIndexerRgd.dao;

import edu.mcw.rgd.dao.impl.VariantInfoDAO;
import edu.mcw.rgd.datamodel.VariantInfo;
import edu.mcw.rgd.datamodel.variants.VariantTranscript;
import edu.mcw.rgd.variantIndexerRgd.model.VariantIndex;
import org.springframework.jdbc.object.MappingSqlQuery;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class VariantIndexQuery extends MappingSqlQuery {
   public VariantIndexQuery(DataSource ds, String query){
        super(ds, query);
    }
    @Override
    protected VariantIndex mapRow(ResultSet rs, int rowNum) throws SQLException {

            VariantIndex vi = new VariantIndex();
            vi.setCategory("Variant");
            vi.setVariant_id(rs.getLong("rgd_id"));
            vi.setChromosome(rs.getString("chromosome"));
            vi.setPaddingBase(rs.getString("padding_base"));
            vi.setEndPos(rs.getLong("end_pos"));
            vi.setRefNuc(rs.getString("ref_nuc"));
            vi.setSampleId(rs.getInt("sample_id"));
            vi.setStartPos(rs.getLong("start_pos"));
            vi.setTotalDepth(rs.getInt("total_depth"));
            vi.setVarFreq(rs.getInt("var_freq"));
            vi.setVariantType(rs.getString("variant_type"));
            vi.setVarNuc(rs.getString("var_nuc"));
            vi.setZygosityStatus(rs.getString("zygosity_status"));
            vi.setGenicStatus(rs.getString("genic_status"));
            vi.setZygosityPercentRead(rs.getDouble("zygosity_percent_read"));
            vi.setZygosityPossError(rs.getString("zygosity_poss_error"));
            vi.setZygosityRefAllele(rs.getString("zygosity_ref_allele"));
            vi.setZygosityNumAllele(rs.getInt("zygosity_num_allele"));
            vi.setZygosityInPseudo(rs.getString("zygosity_in_pseudo"));
            vi.setQualityScore(rs.getInt("quality_score"));
            //   vi.setHGVSNAME(rs.getString("hgvs_name"));
            //  vi.setAnalysisName(rs.getString("analysis_name"));

            vi.setMapKey(rs.getInt("map_key"));

            /***************Variant Transcript****************************/
            try{
            if(rs.getInt("transcript_rgd_d")!=0) {
                    List<VariantTranscript> vts = new ArrayList<>();
                    VariantTranscript vt = new VariantTranscript();

                    vt.setTranscriptRgdId(rs.getInt("transcript_rgd_id"));
                    vt.setRefAA(rs.getString("ref_aa"));
                    vt.setVarAA(rs.getString("var_aa"));
                    vt.setPolyphenStatus(rs.getString("prediction"));
                    vt.setSynStatus(rs.getString("syn_status"));
                    vt.setLocationName(rs.getString("location_name"));
                    vt.setNearSpliceSite(rs.getString("near_splice_site"));

                    vt.setTripletError(rs.getString("triplet_error"));
                    vt.setFrameShift(rs.getString("frameshift"));
                    vts.add(vt);
                    vi.setVariantTranscripts(vts);
            }}catch (Exception e){}
            /******************region_name*******************/
            try {
                    String regionName = rs.getString("region_name");
                    List<String> regionNames = new ArrayList<>();
                    if (regionName != null) {
                            regionNames.add(regionName);
                            vi.setRegionName(regionNames);

                            vi.setRegionNameLc(Arrays.asList(regionName.toLowerCase()));
                    }
                    ;
            }catch (Exception e){}
            try {
                    if(rs.getInt("gene_rgd_id")>0) {
                            vi.setGeneRgdId(rs.getInt("gene_rgd_id"));
                            vi.setStrand(rs.getString("strand"));
                            vi.setGeneSymbol(rs.getString("gene_symbol_lc"));
                    }
            }catch (Exception e){}
            List<String> conScores = new ArrayList<>();
            try {
                    conScores.add(rs.getString("score"));
                    vi.setConScores(conScores);
            }catch (Exception e){}

        return vi;
    }

}
