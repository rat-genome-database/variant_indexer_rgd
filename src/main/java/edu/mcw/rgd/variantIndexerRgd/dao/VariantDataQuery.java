package edu.mcw.rgd.variantIndexerRgd.dao;

import edu.mcw.rgd.variantIndexerRgd.model.VariantData;
import org.springframework.jdbc.object.MappingSqlQuery;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;

public class VariantDataQuery extends MappingSqlQuery {
    public VariantDataQuery(DataSource ds, String query){
        super(ds, query);
    }
    @Override
    protected VariantData mapRow(ResultSet rs, int rowNum) throws SQLException {
        VariantData v=new VariantData();
        v.setVariantRgdId(rs.getInt("rgd_id"));
        v.setVarNuc(rs.getString("var_nuc"));
        v.setRefNuc(rs.getString("ref_nuc"));
        v.setRsId(rs.getString("rs_id"));
        v.setClinvarId(rs.getString("clinvar_id"));
        v.setVariantTpe(rs.getString("variant_type"));
        v.setChromosome(rs.getString("chromosome"));
        v.setStartPos(rs.getInt("start_pos"));
        v.setEndPos(rs.getInt("end_pos"));
        v.setGenicStatus(rs.getString("genic_status"));
        v.setPaddingBase(rs.getString("padding_base"));
        v.setMapKey(rs.getInt("map_key"));
        v.setSampleId(rs.getInt("sample_id"));
        v.setSource(rs.getString("source"));
        v.setTotatlDepth(rs.getInt("total_depth"));
        v.setQualityScore(rs.getInt("quality_score"));
        v.setVarFreq(rs.getInt("var_freq"));
        v.setZygosityInPseudo(rs.getString("zygosity_in_pseudo"));
        v.setZygosityNumAllele(rs.getInt("zygosity_num_allele"));
        v.setZygosityPercentRead(rs.getInt("zygosity_percent_read"));
        v.setZygosityPossError(rs.getString("zygosity_poss_error"));
        v.setZygosityRefAllele(rs.getString("zygosity_ref_allele"));
        v.setZygosityStatus(rs.getString("zygosity_status"));

        v.setTranscriptRgdId(rs.getInt("TRANSCRIPT_RGD_ID"));
        v.setRefAA(rs.getString("REF_AA"));

        v.setVarAA(rs.getString("VAR_AA"));
        v.setSynStatus(rs.getString("SYN_STATUS"));
        v.setLocationName(rs.getString("LOCATION_NAME"));
        v.setNearSpliceSite(rs.getString("NEAR_SPLICE_SITE"));
        v.setFullRefNucSeqKey(rs.getInt("FULL_REF_NUC_SEQ_KEY"));
        v.setFullRefNucPos(rs.getInt("FULL_REF_NUC_POS"));
        v.setFullRefAASeqKey(rs.getInt("FULL_REF_AA_SEQ_KEY"));

        v.setFulRefAAPos(rs.getInt("FULL_REF_AA_POS"));
        v.setTripletError(rs.getString("TRIPLET_ERROR"))    ;
        v.setFrameShift(rs.getString("FRAMESHIFT"));
        v.setPolyphenPrediction(rs.getString("prediction"));
        v.setConservationScore(rs.getBigDecimal("score"));
        return v;
    }
}
