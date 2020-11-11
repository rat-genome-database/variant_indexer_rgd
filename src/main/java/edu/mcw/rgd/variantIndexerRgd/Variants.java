package edu.mcw.rgd.variantIndexerRgd;

import edu.mcw.rgd.dao.AbstractDAO;
import edu.mcw.rgd.dao.DataSourceFactory;
import edu.mcw.rgd.variantIndexerRgd.dao.VariantDataQuery;
import edu.mcw.rgd.variantIndexerRgd.model.VariantData;
import org.springframework.jdbc.core.SqlParameter;

import java.util.List;

public class Variants extends AbstractDAO {
    public List<VariantData> getVariants(int speciesTypeKey, String chromosome, int mapKey, int sampleId) throws Exception {
 //   public List<VariantData> getVariants(int speciesTypeKey, String chromosome,int mapKey, int variantRgdId) throws Exception {
        String sql="select " +
                " v.rgd_id, v.var_nuc, v.ref_nuc, v.species_type_key, v.variant_type, v.rs_id, v.CLINVAR_ID, " +
                " vmd.chromosome, vmd.start_pos, vmd.end_pos, vmd.genic_status, vmd.padding_base, vmd.map_key, " +
                " vsd.sample_id, vsd.TOTAL_DEPTH, " +
                " vsd.SOURCE, " +
                " vsd.VAR_FREQ, " +
                " vsd.ZYGOSITY_STATUS, " +
                " vsd.ZYGOSITY_PERCENT_READ, " +
                " vsd.ZYGOSITY_POSS_ERROR, " +
                " vsd.ZYGOSITY_REF_ALLELE, " +
                " vsd.ZYGOSITY_NUM_ALLELE, " +
                " vsd.ZYGOSITY_IN_PSEUDO, " +
                " vsd.QUALITY_SCORE, " +
                " vt.TRANSCRIPT_RGD_ID," +
                " vt.REF_AA," +
                " vt.VAR_AA," +
                " vt.SYN_STATUS, " +
                " vt.LOCATION_NAME, " +
                " vt.NEAR_SPLICE_SITE," +
                " vt.FULL_REF_NUC_SEQ_KEY, " +
                " vt.FULL_REF_NUC_POS, " +
                " vt.FULL_REF_AA_SEQ_KEY, " +
                " vt.FULL_REF_AA_POS, " +
                " vt.TRIPLET_ERROR, " +
                " vt.FRAMESHIFT ," +
                " p.prediction, " +
                "cs.score " +
                " from variant v " +
                "              left outer join variant_map_data vmd on (v.rgd_id=vmd.rgd_id) " +
                "              left outer join variant_sample_detail vsd on (v.rgd_id=vsd.rgd_id) " +
                "              left outer join variant_transcript vt on (vt.variant_rgd_id=v.rgd_id) " +
                "              left outer join polyphen p on (v.rgd_id=p.variant_rgd_id and vt.transcript_rgd_id=p.transcript_rgd_id) " +
                "              left outer join CONSERVATION_SCORE cs on (cs.chr=vmd.chromosome and cs.position=vmd.start_pos)"+
                "              where v.species_type_key=? " +
                "              and vmd.map_key=? " +
                "              and vmd.chromosome=?"+
           //     " and v.rgd_id=? " +
             " and vsd.sample_id=?"
                ;

       // VariantDataQuery query=new VariantDataQuery(DataSourceFactory.getInstance().getDataSource("Variant"), sql);
        VariantDataQuery query=new VariantDataQuery(DataSourceFactory.getInstance().getCarpeNovoDataSource(), sql);

            return execute(query, speciesTypeKey, mapKey, chromosome, sampleId);


        }

}
