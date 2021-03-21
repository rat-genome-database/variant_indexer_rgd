package edu.mcw.rgd.variantIndexerRgd.dao;

import edu.mcw.rgd.dao.AbstractDAO;
import edu.mcw.rgd.dao.DataSourceFactory;

import edu.mcw.rgd.dao.spring.ConservationScoreMapper;
import edu.mcw.rgd.dao.spring.variants.*;
import edu.mcw.rgd.datamodel.ConservationScore;
import edu.mcw.rgd.datamodel.variants.VariantMapData;
import edu.mcw.rgd.datamodel.variants.VariantObject;
import edu.mcw.rgd.datamodel.variants.VariantSampleDetail;
import edu.mcw.rgd.datamodel.variants.VariantTranscript;
import edu.mcw.rgd.process.Utils;
import edu.mcw.rgd.variantIndexerRgd.model.VariantData;
import edu.mcw.rgd.variantIndexerRgd.model.VariantIndex;
import edu.mcw.rgd.variantIndexerRgd.model.VariantIndexObject;
import org.springframework.jdbc.core.SqlParameter;


import java.sql.*;
import java.util.*;

/**
 * Created by jthota on 1/16/2020.
 */
public class VariantDao extends AbstractDAO {
    public VariantIndexObject mapSamplesNTranscripts(List<VariantData> records, VariantIndexObject indexObject){
        List<VariantSampleDetail> sampleDetails= new ArrayList<>();
        sampleDetails=indexObject.getSamples();
        if(sampleDetails==null){
            sampleDetails=new ArrayList<>();
        }
        List<VariantTranscript> vts= new ArrayList<>();
        vts=indexObject.getVariantTranscripts();
        if(vts==null || vts.size()==0){
            vts=new ArrayList<>();
        }
        for(VariantData r: records){
                    boolean existsSample = false;
                    for (VariantSampleDetail s : sampleDetails) {
                        if (s.getSampleId() == r.getSampleId()) {
                            existsSample = true;
                        }
                    }
                    if (!existsSample) {
                        sampleDetails.add(this.mapSampleDetails(r));

                    }

                /*********************add VARIANT TRANSCRIPTS*****************/

                    boolean existsTranscript=false;
                    for(VariantTranscript vt:vts){
                        if(vt.getTranscriptRgdId()==r.getTranscriptRgdId()){
                            existsTranscript=true;
                        }
                    }
                    if(!existsTranscript){
                        VariantTranscript vt=this.mapVariantTranscript(r);
                        vts.add(vt);


                    }


        }
        indexObject.setSamples(sampleDetails);
        indexObject.setVariantTranscripts(vts);
        return indexObject;
    }
    public List<Polyphen> getPolyphen(long variantRgdId) throws Exception {
        String sql="select * from polyphen where variant_rgd_id=?";
        PolyphenQuery q= new PolyphenQuery(DataSourceFactory.getInstance().getCarpeNovoDataSource(), sql);
        return execute(q,variantRgdId);
    }
    public List<ConservationScore> getConservationScores(long startPos,String chr, String tableName) throws Exception {
        String sql="select * from "+tableName+" where position=? and chr=?";
        ConservationScoreMapper q= new ConservationScoreMapper(DataSourceFactory.getInstance().getCarpeNovoDataSource(), sql);

        return execute(q, startPos,chr);
    }
    public List<edu.mcw.rgd.datamodel.variants.VariantTranscript> getVariantTranscripts(long rgdId, int mapKey) throws Exception {
        String sql=" select t.*, p.prediction from variant_transcript t left outer join " +
                "                polyphen p on (t.variant_rgd_id=p.variant_rgd_id and t.transcript_rgd_id=p.transcript_rgd_id)\n" +
                "                where t.variant_rgd_id=? " +
                "                and t.map_key=?";
        VariantTranscriptQuery q=new VariantTranscriptQuery(DataSourceFactory.getInstance().getCarpeNovoDataSource(), sql);
        q.declareParameter(new SqlParameter(Types.INTEGER));
        q.declareParameter(new SqlParameter(Types.INTEGER));

        return q.execute(rgdId, mapKey);
    }
    public List<edu.mcw.rgd.datamodel.variants.VariantTranscript> getVariantTranscriptsNpolyphen(long rgdId) throws Exception {
        String sql="select t.*, p.PREDICTION from variant_transcript t, polyphen p where v.variant_rgd_id =p.rgd_id and" +
                "v.transcript_rgd_id=p.transcript_rgd_id ";
        VariantTranscriptQuery q=new VariantTranscriptQuery(DataSourceFactory.getInstance().getCarpeNovoDataSource(), sql);
        q.declareParameter(new SqlParameter(Types.INTEGER));
        return q.execute(rgdId);
    }
   /* public List<VariantSampleDetail> getSamples(long rgdId) throws Exception {
        String sql="select * from variant_sample_detail where rgd_id=?";
        VariantSampleQuery q=new VariantSampleQuery(DataSourceFactory.getInstance().getCarpeNovoDataSource(),sql);
        return execute(q,rgdId);
    }*/
    public List<VariantSampleDetail> getSamples(long rgdId) throws Exception {
        String sql="select * from variant_sample_detail where rgd_id=?";
        VariantSampleQuery q=new VariantSampleQuery(DataSourceFactory.getInstance().getCarpeNovoDataSource(),sql);
        return execute(q,rgdId);
    }
    public List<VariantSampleDetail> getSamples(Set<Long> rgdIds) throws Exception {
        StringBuilder sql= new StringBuilder("select * from variant_sample_detail where rgd_id in (");
        boolean first=true;
        for(long rgdId: rgdIds){
            if(first){
                sql.append(rgdId);
                first=false;
            }else{
                sql.append(",").append(rgdId);
            }
        }
        sql.append(")");
    //    System.out.println("SQL:"+ sql.toString());
        VariantSampleQuery q=new VariantSampleQuery(DataSourceFactory.getInstance().getCarpeNovoDataSource(), sql.toString());
        return q.execute();
    }
    public List<VariantObject> getVariants(String chr, int mapKey, int speciesTypeKey, int sampleId) throws Exception {
        String conScoreTable=getConScoreTable(mapKey,null);

        String sql =" select             " +
                "v.rgd_id, v.var_nuc, v.ref_nuc, v.species_type_key, v.variant_type, v.rs_id, v.CLINVAR_ID, \n" +
                "                 vmd.chromosome, vmd.start_pos, vmd.end_pos, vmd.genic_status, vmd.padding_base, vmd.map_key,\n" +
                "                 vt.TRANSCRIPT_RGD_ID," +
                "                 vt.REF_AA, " +
                "                 vt.VAR_AA, " +
                "                 vt.SYN_STATUS, " +
                "                 vt.LOCATION_NAME, " +
                "                 vt.NEAR_SPLICE_SITE," +
                "                 vt.FULL_REF_NUC_SEQ_KEY," +
                "                 vt.FULL_REF_NUC_POS, " +
                "                 vt.FULL_REF_AA_SEQ_KEY, " +
                "                 vt.FULL_REF_AA_POS, " +
                "                 vt.TRIPLET_ERROR, " +
                "                 vt.FRAMESHIFT ," +
                "                 p.prediction, " +
                "                cs.score, " +
                "   vsd.RGD_ID," +
                "   vsd.SOURCE," +
                "   vsd.SAMPLE_ID," +
                "   vsd.TOTAL_DEPTH," +
                "   vsd.VAR_FREQ," +
                "   vsd.ZYGOSITY_STATUS," +
                "   vsd.ZYGOSITY_PERCENT_READ," +
                "   vsd.ZYGOSITY_POSS_ERROR," +
                "   vsd.ZYGOSITY_REF_ALLELE," +
                "   vsd.ZYGOSITY_NUM_ALLELE," +
                "   vsd.ZYGOSITY_IN_PSEUDO," +
                "   vsd.QUALITY_SCORE" +
                " from variant v " +
                "                              left outer join variant_map_data vmd on (v.rgd_id=vmd.rgd_id)" +
                "                              left outer join variant_transcript vt on (vt.variant_rgd_id=v.rgd_id)" +
                "                              left outer join polyphen p on (v.rgd_id=p.variant_rgd_id and vt.transcript_rgd_id=p.transcript_rgd_id) " +
                "                              left outer join "+ conScoreTable+" cs on (cs.chr=vmd.chromosome and cs.position=vmd.start_pos)" +
                "                               left outer join variant_sample_detail vsd on (v.rgd_id=vsd.rgd_id)" +
                "                              where v.species_type_key=? " +
                "                               and vmd.chromosome=?" +
                "                              and vmd.map_key=? " +
                "                               and vsd.sample_id=?" +
           //     "                              and v.rgd_id=?" +
                "                             ";  //Total RECORD COUNT: 1888283; chr:1; map_key:360
      //  VariantMapQuery q=new VariantMapQuery(DataSourceFactory.getInstance().getDataSource("Variant"), sql);
        VariantQuery q=new VariantQuery(DataSourceFactory.getInstance().getCarpeNovoDataSource(), sql);
        return execute(q,speciesTypeKey,chr,mapKey,sampleId);
    }
    public String getConScoreTable(int mapKey, String genicStatus ) {
        switch(mapKey) {
            case 17:
                return " B37_CONSCORE_PART_IOT ";
            case 38:
                return " CONSERVATION_SCORE_HG38 ";
            case 60:
                if (genicStatus.equalsIgnoreCase("GENIC")) {
                    return " CONSERVATION_SCORE_GENIC ";
                }

                return " CONSERVATION_SCORE ";
            case 70:
                return " CONSERVATION_SCORE_5 ";
            case 360:
                return " CONSERVATION_SCORE_6 ";
            default:
                return " CONSERVATION_SCORE_6 ";
        }
    }
    public List<VariantMapData> getVariants1( String chr, int mapKey, int speciesTypeKey) throws Exception {
        String sql ="select * from variant v, variant_map_data vmd  " +
                "where v.rgd_id=vmd.rgd_id " +
                " and v.species_type_key=? " +
                " and vmd.chromosome=? " +
                " and vmd.map_key=?";  //Total RECORD COUNT: 1888283; chr:1; map_key:360
        //  VariantMapQuery q=new VariantMapQuery(DataSourceFactory.getInstance().getDataSource("Variant"), sql);
        VariantMapQuery q=new VariantMapQuery(DataSourceFactory.getInstance().getCarpeNovoDataSource(), sql);
        return execute(q,speciesTypeKey,chr,mapKey);
    }
    public List<VariantMapData> getUniqueVariants1( String chr, int mapKey, int speciesTypeKey) throws Exception {
        String sql ="select * from variant v, variant_map_data vmd  " +
                "where v.rgd_id=vmd.rgd_id " +
                " and v.species_type_key=? " +
                " and vmd.chromosome=? " +
                " and vmd.map_key=?";  //Total RECORD COUNT: 1888283; chr:1; map_key:360
        //  VariantMapQuery q=new VariantMapQuery(DataSourceFactory.getInstance().getDataSource("Variant"), sql);
        VariantMapQuery q=new VariantMapQuery(DataSourceFactory.getInstance().getCarpeNovoDataSource(), sql);
        return execute(q,speciesTypeKey,chr,mapKey);
    }
    public List<VariantIndex> getVariants(int sampleId, String chr, int mapKey) throws Exception {
        String sql = "select v.*, " +
                "      vsd.RGD_ID," +
                "      vsd.SOURCE," +
                "      vsd.SAMPLE_ID," +
                "      vsd.TOTAL_DEPTH," +
                "      vsd.VAR_FREQ," +
                "      vsd.ZYGOSITY_STATUS," +
                "      vsd.ZYGOSITY_PERCENT_READ," +
                "      vsd.ZYGOSITY_POSS_ERROR," +
                "      vsd.ZYGOSITY_REF_ALLELE," +
                "      vsd.ZYGOSITY_NUM_ALLELE," +
                "      vsd.ZYGOSITY_IN_PSEUDO," +
                "      vsd.QUALITY_SCORE," +
                "      vmd.chromosome, vmd.start_pos, vmd.end_pos, " +
                "      vmd.genic_status, vmd.padding_base, vmd.map_key " +
                " from variant v, variant_sample_detail vsd, " +
                "variant_map_data vmd where " +
                "v.rgd_id=vmd.rgd_id " +
                "and v.rgd_id=vsd.rgd_id " +
                "and vmd.map_key=? " +
                "and vmd.chromosome=? " +
                "and vsd.sample_id=?";

        List<VariantIndex> indexObjects=new ArrayList<>();

        ResultSet rs = null;
        Connection connection = null;
        PreparedStatement stmt = null;
        try {
            connection = DataSourceFactory.getInstance().getCarpeNovoDataSource().getConnection();
            stmt = connection.prepareStatement(sql);
            stmt.setInt(1, mapKey);
            stmt.setString(2, chr);
            stmt.setInt(3, sampleId);
            rs = stmt.executeQuery();
            while (rs.next()) {
                try {
                    VariantIndex vi = new VariantIndex();
                    long variant_id = rs.getLong("rgd_id");
                    vi.setVariant_id(variant_id);
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
                  //  vi.setHGVSNAME(rs.getString("hgvs_name"));
                 //   vi.setAnalysisName(rs.getString("analysis_name"));
                    vi.setMapKey(mapKey);
                    indexObjects.add(vi);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            rs.close();
            stmt.close();
            connection.close();
        }catch (Exception e){
            if (rs != null) {
                rs.close();
            }
            if (stmt != null) {
                stmt.close();
            }
            if (connection != null) {
                connection.close();
            }
        }
        return indexObjects;
    }
  /*  public List<VariantIndex> getVariantResults(int sampleId, String chr, int mapKey) {
        String variantTable=new String();
        String variantTranscriptTable= new String();
        String polyphenTable=new String();
            if(mapKey==17 || mapKey==38){
                variantTable="variant_human";
                variantTranscriptTable="variant_transcript_human";
                polyphenTable="polyphen_human";
            }
        if(mapKey==360 || mapKey==60 || mapKey==70){
            variantTable="variant";
            variantTranscriptTable="variant_transcript";
            polyphenTable="polyphen";
        }
        if(mapKey==600 || mapKey==631){
            variantTable="variant_dog";
            variantTranscriptTable="variant_transcript_dog";
            polyphenTable="polyphen_dog";
        }

      String sql="select v.*, vt.*,t.*, p.*, cs.*,dbs.* , dbs.snp_name as MCW_DBS_SNP_NAME, md.*, gl.gene_symbols as region_name,s.*, g.gene_symbol as symbol, g.gene_symbol_lc as symbol_lc from "+variantTable+" v " +
                "left outer join gene_loci gl on (gl.map_key=? and gl.chromosome=v.chromosome and gl.pos=v.start_pos) " +
                "left outer join "+variantTranscriptTable +" vt on v.variant_id=vt.variant_id " +
                "left outer join transcripts t on vt.transcript_rgd_id=t.transcript_rgd_id " +
                "left outer join "+polyphenTable +" p on (v.variant_id =p.variant_id) "+
                "left outer JOIN sample s on (v.sample_id=s.sample_id and s.map_key=?)" +
                "left outer JOIN  db_snp dbs  ON  " +
                "( v.START_POS = dbs.POSITION     AND v.CHROMOSOME = dbs.CHROMOSOME      AND v.VAR_NUC = dbs.ALLELE      AND dbs.MAP_KEY = s.MAP_KEY AND dbs.source=s.dbsnp_source) " +
                "left outer join CONSERVATION_SCORE cs on (cs.chr=v.chromosome and cs.position=v.start_pos) " +
                "left outer join maps_data md on (md.chromosome=v.chromosome and md.rgd_id=t.gene_rgd_id and md.map_key=?) " +
                "left outer join genes g on (g.rgd_id=t.gene_rgd_id) " +
                "left outer join rgd_ids r on (r.rgd_id=md.rgd_id and r.object_status='ACTIVE') " +
                "where v.chromosome=? " +
             //   "and v.total_depth>8 " +
                "and v.sample_id=?";

        List<VariantIndex> vrList = new ArrayList<>();
        java.util.Map<Long, VariantIndex> variants= new HashMap<>();
        Set<Long> variantIds= new HashSet<>();
        Set<Long> variantTranscripIds=new HashSet<>();
        ResultSet rs= null;
        Connection connection= null;
        PreparedStatement stmt=null;
        try{
            connection= DataSourceFactory.getInstance().getCarpeNovoDataSource().getConnection();
            stmt=connection.prepareStatement(sql);
            stmt.setInt(1, mapKey);
            stmt.setInt(2, mapKey);
            stmt.setInt(3, mapKey);
            stmt.setString(4, chr);
            stmt.setInt(5, sampleId);
            rs=  stmt.executeQuery();

            //   System.out.println("RESULT SET SIZE: "+ rs.getFetchSize());
            while(rs.next()) {
                try {
                    VariantIndex vi = new VariantIndex();
                    long variant_id = rs.getLong("variant_id");
                    if (!variantIds.contains(variant_id)) {
                        variantIds.add(variant_id);
                        vi.setVariant_id(variant_id);
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
                        vi.setHGVSNAME(rs.getString("hgvs_name"));
                       vi.setAnalysisName(rs.getString("analysis_name"));
                        vi.setStrainRgdId(rs.getInt("strain_rgd_id"));
                        vi.setGender(rs.getString("gender"));
                        vi.setPatientId(rs.getInt("patient_id"));
                        vi.setMapKey(mapKey);

                        /***************Variant Transcript****************************/
          /*              List<BasicTranscriptData> vts= new ArrayList<>();
                        variantTranscripIds.add(rs.getLong("variant_transcript_id"));
                        BasicTranscriptData vt=new BasicTranscriptData();

                        vt.setTranscriptRgdId(rs.getInt("transcript_rgd_id"));
                        vt.setRefAA(rs.getString("ref_aa"));
                        vt.setVarAA(rs.getString("var_aa"));
                        vt.setGenespliceStatus(rs.getString("genesplice_status"));
                        vt.setPolyphenStatus(rs.getString("polyphen_status"));
                        vt.setSynStatus(rs.getString("syn_status"));
                        vt.setLocationName(rs.getString("location_name"));
                        vt.setNearSliceSite(rs.getString("near_splice_site"));

                        vt.setTripleError(rs.getString("triplet_error"));
                        vt.setFrameShift(rs.getString("frameshift"));
                        vts.add(vt);
                        vi.setVariantTranscripts(vts);
                        /*****************polyphen******************/

//                        vi.setPolyphenPrediction(rs.getString("prediction"));
                        /**************************dbs_snp****************************/
     /*                   vi.setDbsSnpName(rs.getString("MCW_DBS_SNP_NAME"));
                        /******************region_name*******************/
     /*                   String regionName=rs.getString("region_name");
                        List<String> regionNames=new ArrayList<>();
                        regionNames.add(regionName);
                        vi.setRegionName(regionNames);
                        vi.setRegionNameLc(Arrays.asList(regionName.toLowerCase()));
                        List<BigDecimal> conScores = new ArrayList<>();
                        conScores.add(rs.getBigDecimal("score"));
                        vi.setConScores(conScores);
                        variants.put(variant_id, vi);

                    } else {
                        VariantIndex obj = variants.get(variant_id);
                        Long vtId=rs.getLong("variant_transcript_id");

                        List<BasicTranscriptData>vtranscripts=new ArrayList<>();
                        if(vtId!=0 && !variantTranscripIds.contains(vtId)) {
                            if(obj.getVariantTranscripts()!=null)
                            vtranscripts=obj.getVariantTranscripts();
                            variantTranscripIds.add(vtId);
                            variantTranscripIds.add(rs.getLong("variant_transcript_id"));
                            BasicTranscriptData vt=new BasicTranscriptData();

                            vt.setTranscriptRgdId(rs.getInt("transcript_rgd_id"));
                            vt.setRefAA(rs.getString("ref_aa"));
                            vt.setVarAA(rs.getString("var_aa"));
                            vt.setGenespliceStatus(rs.getString("genesplice_status"));
                            vt.setPolyphenStatus(rs.getString("polyphen_status"));
                            vt.setSynStatus(rs.getString("syn_status"));
                            vt.setLocationName(rs.getString("location_name"));
                            vt.setNearSliceSite(rs.getString("near_splice_site"));
                            vt.setTripleError(rs.getString("triplet_error"));
                            vt.setFrameShift(rs.getString("frameshift"));

                            vtranscripts.add(vt);
                            vi.setVariantTranscripts(vtranscripts);

                        }
                        variants.put(variant_id, obj);
                    }
                }catch (Exception e){

                    e.printStackTrace();
                }
            }

            rs.close();
            stmt.close();
            connection.close();

        } catch (Exception e) {
            e.printStackTrace();
            try {
                if(rs!=null)
                    rs.close();
                if(stmt!=null)
                    stmt.close();
                if(connection!=null)
                    connection.close();
            } catch (SQLException ex) {
                ex.printStackTrace();
            }

        }finally {
            try {
                if(rs!=null)
                    rs.close();
                if(stmt!=null)
                    stmt.close();
                if(connection!=null)
                    connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }

        }
        for(Map.Entry e:variants.entrySet()){
            vrList.add((VariantIndex) e.getValue());
        }
        System.out.println("varaiants size: "+ vrList.size());
        return vrList;
    }*/
                        public VariantMapData mapVariant(VariantData r){
                            VariantMapData v=new VariantMapData();
                            v.setId(r.getVariantRgdId());
                            v.setVariantNucleotide(r.getVarNuc());
                            v.setReferenceNucleotide(r.getRefNuc());
                            v.setVariantType(r.getVariantTpe());
                            v.setRsId(r.getRsId());
                            v.setChromosome(r.getChromosome());
                            v.setStartPos(r.getStartPos());
                            v.setEndPos(r.getEndPos());
                            v.setGenicStatus(r.getGenicStatus());
                            v.setPaddingBase(r.getPaddingBase());
                            v.setMapKey(r.getMapKey());
                            return v;
                        }
    public VariantSampleDetail mapSampleDetails(VariantData v){
        VariantSampleDetail s= new VariantSampleDetail();
        s.setSampleId(v.getSampleId());
        s.setSource(v.getSource());
        s.setVariantFrequency(v.getVarFreq());
        s.setQualityScore(v.getQualityScore());
        s.setZygosityNumberAllele(v.getZygosityNumAllele());
        s.setZygosityPossibleError(v.getZygosityPossError());
        s.setDepth(v.getTotatlDepth());
        s.setZygosityInPseudo(v.getZygosityInPseudo());
        s.setZygosityPercentRead(v.getZygosityPercentRead());
        s.setZygosityRefAllele(v.getZygosityRefAllele());
        s.setZygosityStatus(v.getZygosityStatus());
        return s;
    }
    public VariantTranscript mapVariantTranscript(VariantData v){
        VariantTranscript vt=new VariantTranscript();
        vt.setNearSpliceSite(v.getNearSpliceSite());
        vt.setTripletError(v.getTripletError());
        vt.setFrameShift(v.getFrameShift());
        vt.setTranscriptRgdId(v.getTranscriptRgdId());
        vt.setFullRefAASeqKey(v.getFullRefAASeqKey());
        vt.setFullRefNucSeqKey(v.getFullRefNucSeqKey());
        vt.setFullRefNucPos(v.getFullRefNucPos());
        vt.setFullRefAAPos(v.getFulRefAAPos());
        vt.setLocationName(v.getLocationName());
        vt.setSynStatus(v.getSynStatus());
        vt.setPolyphenStatus(v.getPolyphenPrediction());
        return vt;
    }
}
