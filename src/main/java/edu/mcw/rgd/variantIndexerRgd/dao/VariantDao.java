package edu.mcw.rgd.variantIndexerRgd.dao;

import edu.mcw.rgd.dao.AbstractDAO;
import edu.mcw.rgd.dao.DataSourceFactory;

import edu.mcw.rgd.dao.impl.VariantInfoDAO;
import edu.mcw.rgd.dao.spring.ConservationScoreMapper;
import edu.mcw.rgd.dao.spring.IntListQuery;
import edu.mcw.rgd.dao.spring.SolrDocQuery;
import edu.mcw.rgd.dao.spring.variants.*;
import edu.mcw.rgd.datamodel.ConservationScore;
import edu.mcw.rgd.datamodel.Variant;
import edu.mcw.rgd.datamodel.VariantInfo;
import edu.mcw.rgd.datamodel.variants.VariantMapData;
import edu.mcw.rgd.datamodel.variants.VariantObject;
import edu.mcw.rgd.datamodel.variants.VariantSampleDetail;
import edu.mcw.rgd.datamodel.variants.VariantTranscript;
import edu.mcw.rgd.process.Utils;
import edu.mcw.rgd.services.IndexDocument;
import edu.mcw.rgd.variantIndexerRgd.model.BasicTranscriptData;
import edu.mcw.rgd.variantIndexerRgd.model.VariantData;
import edu.mcw.rgd.variantIndexerRgd.model.VariantIndex;
import edu.mcw.rgd.variantIndexerRgd.model.VariantIndexObject;
import org.springframework.jdbc.core.SqlParameter;


import java.math.BigDecimal;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by jthota on 1/16/2020.
 */
public class VariantDao extends AbstractDAO {
    VariantInfoDAO variantInfoDAO=new VariantInfoDAO();
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






    public String getConScoreTable(int mapKey, String genicStatus ) {
        switch(mapKey) {
            case 17:
                return " B37_CONSCORE_PART_IOT ";
            case 38:
                return " CONSERVATION_SCORE_HG38 ";
            case 60:
            /*    if (genicStatus.equalsIgnoreCase("GENIC")) {
                    return " CONSERVATION_SCORE_GENIC ";
                }
*/
                return " CONSERVATION_SCORE ";
            case 70:
                return " CONSERVATION_SCORE_5 ";
            case 360:
                return " CONSERVATION_SCORE_6 ";
            default:
                return "";
        }
    }


    public List<Integer> getUniqueVariantsIds( String chr, int mapKey, int speciesTypeKey) throws Exception {
        String sql ="select v.rgd_id from variant v, variant_map_data vmd  " +
                "where v.rgd_id=vmd.rgd_id " +
                " and v.species_type_key=? " +
                " and vmd.chromosome=? " +
                " and vmd.map_key=?";  //Total RECORD COUNT: 1888283; chr:1; map_key:360
        //  VariantMapQuery q=new VariantMapQuery(DataSourceFactory.getInstance().getDataSource("Variant"), sql);
        IntListQuery q=new IntListQuery(DataSourceFactory.getInstance().getCarpeNovoDataSource(), sql);
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
    public List<VariantIndex> getVariantDocs(int mapKey, String chromosome, int limit, int offset) throws Exception {
        String sql="select v.*,vmd.*" ;
        sql+=   " from variant v " +
                " left outer join variant_map_data vmd on (vmd.rgd_id=v.rgd_id) " +
                  " where  vmd.map_key=? and vmd.chromosome=? Order by v.rgd_id OFFSET ? ROWS FETCH NEXT ? ROWS ONLY";

        VariantIndexQuery query=new VariantIndexQuery(DataSourceFactory.getInstance().getCarpeNovoDataSource(), sql);

        return execute(query, mapKey, chromosome,offset, limit);
    }
    public List<VariantIndex> getVariantsNewTbaleStructure1(  int mapKey, List<Integer> variantIdsList) throws Exception {

        String csTable=getConScoreTable(mapKey,null);
        String sql="select v.*,vmd.*, vsd.*,vt.*,t.acc_id as transcript_acc_id,t.protein_acc_id as protein_acc_id, p.prediction ,gl.gene_symbols as region_name, g.rgd_id as gene_rgd_id," +
                " g.gene_symbol_lc as gene_symbol_lc, md.strand as strand " ;

        if(!csTable.equals("")){
            sql+=" , cs.score ";
        }
        sql+=   " from variant v " +
                " left outer join variant_map_data vmd on (vmd.rgd_id=v.rgd_id) " +
                " left outer join variant_sample_detail vsd on (vsd.rgd_id=v.rgd_id) " +
                " left outer join variant_transcript vt on ( v.rgd_id=vt.variant_rgd_id )  " +
                " left outer join transcripts t on (t.transcript_rgd_id=vt.transcript_rgd_id) " +
                " left outer join genes g on (g.rgd_id=t.gene_rgd_id) " +
                " left outer join maps_data md on ( md.rgd_id=g.rgd_id and md.map_key=vmd.map_key) " +
                " left outer join polyphen  p on (vt.variant_rgd_id =p.variant_rgd_id and vt.transcript_rgd_id=p.transcript_rgd_id)   " ;
        if(!csTable.equals("")) {
            sql+=  " left outer join" + csTable + "cs on (cs.position=vmd.start_pos and cs.chr=vmd.chromosome)     ";
        }
        sql+=   " left outer join gene_loci gl on (gl.map_key=vmd.map_key and gl.chromosome=vmd.chromosome and gl.pos=vmd.start_pos)         " +
                " where  " +
                " v.rgd_id in (" ;
        //   "63409322)";
        String ids=  variantIdsList.stream().map(Object::toString).collect(Collectors.joining(","));
        sql=sql+ids;
        sql=sql+") ";

        sql=sql+ " and vsd.sample_id in (select sample_id from sample where map_key=?) "+
                " and vt.map_key=? " +
                " and vmd.map_key=?";

        VariantIndexQuery query=new VariantIndexQuery(DataSourceFactory.getInstance().getCarpeNovoDataSource(), sql);
        List<VariantIndex> objects=   execute(query, mapKey, mapKey,mapKey);
        // sortVariants(objects, variantIdsList, mapKey);
        return objects;
    }

    public List<VariantIndex> getVariantsNewTableStructure(  int mapKey, String chromosome) throws Exception {

        String sql="select v.*, vmd.* from variant v, variant_map_data vmd where " +
                "   vmd.rgd_id=v.rgd_id " +
                "   and vmd.chromosome=?" +
                "   and vmd.map_key=?";
        VariantQuery query=new VariantQuery(DataSourceFactory.getInstance().getCarpeNovoDataSource(), sql);
        return execute(query, chromosome,mapKey);
    }

    public void sortVariants(List<VariantIndex> indexList,  int mapKey) throws Exception {

        Set<Long> variantIdsWithTrancripts=new HashSet<>();
        Set<Long> uniqueVariantIds=indexList.stream().map(VariantIndex::getVariant_id).collect(Collectors.toSet());
        Set<Integer> sampleIds=indexList.stream().map(VariantIndex::getSampleId).collect(Collectors.toSet());
        for(long variantId:uniqueVariantIds) {

            for (int sampleId : sampleIds) {
                boolean first=true;
                VariantIndex indexDoc=null;
                for (VariantIndex variant : indexList) {
                    if (variant.getSampleId() == sampleId && variant.getVariant_id() == variantId) {
                        if(first){
                            indexDoc=variant;
                            first=false;
                        }
                        variantIdsWithTrancripts.add(variant.getVariant_id());
//                        String key = variant.getVariant_id() + "-" + variant.getSampleId() + "-" + variant.getMapKey();
                        if (mapKey == 38 || mapKey == 17) {
                            try {
                                String clinvarSignificance = getClinvarInfo((int) variant.getVariant_id());
                                if (clinvarSignificance != null && !clinvarSignificance.equals(""))
                                    indexDoc.setClinicalSignificance(clinvarSignificance);
                            } catch (Exception e) {
                                System.out.println("NO CLINICAL SIGNIFICACE SAMPLE_ID:" + variant.getSampleId() + " RGD_ID:" + variant.getVariant_id());
                            }
                        }

                        List<VariantTranscript> vTranscripts=new ArrayList<>() ;
                        if(  indexDoc.getVariantTranscripts()!=null){
                            vTranscripts.addAll(  indexDoc.getVariantTranscripts());
                        }
                        boolean exists = false;
                        if (variant.getVariantTranscripts() != null) {
                            for (VariantTranscript transcript : variant.getVariantTranscripts()) {
                                    for (VariantTranscript variantTranscript :  vTranscripts) {
                                        if (transcript.getTranscriptRgdId() == variantTranscript.getTranscriptRgdId()) {
                                            exists = true;
                                            break;
                                        }
                                    }
                                    if (!exists) {
                                        vTranscripts.add(transcript);
                                        indexDoc.setVariantTranscripts(vTranscripts);
                                    }

                            }
                        }

                    }
                }
                IndexDocument.index(indexDoc);
            }
        }


//        Set<Long> variantIdsWithoutTranscripts=new HashSet<>();
//        if(uniqueVariantIds.size()>variantIdsWithTrancripts.size()){
//            for(int id:uniqueVariantIds){
//                if(!variantIdsWithTrancripts.contains((long)id)){
//                    variantIdsWithoutTranscripts.add((long) id);
//                }
//            }
//            if(variantIdsWithoutTranscripts.size()>0) {
//                System.out.println("WITHOUT TRANSCRIPTS:"+ variantIdsWithoutTranscripts.size());
//                List<VariantIndex> variantsWithoutTranscripts = getVariantsWithoutTranscripts(mapKey, variantIdsWithoutTranscripts);
//                for(VariantIndex indexDoc:variantsWithoutTranscripts){
//                    IndexDocument.index(indexDoc);
//                }
//            }
//        }

    }
    public List<VariantIndex> getVariantsWithoutTranscripts(int mapKey,Set<Long> variantIdsWithoutTranscripts) throws Exception {
        String csTable=getConScoreTable(mapKey,null);
        String sql="select v.*,vmd.*, vsd.* ,gl.gene_symbols as region_name " ;
        if(!csTable.equals("")){
            sql+=" , cs.score ";
        }
        sql+=   " from variant v " +
                " left outer join variant_map_data vmd on (vmd.rgd_id=v.rgd_id) " +
                " left outer join variant_sample_detail vsd on (vsd.rgd_id=v.rgd_id) " ;

        if(!csTable.equals("")) {
            sql+=  " left outer join" + csTable + "cs on (cs.position=vmd.start_pos and cs.chr=vmd.chromosome) ";
        }
        sql+=   " left outer join gene_loci gl on (gl.map_key=vmd.map_key and gl.chromosome=vmd.chromosome and gl.pos=vmd.start_pos) " +
                " where  " +
                " v.rgd_id in (" ;
        //   "63409322)";
        String ids=  variantIdsWithoutTranscripts.stream().map(Object::toString).collect(Collectors.joining(","));
        sql=sql+ids;
        sql=sql+") ";
        sql=sql+ " and vsd.sample_id in (select sample_id from sample where map_key=?) "+
                " and vmd.map_key=?";
     //   System.out.println("SQL:"+sql);
        VariantIndexQuery query=new VariantIndexQuery(DataSourceFactory.getInstance().getCarpeNovoDataSource(), sql);

        return execute(query,mapKey,mapKey);
     //   return null;

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
    public String getClinvarInfo(int variantRgdId) throws Exception {
        VariantInfo info=variantInfoDAO.getVariant(variantRgdId)  ;
        return  info.getClinicalSignificance();
    }
}
