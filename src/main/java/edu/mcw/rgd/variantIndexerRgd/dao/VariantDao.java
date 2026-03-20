package edu.mcw.rgd.variantIndexerRgd.dao;

import edu.mcw.rgd.dao.AbstractDAO;
import edu.mcw.rgd.dao.DataSourceFactory;

import edu.mcw.rgd.dao.impl.VariantInfoDAO;
import edu.mcw.rgd.dao.spring.ConservationScoreMapper;
import edu.mcw.rgd.dao.spring.IntListQuery;
import edu.mcw.rgd.dao.spring.variants.*;
import edu.mcw.rgd.datamodel.ConservationScore;
import edu.mcw.rgd.datamodel.VariantInfo;
import edu.mcw.rgd.datamodel.variants.VariantMapData;
import edu.mcw.rgd.datamodel.variants.VariantObject;
import edu.mcw.rgd.datamodel.variants.VariantSampleDetail;
import edu.mcw.rgd.datamodel.variants.VariantTranscript;
import edu.mcw.rgd.process.Utils;
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
        String sql ="select distinct v.rgd_id from variant v, variant_map_data vmd  " +
                "where v.rgd_id=vmd.rgd_id " +
                " and v.species_type_key=? " +
                " and vmd.chromosome=? " +
                " and vmd.map_key=?";
        //  VariantMapQuery q=new VariantMapQuery(DataSourceFactory.getInstance().getDataSource("Variant"), sql);
        IntListQuery q=new IntListQuery(DataSourceFactory.getInstance().getCarpeNovoDataSource(), sql);
        return execute(q,speciesTypeKey,chr,mapKey);
    }
    public List<VariantIndex> getVariantsNewTbaleStructure(  int mapKey, List<Integer> variantIdsList) throws Exception {

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
        List<VariantIndex> variants=  execute(query, mapKey, mapKey,mapKey);

        // Batch-load clinvar info once for human assemblies instead of N+1 per-row queries
        Map<Long, String> clinvarCache = new HashMap<>();
        if(mapKey==38 || mapKey==17){
            for(VariantIndex variant:variants){
                long vid = variant.getVariant_id();
                if(!clinvarCache.containsKey(vid)){
                    try {
                        String sig = getClinvarInfo((int) vid);
                        clinvarCache.put(vid, sig != null && !sig.isEmpty() ? sig : null);
                    }catch (Exception e){
                        clinvarCache.put(vid, null);
                    }
                }
            }
        }

        java.util.Map<String, VariantIndex> sortedVariants=new LinkedHashMap<>();
        Set<Long> variantIdsWithTranscripts=new HashSet<>();
        for(VariantIndex variant:variants){
            variantIdsWithTranscripts.add(variant.getVariant_id());
            String key=variant.getVariant_id()+"-"+variant.getSampleId()+"-"+variant.getMapKey();

            String clinvarSig = clinvarCache.get(variant.getVariant_id());
            if(clinvarSig != null){
                variant.setClinicalSignificance(clinvarSig);
            }

            if(!sortedVariants.containsKey(key)){
                sortedVariants.put(key,variant);
            }else{
                VariantIndex obj = sortedVariants.get(key);
                if(variant.getVariantTranscripts()!=null && obj != null) {
                    List<VariantTranscript> vtranscripts = obj.getVariantTranscripts();
                    for (VariantTranscript transcript : variant.getVariantTranscripts()) {
                        boolean exists = false;
                        for (VariantTranscript variantTranscript : vtranscripts) {
                            if (transcript.getTranscriptRgdId() == variantTranscript.getTranscriptRgdId()) {
                                exists = true;
                                break;
                            }
                        }
                        if (!exists) {
                            vtranscripts.add(transcript);
                        }
                    }
                    obj.setVariantTranscripts(vtranscripts);
                }
            }
        }

        List<VariantIndex> vrList=new ArrayList<>(sortedVariants.values());
        Set<Long> variantIdsWithoutTranscripts=new HashSet<>();
        Set<Long> distinctInputIds = new HashSet<>();
        for(int id : variantIdsList) {
            distinctInputIds.add((long) id);
        }
        for(long id : distinctInputIds){
            if(!variantIdsWithTranscripts.contains(id)){
                variantIdsWithoutTranscripts.add(id);
            }
        }
        if(!variantIdsWithoutTranscripts.isEmpty()){
            List<VariantIndex> variantsWithoutTranscripts = getVariantsWithoutTranscripts(mapKey, variantIdsWithoutTranscripts);
            // Deduplicate: gene_loci and conservation_score joins can produce multiple rows per variant+sample
            Map<String, VariantIndex> dedupMap = new LinkedHashMap<>();
            for (VariantIndex vi : variantsWithoutTranscripts) {
                String key = vi.getVariant_id() + "-" + vi.getSampleId() + "-" + vi.getMapKey();
                dedupMap.putIfAbsent(key, vi);
            }
            vrList.addAll(dedupMap.values());
        }
        return vrList;
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

    public String getClinvarInfo(int variantRgdId) throws Exception {
        VariantInfo info=variantInfoDAO.getVariant(variantRgdId)  ;
        return  info.getClinicalSignificance();
    }
}
