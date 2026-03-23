package edu.mcw.rgd.variantIndexerRgd.dao;

import edu.mcw.rgd.dao.AbstractDAO;
import edu.mcw.rgd.dao.DataSourceFactory;
import edu.mcw.rgd.dao.impl.VariantInfoDAO;
import edu.mcw.rgd.dao.spring.IntListQuery;
import edu.mcw.rgd.datamodel.VariantInfo;
import edu.mcw.rgd.datamodel.variants.VariantTranscript;
import edu.mcw.rgd.variantIndexerRgd.model.VariantIndex;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.sql.DataSource;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class VariantDao extends AbstractDAO {
    private static final Logger log = LogManager.getLogger(VariantDao.class);
    private static final int ORACLE_IN_LIMIT = 1000;

    private final VariantInfoDAO variantInfoDAO = new VariantInfoDAO();

    public String getConScoreTable(int mapKey, String genicStatus) {
        switch (mapKey) {
            case 17:  return " B37_CONSCORE_PART_IOT ";
            case 38:  return " CONSERVATION_SCORE_HG38 ";
            case 60:  return " CONSERVATION_SCORE ";
            case 70:  return " CONSERVATION_SCORE_5 ";
            case 360: return " CONSERVATION_SCORE_6 ";
            default:  return "";
        }
    }

    static String buildInClause(String column, List<Integer> ids) {
        if (ids.size() <= ORACLE_IN_LIMIT) {
            return column + " in (" + ids.stream().map(Object::toString).collect(Collectors.joining(",")) + ")";
        }
        StringBuilder sb = new StringBuilder("(");
        for (int i = 0; i < ids.size(); i += ORACLE_IN_LIMIT) {
            if (i > 0) sb.append(" or ");
            List<Integer> chunk = ids.subList(i, Math.min(i + ORACLE_IN_LIMIT, ids.size()));
            sb.append(column).append(" in (").append(chunk.stream().map(Object::toString).collect(Collectors.joining(","))).append(")");
        }
        sb.append(")");
        return sb.toString();
    }

    /**
     * Streams variant IDs in pages directly from DB, avoiding loading all IDs into memory.
     * Calls the processor for each page of IDs.
     */
    public void processVariantIdsByPage(String chr, int mapKey, int speciesTypeKey, int pageSize, VariantIdPageProcessor processor) throws Exception {
        String sql = "select distinct v.rgd_id from variant v, variant_map_data vmd " +
                "where v.rgd_id=vmd.rgd_id " +
                " and v.species_type_key=? " +
                " and vmd.chromosome=? " +
                " and vmd.map_key=? " +
                " order by v.rgd_id";
        DataSource ds = DataSourceFactory.getInstance().getCarpeNovoDataSource();
        try (Connection conn = ds.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setInt(1, speciesTypeKey);
            stmt.setString(2, chr);
            stmt.setInt(3, mapKey);
            stmt.setFetchSize(pageSize);
            try (ResultSet rs = stmt.executeQuery()) {
                List<Integer> page = new ArrayList<>(pageSize);
                while (rs.next()) {
                    page.add(rs.getInt("rgd_id"));
                    if (page.size() >= pageSize) {
                        processor.process(page);
                        page = new ArrayList<>(pageSize);
                    }
                }
                if (!page.isEmpty()) {
                    processor.process(page);
                }
            }
        }
    }

    @FunctionalInterface
    public interface VariantIdPageProcessor {
        void process(List<Integer> variantIds) throws Exception;
    }

    /**
     * Loads variant index data using parallel focused queries.
     * Runs base variants, transcripts, gene_loci, and conservation scores concurrently.
     */
    public List<VariantIndex> getVariantsForIndexing(int mapKey, List<Integer> variantIdsList) throws Exception {
        DataSource ds = DataSourceFactory.getInstance().getCarpeNovoDataSource();
        String csTable = getConScoreTable(mapKey, null);

        // Run all 4 queries in parallel
        ExecutorService pool = Executors.newFixedThreadPool(4);
        try {
            Future<Map<String, VariantIndex>> baseFuture = pool.submit(() -> loadBaseVariants(ds, variantIdsList, mapKey));
            Future<Map<Long, List<VariantTranscript>>> transcriptFuture = pool.submit(() -> loadTranscripts(ds, variantIdsList, mapKey));
            Future<Map<Long, List<String>>> geneLociFuture = pool.submit(() -> loadGeneLoci(ds, variantIdsList, mapKey));
            Future<Map<Long, List<String>>> conScoreFuture = !csTable.isEmpty()
                    ? pool.submit(() -> loadConservationScores(ds, variantIdsList, csTable))
                    : CompletableFuture.completedFuture(Collections.emptyMap());

            Map<String, VariantIndex> variantMap = baseFuture.get();
            Map<Long, List<VariantTranscript>> transcriptMap = transcriptFuture.get();
            Map<Long, List<String>> geneLociMap = geneLociFuture.get();
            Map<Long, List<String>> conScoreMap = conScoreFuture.get();

            // Merge results
            Set<Long> variantRgdIds = new HashSet<>();
            for (VariantIndex vi : variantMap.values()) {
                long vid = vi.getVariant_id();
                variantRgdIds.add(vid);

                List<VariantTranscript> vts = transcriptMap.get(vid);
                if (vts != null) vi.setVariantTranscripts(vts);

                List<String> regionNames = geneLociMap.get(vi.getStartPos());
                if (regionNames != null && !regionNames.isEmpty()) {
                    vi.setRegionName(regionNames);
                    List<String> lc = new ArrayList<>(regionNames.size());
                    for (String name : regionNames) lc.add(name.toLowerCase());
                    vi.setRegionNameLc(lc);
                }

                List<String> scores = conScoreMap.get(vi.getStartPos());
                if (scores != null && !scores.isEmpty()) vi.setConScores(scores);
            }

            if (mapKey == 38 || mapKey == 17) {
                loadClinvarBatch(variantRgdIds, variantMap);
            }

            return new ArrayList<>(variantMap.values());
        } finally {
            pool.shutdown();
        }
    }

    private Map<String, VariantIndex> loadBaseVariants(DataSource ds, List<Integer> variantIdsList, int mapKey) throws Exception {
        String inClause = buildInClause("v.rgd_id", variantIdsList);
        String sql = "select v.rgd_id, v.ref_nuc, v.var_nuc, v.variant_type, v.rs_id, v.clinvar_id, v.species_type_key," +
                " vmd.chromosome, vmd.start_pos, vmd.end_pos, vmd.genic_status, vmd.padding_base, vmd.map_key," +
                " vsd.sample_id, vsd.total_depth, vsd.var_freq, vsd.zygosity_status," +
                " vsd.zygosity_percent_read, vsd.zygosity_poss_error, vsd.zygosity_ref_allele," +
                " vsd.zygosity_num_allele, vsd.zygosity_in_pseudo, vsd.quality_score" +
                " from variant v" +
                " join variant_map_data vmd on (vmd.rgd_id=v.rgd_id)" +
                " join variant_sample_detail vsd on (vsd.rgd_id=v.rgd_id)" +
                " where " + inClause +
                " and vsd.sample_id in (select sample_id from sample where map_key=?)" +
                " and vmd.map_key=?";

        Map<String, VariantIndex> result = new LinkedHashMap<>();
        try (Connection conn = ds.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setInt(1, mapKey);
            stmt.setInt(2, mapKey);
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    VariantIndex vi = new VariantIndex();
                    vi.setCategory("Variant");
                    vi.setVariant_id(rs.getLong("rgd_id"));
                    vi.setRefNuc(rs.getString("ref_nuc"));
                    vi.setVarNuc(rs.getString("var_nuc"));
                    vi.setVariantType(rs.getString("variant_type"));
                    vi.setRsId(rs.getString("rs_id"));
                    vi.setClinvarId(rs.getString("clinvar_id"));
                    vi.setChromosome(rs.getString("chromosome"));
                    vi.setStartPos(rs.getLong("start_pos"));
                    vi.setEndPos(rs.getLong("end_pos"));
                    vi.setGenicStatus(rs.getString("genic_status"));
                    vi.setPaddingBase(rs.getString("padding_base"));
                    vi.setMapKey(rs.getInt("map_key"));
                    vi.setSampleId(rs.getInt("sample_id"));
                    vi.setTotalDepth(rs.getInt("total_depth"));
                    vi.setVarFreq(rs.getInt("var_freq"));
                    vi.setZygosityStatus(rs.getString("zygosity_status"));
                    vi.setZygosityPercentRead(rs.getDouble("zygosity_percent_read"));
                    vi.setZygosityPossError(rs.getString("zygosity_poss_error"));
                    vi.setZygosityRefAllele(rs.getString("zygosity_ref_allele"));
                    vi.setZygosityNumAllele(rs.getInt("zygosity_num_allele"));
                    vi.setZygosityInPseudo(rs.getString("zygosity_in_pseudo"));
                    vi.setQualityScore(rs.getInt("quality_score"));
                    String key = vi.getVariant_id() + "-" + vi.getSampleId() + "-" + vi.getMapKey();
                    result.putIfAbsent(key, vi);
                }
            }
        }
        return result;
    }

    private Map<Long, List<VariantTranscript>> loadTranscripts(DataSource ds, List<Integer> variantIdsList, int mapKey) throws Exception {
        String inClause = buildInClause("vt.variant_rgd_id", variantIdsList);
        String sql = "select vt.variant_rgd_id, vt.transcript_rgd_id, vt.ref_aa, vt.var_aa," +
                " vt.syn_status, vt.location_name, vt.near_splice_site," +
                " vt.full_ref_nuc_pos, vt.full_ref_aa_pos, vt.triplet_error, vt.frameshift," +
                " t.acc_id as transcript_acc_id, t.protein_acc_id," +
                " p.prediction, g.rgd_id as gene_rgd_id, g.gene_symbol_lc, md.strand" +
                " from variant_transcript vt" +
                " left outer join transcripts t on (t.transcript_rgd_id=vt.transcript_rgd_id)" +
                " left outer join genes g on (g.rgd_id=t.gene_rgd_id)" +
                " left outer join maps_data md on (md.rgd_id=g.rgd_id and md.map_key=?)" +
                " left outer join polyphen p on (vt.variant_rgd_id=p.variant_rgd_id and vt.transcript_rgd_id=p.transcript_rgd_id)" +
                " where " + inClause +
                " and vt.map_key=?";

        Map<Long, List<VariantTranscript>> result = new HashMap<>();
        Set<String> seen = new HashSet<>();
        try (Connection conn = ds.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setInt(1, mapKey);
            stmt.setInt(2, mapKey);
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    long variantId = rs.getLong("variant_rgd_id");
                    int transcriptId = rs.getInt("transcript_rgd_id");
                    String dedupKey = variantId + "-" + transcriptId;
                    if (!seen.add(dedupKey)) continue;

                    VariantTranscript vt = new VariantTranscript();
                    vt.setTranscriptRgdId(transcriptId);
                    vt.setRefAA(rs.getString("ref_aa"));
                    vt.setVarAA(rs.getString("var_aa"));
                    vt.setSynStatus(rs.getString("syn_status"));
                    vt.setLocationName(rs.getString("location_name"));
                    vt.setNearSpliceSite(rs.getString("near_splice_site"));
                    vt.setFullRefNucPos(rs.getInt("full_ref_nuc_pos"));
                    vt.setFullRefAAPos(rs.getInt("full_ref_aa_pos"));
                    vt.setTripletError(rs.getString("triplet_error"));
                    vt.setFrameShift(rs.getString("frameshift"));
                    vt.setPolyphenStatus(rs.getString("prediction"));
                    try {
                        vt.setTranscriptSymbol(rs.getString("transcript_acc_id"));
                        vt.setProteinSymbol(rs.getString("protein_acc_id"));
                    } catch (Exception ignored) {}
                    result.computeIfAbsent(variantId, k -> new ArrayList<>()).add(vt);
                }
            }
        }
        return result;
    }

    private Map<Long, List<String>> loadGeneLoci(DataSource ds, List<Integer> variantIdsList, int mapKey) throws Exception {
        String inClause = buildInClause("vmd.rgd_id", variantIdsList);
        String sql = "select distinct gl.pos, gl.gene_symbols" +
                " from gene_loci gl, variant_map_data vmd" +
                " where " + inClause +
                " and vmd.map_key=?" +
                " and gl.map_key=vmd.map_key and gl.chromosome=vmd.chromosome and gl.pos=vmd.start_pos";

        Map<Long, List<String>> result = new HashMap<>();
        try (Connection conn = ds.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setInt(1, mapKey);
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    long pos = rs.getLong("pos");
                    String symbol = rs.getString("gene_symbols");
                    if (symbol != null) {
                        result.computeIfAbsent(pos, k -> new ArrayList<>()).add(symbol);
                    }
                }
            }
        }
        return result;
    }

    private Map<Long, List<String>> loadConservationScores(DataSource ds, List<Integer> variantIdsList, String csTable) throws Exception {
        String inClause = buildInClause("vmd.rgd_id", variantIdsList);
        String sql = "select distinct vmd.start_pos, cs.score" +
                " from variant_map_data vmd" +
                " join" + csTable + "cs on (cs.position=vmd.start_pos and cs.chr=vmd.chromosome)" +
                " where " + inClause;

        Map<Long, List<String>> result = new HashMap<>();
        try (Connection conn = ds.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    long pos = rs.getLong("start_pos");
                    String score = rs.getString("score");
                    if (score != null) {
                        result.computeIfAbsent(pos, k -> new ArrayList<>()).add(score);
                    }
                }
            }
        }
        return result;
    }

    private void loadClinvarBatch(Set<Long> variantRgdIds, Map<String, VariantIndex> variantMap) {
        Map<Long, String> clinvarCache = new HashMap<>();
        for (long vid : variantRgdIds) {
            try {
                VariantInfo info = variantInfoDAO.getVariant((int) vid);
                String sig = info.getClinicalSignificance();
                clinvarCache.put(vid, sig != null && !sig.isEmpty() ? sig : null);
            } catch (Exception e) {
                clinvarCache.put(vid, null);
            }
        }
        for (VariantIndex vi : variantMap.values()) {
            String sig = clinvarCache.get(vi.getVariant_id());
            if (sig != null) {
                vi.setClinicalSignificance(sig);
            }
        }
    }
}
