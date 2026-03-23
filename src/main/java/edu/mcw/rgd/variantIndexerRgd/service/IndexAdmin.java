package edu.mcw.rgd.variantIndexerRgd.service;

import edu.mcw.rgd.services.ClientInit;
import edu.mcw.rgd.services.RgdContext;
import edu.mcw.rgd.variantIndexerRgd.model.RgdIndex;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.client.GetAliasesResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xcontent.XContentType;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Manages Elasticsearch index creation and alias resolution.
 * Replaces the rgdcore IndexAdmin to fix alias switching logic.
 */
public class IndexAdmin {
    private static final Logger log = LogManager.getLogger(IndexAdmin.class);

    /**
     * For "reindex" command: determines which index the alias currently points to,
     * picks the other index as the new target, deletes and recreates it,
     * and sets newAlias/oldAlias on RgdIndex.
     */
    public void createIndex(String mappings, String species) throws Exception {

        String path="";
        if(mappings!=null && !mappings.equals("")){
            path+="data/"+mappings+".json";
            mappings=new String(Files.readAllBytes(Paths.get(path)));
        }
        RgdIndex rgdIndex = RgdIndex.getInstance();
        String aliasName = rgdIndex.getIndex();
        List<String> indices = rgdIndex.getIndices();
        String index1 = indices.get(0); // e.g. variants_rat372_dev1
        String index2 = indices.get(1); // e.g. variants_rat372_dev2

        RestHighLevelClient client = ClientInit.getClient();

        // Find which index the alias currently points to
        String currentIndex = resolveAlias(client, aliasName);
        log.info("Alias '" + aliasName + "' currently points to: " + (currentIndex != null ? currentIndex : "NONE"));

        String newIndex;
        String oldIndex;
        if (index1.equals(currentIndex)) {
            // Alias points to index1, so write to index2
            newIndex = index2;
            oldIndex = index1;
        } else {
            // Alias points to index2 (or doesn't exist yet), so write to index1
            newIndex = index1;
            oldIndex = currentIndex; // null if alias doesn't exist yet
        }

        // Delete and recreate the new target index
        boolean exists = client.indices().exists(new GetIndexRequest(newIndex), RequestOptions.DEFAULT);
        if (exists) {
            log.info("Deleting existing index: " + newIndex);
            client.indices().delete(
                    new org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest(newIndex),
                    RequestOptions.DEFAULT);
        }
        log.info("Creating index: " + newIndex);
        int replicates=0;
        int shards=5;
//        if(!index.contains("dev") && !index.contains("test")){
        if(RgdContext.isProduction() || RgdContext.isPipelines()){
            replicates=1;
        }
        String analyzers=null;
        try {
            analyzers=  new String(Files.readAllBytes(Paths.get("data/analyzers.json")));
        }catch (Exception ignored){}

        /********* create index, put mappings and analyzers ****/
        CreateIndexRequest request=new CreateIndexRequest(newIndex);
        if(analyzers!=null) {
            request.settings(Settings.builder()
                    .put("index.number_of_shards", shards)
                    .put("index.number_of_replicas", replicates)
                    .loadFromSource(analyzers, XContentType.JSON));
        }else{
            request.settings(Settings.builder()
                    .put("index.number_of_shards", shards)
                    .put("index.number_of_replicas", replicates));
        }
        if(mappings!=null)
            request.mapping(mappings, XContentType.JSON);
        org.elasticsearch.client.indices.CreateIndexResponse createIndexResponse = ClientInit.getClient().indices().create(request, RequestOptions.DEFAULT);

        log.info("Index created: " + newIndex + " acknowledged=" + createIndexResponse.isAcknowledged());

        // Set on RgdIndex so the pipeline writes to newIndex and switchAlias knows what to swap
        rgdIndex.setNewAlias(newIndex);
        rgdIndex.setOldAlias(oldIndex);

        log.info("Will index into: " + newIndex + " | Will switch alias from: " + oldIndex);
    }

    /**
     * For "update" command: data goes into whichever index the alias currently points to.
     * No index recreation, no alias switching needed.
     */
    public int updateIndex() throws Exception {
        RgdIndex rgdIndex = RgdIndex.getInstance();
        String aliasName = rgdIndex.getIndex();

        RestHighLevelClient client = ClientInit.getClient();
        String currentIndex = resolveAlias(client, aliasName);

        if (currentIndex != null) {
            rgdIndex.setNewAlias(currentIndex);
            rgdIndex.setOldAlias(null);
            log.info("Update mode: writing to existing index " + currentIndex);
        } else {
            // No alias exists yet — use first index
            String index1 = rgdIndex.getIndices().get(0);
            rgdIndex.setNewAlias(index1);
            rgdIndex.setOldAlias(null);
            log.info("Update mode: no alias found, writing to " + index1);
        }
        return 0;
    }

    /**
     * Resolves which concrete index an alias currently points to.
     * Returns null if the alias doesn't exist.
     */
    private String resolveAlias(RestHighLevelClient client, String aliasName) throws Exception {
        try {
            GetAliasesRequest request = new GetAliasesRequest(aliasName);
            GetAliasesResponse response = client.indices().getAlias(request, RequestOptions.DEFAULT);
            Map<String, Set<AliasMetadata>> aliases = response.getAliases();
            if (aliases != null && !aliases.isEmpty()) {
                // Return the first index that has this alias
                return aliases.keySet().iterator().next();
            }
        } catch (Exception e) {
            log.info("Alias '" + aliasName + "' not found: " + e.getMessage());
        }
        return null;
    }
}
