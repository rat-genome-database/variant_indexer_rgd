package edu.mcw.rgd.variantIndexerRgd.vvIndexer;

import edu.mcw.rgd.dao.impl.GeneDAO;

import edu.mcw.rgd.dao.impl.MapDAO;

import edu.mcw.rgd.dao.impl.VariantInfoDAO;
import edu.mcw.rgd.dao.impl.variants.VariantDAO;
import edu.mcw.rgd.datamodel.*;
import edu.mcw.rgd.datamodel.variants.VariantIndex;
import edu.mcw.rgd.datamodel.variants.VariantSampleDetail;
import edu.mcw.rgd.datamodel.variants.VariantTranscript;

import edu.mcw.rgd.services.IndexDocument;


import java.util.*;
import java.util.stream.Collectors;

public class VariantDetailsThread implements Runnable{
    private final VariantIndex variant;
    private List<VariantTranscript> transcripts;
    public List<VariantSampleDetail> sampleDetails;
    private List<ConservationScore> conservationScores;

    private final List<GeneLoci> geneLoci;
    private List<Gene> genes;
    private final int mapKey;
    private List<MapData> mapData;

    VariantDAO variantDAO=new VariantDAO();
    GeneDAO geneDAO=new GeneDAO();
    MapDAO mapDAO=new MapDAO();
    VariantInfoDAO variantInfoDAO=new VariantInfoDAO();

    public VariantDetailsThread(int mapKey, VariantIndex variant, List<GeneLoci> geneLoci) throws Exception {
        this.variant=variant;
        this.mapKey=mapKey;
        this.geneLoci=geneLoci;
        setTranscripts();
        setGeneDetails();
        setMapData();
        setConservationScore();
        setSampleDetails();

    }
    @Override
    public void run() {
        VariantIndex variantIndex=new VariantIndex();
        mapVariant(variantIndex);
        mapTranscripts(variantIndex);
        mapConservationScore(variantIndex);
        mapGeneLoci(variantIndex);
        try {
            mapGeneDetails(variantIndex);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        try {
            mapMapData(variantIndex);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        setClinicalSignificance(variantIndex);
        for(VariantSampleDetail vsd:sampleDetails){
            mapSampleDetails(variantIndex,vsd);
            IndexDocument.index(variantIndex);
        }

    }
    public void mapVariant( VariantIndex vi){
        vi.setCategory("Variant");
        vi.setVariant_id(variant.getVariant_id());
        vi.setChromosome(variant.getChromosome());
        vi.setPaddingBase(variant.getPaddingBase());
        vi.setEndPos(variant.getEndPos());
        vi.setRefNuc(variant.getRefNuc());
        vi.setStartPos(variant.getStartPos());
        vi.setVariantType(variant.getVariantType());
        vi.setVarNuc(variant.getVarNuc());
        vi.setGenicStatus(variant.getGenicStatus());
        vi.setMapKey(mapKey);
        try {
            vi.setRsId(variant.getRsId());
        }catch (Exception e){

        }

    }
    public void mapSampleDetails( VariantIndex vi, VariantSampleDetail variant){

        vi.setSampleId(variant.getSampleId());
        vi.setAnalysisName(variant.getAnalysisName());
        vi.setTotalDepth(variant.getDepth());
        vi.setVarFreq(variant.getVariantFrequency());
        vi.setZygosityStatus(variant.getZygosityStatus());
        vi.setZygosityPercentRead(variant.getZygosityPercentRead());
        vi.setZygosityPossError(variant.getZygosityPossibleError());
        vi.setZygosityRefAllele(variant.getZygosityRefAllele());
        vi.setZygosityNumAllele(variant.getZygosityNumberAllele());
        vi.setZygosityInPseudo(variant.getZygosityInPseudo());
        vi.setQualityScore(variant.getQualityScore());
    }
    public void mapTranscripts(VariantIndex vi){
       vi.setVariantTranscripts(transcripts);
    }
    public void mapConservationScore(VariantIndex vi){
        if(conservationScores!=null && conservationScores.size()>0){
            vi.setConScores(conservationScores.stream().map(c->c.getScore().toString()).collect(Collectors.toList()));
        }
    }
    public void mapGeneLoci(VariantIndex variantindex){
        List<String> regionNames = new ArrayList<>();
       for(GeneLoci gl:geneLoci){
           if(gl.getMapKey()==mapKey && Objects.equals(gl.getChromosome(), variantindex.getChromosome()) && gl.getPosition()==variant.getStartPos()){

               if (gl.getGeneSymbols()!=null) {
                   regionNames.add(gl.getGeneSymbols());

               }
           }

       }
        variantindex.setRegionName(regionNames);
       variantindex.setRegionNameLc(regionNames.stream().map(String::toLowerCase).collect(Collectors.toList()));
    }
    public void setTranscripts() throws Exception {
       this. transcripts= variantDAO.getVariantTranscripts(variant.getVariant_id(), mapKey);
    }
    public void setGeneDetails() throws Exception {
        if(transcripts!=null && transcripts.size()>0){
        Set<Integer> transcriptIds=transcripts.stream().map(VariantTranscript::getTranscriptRgdId).collect(Collectors.toSet());
       this.genes=geneDAO.getGenesByTranscriptIdsList(new ArrayList<>(transcriptIds));
    }
    }
    public void mapGeneDetails(VariantIndex vi) throws  Exception{
        if(genes!=null && genes.size()>0) {
            Set<Integer> geneRgdIds = genes.stream().map(g -> g.getRgdId()).collect(Collectors.toSet());
            Set<String> geneSymbols = genes.stream().map(g -> g.getSymbol()).collect(Collectors.toSet());
            vi.setGeneSymbol(String.join(", ", geneSymbols));
            vi.setGeneRgdId((new ArrayList<>(geneRgdIds)).get(0));
        }
    }

    public void mapMapData(VariantIndex vi) throws  Exception{
        if(mapData!=null && mapData.size()>0) {
            Set<String> strands=mapData.stream().filter(Objects::nonNull).map(MapData::getStrand).collect(Collectors.toSet());
            String geneStrand= String.join(",", strands);
            vi.setStrand(geneStrand);
        }

    }
    public void setMapData() throws  Exception{
        if(genes!=null && genes.size()>0) {
            Set<Integer> geneRgdIds = genes.stream().map(Gene::getRgdId).collect(Collectors.toSet());
            this. mapData= mapDAO.getMapData(new ArrayList<>(geneRgdIds));

        }

    }
    public void setConservationScore() throws Exception {
        VariantSearchBean vsb=new VariantSearchBean(mapKey);
        String csTable=vsb.getConScoreTable();
        if(csTable!=null && !csTable.equals("")) {
            this.conservationScores = variantDAO.getConservationScores(variant.getStartPos(), variant.getChromosome(), csTable);
        }
    }
    public void setSampleDetails() throws Exception {
        this.sampleDetails=variantDAO.getVariantSampleDetail((int) variant.getVariant_id());
    }
    public void setClinicalSignificance(VariantIndex vi){
        if (mapKey == 38 || mapKey == 17) {
            try {
                String clinvarSignificance = getClinvarInfo((int) variant.getVariant_id());
                if (clinvarSignificance != null && !clinvarSignificance.equals(""))
                    vi.setClinicalSignificance(clinvarSignificance);
            } catch (Exception e) {
                System.out.println("NO CLINICAL SIGNIFICACE SAMPLE_ID:" + variant.getSampleId() + " RGD_ID:" + variant.getVariant_id());
            }
        }
    }
    public String getClinvarInfo(int variantRgdId) throws Exception {
        VariantInfo info=variantInfoDAO.getVariant(variantRgdId)  ;
        return  info.getClinicalSignificance();
    }
}
