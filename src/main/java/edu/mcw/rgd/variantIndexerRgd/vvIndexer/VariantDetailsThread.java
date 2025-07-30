package edu.mcw.rgd.variantIndexerRgd.vvIndexer;

import edu.mcw.rgd.datamodel.*;
import edu.mcw.rgd.datamodel.variants.VariantIndex;
import edu.mcw.rgd.datamodel.variants.VariantTranscript;

import edu.mcw.rgd.services.IndexDocument;


import java.util.*;
import java.util.Map;
import java.util.stream.Collectors;

public class VariantDetailsThread implements Runnable{
    private final VariantIndex variant;
    private final List<VariantIndex> variantDetails;

    private final List<ConservationScore> conservationScores;
    private final Map<Integer, Set<String>> clinicalSignificance;
    private final List<GeneLoci> geneLoci;
    private final int mapKey;


    public VariantDetailsThread(int mapKey, VariantIndex variant, List<VariantIndex> variantDetails, List<ConservationScore> conservationScores, Map<Integer, Set<String>> clinicalSignificance, List<GeneLoci> geneLoci) throws Exception {
        this.variant=variant;
        this.mapKey=mapKey;
        this.variantDetails = variantDetails;
        this.conservationScores = conservationScores;
        this.clinicalSignificance = clinicalSignificance;
        this.geneLoci=geneLoci;

    }
    @Override
    public void run() {
        List<VariantIndex> filteredDetails=variantDetails.stream().filter(v->v.getVariant_id()==variant.getVariant_id()).collect(Collectors.toList());
        Set<Integer> uniqueSampleIds=filteredDetails.stream().map(VariantIndex::getSampleId).collect(Collectors.toSet());
        mapConservationScore(variant);
        mapGeneLoci(variant);
        if (mapKey == 38 || mapKey == 17) {
            mapClinicalSignificance(variant);
        }
        mapTranscripts(variant, filteredDetails);
        mapGene(variant, filteredDetails);

        for(int sampleId:uniqueSampleIds) {
            mapSampleDetails(variant, sampleId, filteredDetails);
            IndexDocument.index(variant);
        }

    }
    public void mapClinicalSignificance(VariantIndex variant){
        try {
            Set<String> clinvarSignificance =clinicalSignificance.get((int)variant.getVariant_id());
            if (clinvarSignificance != null)
                variant.setClinicalSignificance(String.join(",", clinvarSignificance));
        } catch (Exception e) {
            System.out.println("NO CLINICAL SIGNIFICANCE SAMPLE_ID:" + variant.getSampleId() + " RGD_ID:" + variant.getVariant_id());
        }
    }
    public void mapSampleDetails( VariantIndex vi,int sampleId, List<VariantIndex> filteredDetails){

        for(VariantIndex variant:filteredDetails) {
            if(sampleId==variant.getSampleId()) {
                vi.setSampleId(variant.getSampleId());
                vi.setAnalysisName(variant.getAnalysisName());
                vi.setTotalDepth(variant.getTotalDepth());
                vi.setVarFreq(variant.getVarFreq());
                vi.setZygosityStatus(variant.getZygosityStatus());
                vi.setZygosityPercentRead(variant.getZygosityPercentRead());
                vi.setZygosityPossError(variant.getZygosityPossError());
                vi.setZygosityRefAllele(variant.getZygosityRefAllele());
                vi.setZygosityNumAllele(variant.getZygosityNumAllele());
                vi.setZygosityInPseudo(variant.getZygosityInPseudo());
                vi.setQualityScore(variant.getQualityScore());
            }
        }
    }
    public void mapTranscripts(VariantIndex variant, List<VariantIndex> variantDetails){
        List<VariantTranscript>vtranscripts=new ArrayList<>();
        for (VariantIndex details : variantDetails) {
            if(details.getVariantTranscripts()!=null){
                vtranscripts.addAll(details.getVariantTranscripts());
            }
            if(details.getVariantTranscripts()!=null && details.getVariantTranscripts().size()>0) {

                for (VariantTranscript t : details.getVariantTranscripts()) {
                    boolean exists=false;
                    for (VariantTranscript vt : vtranscripts) {
                        if (vt.getTranscriptRgdId() == t.getTranscriptRgdId()) {
                            exists=true;
                            break;

                        }

                    }
                    if(!exists){
                        vtranscripts.add(t);
                    }
                }

            }

        }
        variant.setVariantTranscripts(vtranscripts);
    }
    public void mapGene(VariantIndex variant,List<VariantIndex> variantDetails){
        Set<Integer> geneRgdIds = variantDetails.stream().map(VariantIndex::getGeneRgdId).collect(Collectors.toSet());
        Set<String> geneSymbols = variantDetails.stream().map(VariantIndex::getGeneSymbol).collect(Collectors.toSet());
        Set<String> strand = variantDetails.stream().map(VariantIndex::getStrand).filter(Objects::nonNull).collect(Collectors.toSet());
        if(geneRgdIds.size()>0) {
            variant.setGeneSymbol(String.join(", ", geneSymbols));
            variant.setGeneRgdId((new ArrayList<>(geneRgdIds)).get(0));
            variant.setStrand(String.join(",", strand));
        }
    }
    public void mapConservationScore(VariantIndex vi){
        if(conservationScores!=null && conservationScores.size()>0){
            vi.setConScores(conservationScores.stream().map(c->c.getScore().toString()).collect(Collectors.toList()));
        }
    }
    public void mapGeneLoci(VariantIndex variant){
        List<String> regionNames = new ArrayList<>();
        for(GeneLoci gl:geneLoci){
            if(gl.getMapKey()==mapKey && Objects.equals(gl.getChromosome(), variant.getChromosome()) && gl.getPosition()==variant.getStartPos()){

                if (gl.getGeneSymbols()!=null) {
                    regionNames.add(gl.getGeneSymbols());

                }
            }

        }
        variant.setRegionName(regionNames);
        variant.setRegionNameLc(regionNames.stream().map(String::toLowerCase).collect(Collectors.toList()));
    }
//    public void mapVariant( VariantIndex vi){
//        vi.setCategory("Variant");
//        vi.setVariant_id(variant.getVariant_id());
//        vi.setChromosome(variant.getChromosome());
//        vi.setPaddingBase(variant.getPaddingBase());
//        vi.setEndPos(variant.getEndPos());
//        vi.setRefNuc(variant.getRefNuc());
//        vi.setStartPos(variant.getStartPos());
//        vi.setVariantType(variant.getVariantType());
//        vi.setVarNuc(variant.getVarNuc());
//        vi.setGenicStatus(variant.getGenicStatus());
//        vi.setMapKey(mapKey);
//        try {
//            vi.setRsId(variant.getRsId());
//        }catch (Exception e){
//
//        }

//    }
//    public void mapSampleDetails( VariantIndex vi, VariantSampleDetail variant){
//
//        vi.setSampleId(variant.getSampleId());
//        vi.setAnalysisName(variant.getAnalysisName());
//        vi.setTotalDepth(variant.getDepth());
//        vi.setVarFreq(variant.getVariantFrequency());
//        vi.setZygosityStatus(variant.getZygosityStatus());
//        vi.setZygosityPercentRead(variant.getZygosityPercentRead());
//        vi.setZygosityPossError(variant.getZygosityPossibleError());
//        vi.setZygosityRefAllele(variant.getZygosityRefAllele());
//        vi.setZygosityNumAllele(variant.getZygosityNumberAllele());
//        vi.setZygosityInPseudo(variant.getZygosityInPseudo());
//        vi.setQualityScore(variant.getQualityScore());
//    }
//    public void mapTranscripts(VariantIndex vi){
//       vi.setVariantTranscripts(transcripts);
//    }
//
//    public void setTranscripts() throws Exception {
//       this. transcripts= variantDAO.getVariantTranscripts(variant.getVariant_id(), mapKey);
//    }
//    public void setGeneDetails() throws Exception {
//        if(transcripts!=null && transcripts.size()>0){
//        Set<Integer> transcriptIds=transcripts.stream().map(VariantTranscript::getTranscriptRgdId).collect(Collectors.toSet());
//       this.genes=geneDAO.getGenesByTranscriptIdsList(new ArrayList<>(transcriptIds));
//    }
//    }
//    public void mapGeneDetails(VariantIndex vi) throws  Exception{
//        if(genes!=null && genes.size()>0) {
//            Set<Integer> geneRgdIds = genes.stream().map(g -> g.getRgdId()).collect(Collectors.toSet());
//            Set<String> geneSymbols = genes.stream().map(g -> g.getSymbol()).collect(Collectors.toSet());
//            vi.setGeneSymbol(String.join(", ", geneSymbols));
//            vi.setGeneRgdId((new ArrayList<>(geneRgdIds)).get(0));
//        }
//    }
//
//    public void mapMapData(VariantIndex vi) throws  Exception{
//        if(mapData!=null && mapData.size()>0) {
//            Set<String> strands=mapData.stream().filter(Objects::nonNull).map(MapData::getStrand).collect(Collectors.toSet());
//            String geneStrand= String.join(",", strands);
//            vi.setStrand(geneStrand);
//        }
//
//    }
//    public void setMapData() throws  Exception{
//        if(genes!=null && genes.size()>0) {
//            Set<Integer> geneRgdIds = genes.stream().map(Gene::getRgdId).collect(Collectors.toSet());
//            this. mapData= mapDAO.getMapData(new ArrayList<>(geneRgdIds));
//
//        }
//
//    }
//    public void setConservationScore() throws Exception {
//        VariantSearchBean vsb=new VariantSearchBean(mapKey);
//        String csTable=vsb.getConScoreTable();
//        if(csTable!=null && !csTable.equals("")) {
//            this.conservationScores = variantDAO.getConservationScores(variant.getStartPos(), variant.getChromosome(), csTable);
//        }
//    }
//    public void setSampleDetails() throws Exception {
//        this.sampleDetails=variantDAO.getVariantSampleDetail((int) variant.getVariant_id());
//    }
//    public void setClinicalSignificance(VariantIndex vi){
//        if (mapKey == 38 || mapKey == 17) {
//            try {
//                String clinvarSignificance = getClinvarInfo((int) variant.getVariant_id());
//                if (clinvarSignificance != null && !clinvarSignificance.equals(""))
//                    vi.setClinicalSignificance(clinvarSignificance);
//            } catch (Exception e) {
//                System.out.println("NO CLINICAL SIGNIFICACE SAMPLE_ID:" + variant.getSampleId() + " RGD_ID:" + variant.getVariant_id());
//            }
//        }
//    }
//    public String getClinvarInfo(int variantRgdId) throws Exception {
//        VariantInfo info=variantInfoDAO.getVariant(variantRgdId)  ;
//        return  info.getClinicalSignificance();
//    }
}
