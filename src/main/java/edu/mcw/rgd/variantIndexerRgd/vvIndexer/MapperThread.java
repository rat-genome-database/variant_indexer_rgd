package edu.mcw.rgd.variantIndexerRgd.vvIndexer;

import edu.mcw.rgd.dao.impl.VariantInfoDAO;
import edu.mcw.rgd.dao.impl.variants.VariantDAO;
import edu.mcw.rgd.datamodel.ConservationScore;
import edu.mcw.rgd.datamodel.GeneLoci;
import edu.mcw.rgd.datamodel.VariantSearchBean;
import edu.mcw.rgd.datamodel.variants.VariantIndex;
import edu.mcw.rgd.datamodel.variants.VariantTranscript;
import edu.mcw.rgd.services.IndexDocument;

import java.util.*;
import java.util.stream.Collectors;


public class MapperThread implements Runnable{
    private final List<VariantIndex> variants;
    private final List<VariantIndex> variantDetails;
    private final int mapKey;
    private  Map<Integer, Set<String>> clinicalSignificance;
    private  List<ConservationScore> conservationScores;
    private final String chromosome;

    private final List<GeneLoci> geneLoci;

    VariantDAO variantDAO=new VariantDAO();
    VariantInfoDAO variantInfoDAO=new VariantInfoDAO();
    public MapperThread(List<VariantIndex> variants, List<VariantIndex> variantDetails, int mapKey, List<GeneLoci> geneLoci, String chromosome) throws Exception {
        this.variants = variants;
        this.variantDetails = variantDetails;
        this.mapKey = mapKey;
        if(mapKey==38 || mapKey==17)
            setClinicalSignificance();
        if(mapKey==60 || mapKey==70 || mapKey==360 || mapKey==17 || mapKey==38)
            setConservationScore();
        this.geneLoci = geneLoci;
        this.chromosome=chromosome;
    }

    @Override
    public void run() {

        try {
            sortVariants();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    public void sortVariants() throws Exception {

        for (VariantIndex variant : variants) {
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


    }
    public void setConservationScore() throws Exception {
        VariantSearchBean vsb=new VariantSearchBean(mapKey);
        String csTable=vsb.getConScoreTable();
        if(csTable!=null && !csTable.equals("")) {
            this.conservationScores = variantDAO.getConservationScoresByPositionsList(mapKey,chromosome , new ArrayList<>( variants.stream().map(VariantIndex::getStartPos).collect(Collectors.toSet())));
        }
    }
    public void setClinicalSignificance() throws Exception {
        this.clinicalSignificance=variantInfoDAO.getClinicalSignificance(new ArrayList<>(variants.stream().map(VariantIndex::getVariant_id).collect(Collectors.toSet())));
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
        variant.setGeneSymbol(String.join(", ", geneSymbols));
        variant.setGeneRgdId((new ArrayList<>(geneRgdIds)).get(0));
        variant.setStrand(String.join(",", strand));
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
}
