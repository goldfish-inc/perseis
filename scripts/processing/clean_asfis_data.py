import csv
import re
import os

RAW_ROOT = os.environ.get("EBISU_RAW_ROOT", "/import")
PROCESSED_ROOT = os.environ.get("EBISU_PROCESSED_ROOT", RAW_ROOT)
REFERENCE_OUT = os.path.join(PROCESSED_ROOT, "reference")
LOG_ROOT = os.environ.get("EBISU_LOG_ROOT", os.path.join(PROCESSED_ROOT, "logs"))
os.makedirs(REFERENCE_OUT, exist_ok=True)
os.makedirs(LOG_ROOT, exist_ok=True)

def clean_asfis_data(input_file=None, output_file=None):
    
    """ASFIS Edge Case Preprocessing - Step 1 of ASFIS pipeline"""
    
    # Use default values if not provided
    if input_file is None:
        input_file = os.path.join(RAW_ROOT, "ASFIS_sp_2025.csv")
    if output_file is None:
        output_file = os.path.join(REFERENCE_OUT, "ASFIS_sp_2025_preprocessed.csv")

    output_dir = os.path.dirname(output_file)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)
    
    # Define the edge case mappings
    edge_cases = {
        "Siluriformes (=Siluroidei)": {"speciesScientificNames[0]": "Siluridae", "currentRank": "Family", "speciesScientificNames[1]": ""},
        "Salmoniformes (=Salmonoidei)": {"speciesScientificNames[0]": "Salmoniformes", "currentRank": "Order", "speciesScientificNames[1]": ""},
        "Clupeiformes (=Clupeoidei)": {"speciesScientificNames[0]": "Clupeiformes", "currentRank": "Order", "speciesScientificNames[1]": ""},
        "Microdesminae (=Microdesmidae)": {"speciesScientificNames[0]": "Microdesminae", "currentRank": "Subfamily", "speciesScientificNames[1]": ""},
        "Percoidei (Perciformes)": {"speciesScientificNames[0]": "Percoidei", "currentRank": "Suborder", "speciesScientificNames[1]": ""},
        "Labridae (ex Scaridae)": {"speciesScientificNames[0]": "Labridae", "currentRank": "Family", "speciesScientificNames[1]": ""},
        "Plectorhinchus pica (formerly P. picus)": {"speciesScientificNames[0]": "Plectorhinchus picus", "currentRank": "Species", "speciesScientificNames[1]": ""},
        "Lutjanidae (ex Caesionidae)": {"speciesScientificNames[0]": "Lutjanidae", "currentRank": "Family", "speciesScientificNames[1]": ""},
        "Sparidae (ex Centracanthidae)": {"speciesScientificNames[0]": "Sparidae", "currentRank": "Family", "speciesScientificNames[1]": ""},
        "Cantherhines (=Navodon) spp": {"speciesScientificNames[0]": "Cantherhines", "currentRank": "Genus", "speciesScientificNames[1]": ""},
        "Harpagiferidae (=Artedidraconidae)": {"speciesScientificNames[0]": "Harpagiferidae", "currentRank": "Family", "speciesScientificNames[1]": ""},
        "Scorpaenoidei (Perciformes)": {"speciesScientificNames[0]": "Scorpaenoidei", "currentRank": "Suborder", "speciesScientificNames[1]": ""},
        "Scombroidei (Scombriformes)": {"speciesScientificNames[0]": "Scombroidei", "currentRank": "Suborder", "speciesScientificNames[1]": ""},
        "Selachii or Selachimorpha (Pleurotremata)": {"speciesScientificNames[0]": "Euselachii", "currentRank": "Superorder", "speciesScientificNames[1]": ""},
        "Batoidea or Batoidimorpha (Hypotremata)": {"speciesScientificNames[0]": "Rajiformes", "currentRank": "Order", "speciesScientificNames[1]": ""},
        "Cambarellus (Cambarellus) patzcuarensis": {"speciesScientificNames[0]": "Cambarellus patzcuarensis", "currentRank": "Species", "speciesScientificNames[1]": ""},
        "Acartia (Acartiura) clausi": {"speciesScientificNames[0]": "Acartia clausi", "currentRank": "Species", "speciesScientificNames[1]": ""},
        "Acartia (Acartiura) longiremis": {"speciesScientificNames[0]": "Acartia longiremis", "currentRank": "Species", "speciesScientificNames[1]": ""},
        "DECAPODA (DENDROBRANCHIATA)": {"speciesScientificNames[0]": "Dendrobranchiata", "currentRank": "Suborder", "speciesScientificNames[1]": ""},
        "DECAPODA (PLEOCYEMATA)": {"speciesScientificNames[0]": "Pleocyemata", "currentRank": "Suborder", "speciesScientificNames[1]": ""},
        "Uroteuthis (Uroteuthis) bartschi": {"speciesScientificNames[0]": "Uroteuthis bartschi", "currentRank": "Species", "speciesScientificNames[1]": ""},
        "Uroteuthis (Photololigo) duvaucelii": {"speciesScientificNames[0]": "Uroteuthis duvaucelii", "currentRank": "Species", "speciesScientificNames[1]": ""},
        "Uroteuthis (Photololigo) edulis": {"speciesScientificNames[0]": "Uroteuthis edulis", "currentRank": "Species", "speciesScientificNames[1]": ""},
        "Uroteuthis (Photololigo) sibogae": {"speciesScientificNames[0]": "Uroteuthis sibogae", "currentRank": "Species", "speciesScientificNames[1]": ""},
        "Uroteuthis (Photololigo) singhalensis": {"speciesScientificNames[0]": "Uroteuthis singhalensis", "currentRank": "Species", "speciesScientificNames[1]": ""},
        "Oikopleura (Vexillaria) dioica": {"speciesScientificNames[0]": "Oikopleura dioica", "currentRank": "Species", "speciesScientificNames[1]": ""},
        "Oikopleura (Coecaria) fusiformis": {"speciesScientificNames[0]": "Oikopleura fusiformis", "currentRank": "Species", "speciesScientificNames[1]": ""},
        "Oikopleura (Vexillaria) gorskyi": {"speciesScientificNames[0]": "Oikopleura gorskyi", "currentRank": "Species", "speciesScientificNames[1]": ""},
        "Oikopleura (Vexillaria) labradoriensis": {"speciesScientificNames[0]": "Oikopleura labradoriensis", "currentRank": "Species", "speciesScientificNames[1]": ""},
        "Oikopleura (Coecaria) longicauda": {"speciesScientificNames[0]": "Oikopleura longicauda", "currentRank": "Species", "speciesScientificNames[1]": ""},
        "Oikopleura (Vexillaria) parva": {"speciesScientificNames[0]": "Oikopleura parva", "currentRank": "Species", "speciesScientificNames[1]": ""},
        "Oikopleura (Vexillaria) vanhoeffeni": {"speciesScientificNames[0]": "Oikopleura vanhoeffeni", "currentRank": "Species", "speciesScientificNames[1]": ""},
        "Oikopleura (Vexillaria) villafrancae": {"speciesScientificNames[0]": "Oikopleura villafrancae", "currentRank": "Species", "speciesScientificNames[1]": ""},
        "Leptasterias (Leptasterias) muelleri": {"speciesScientificNames[0]": "Leptasterias muelleri", "currentRank": "Species", "speciesScientificNames[1]": ""},
        "Cheiraster (Luidiaster) hirsutus": {"speciesScientificNames[0]": "Cheiraster hirsutus", "currentRank": "Species", "speciesScientificNames[1]": ""},
        "Porania (Porania) pulvillus": {"speciesScientificNames[0]": "Porania pulvillus", "currentRank": "Species", "speciesScientificNames[1]": ""},
        "Ctenocidaris (Eurocidaris) nutrix": {"speciesScientificNames[0]": "Eurocidaris nutrix", "currentRank": "Species", "speciesScientificNames[1]": ""},
        "Holothuria (Stichothuria) coronopertusa": {"speciesScientificNames[0]": "Holothuria coronopertusa", "currentRank": "Species", "speciesScientificNames[1]": ""},
        "Holothuria (Holothuria) dakarensis": {"speciesScientificNames[0]": "Holothuria dakarensis", "currentRank": "Species", "speciesScientificNames[1]": ""},
        "Holothuria (Holoidema) floridana": {"speciesScientificNames[0]": "Holothuria floridana", "currentRank": "Species", "speciesScientificNames[1]": ""},
        "Holothuria (Penningothuria) forskali": {"speciesScientificNames[0]": "Holothuria forskali", "currentRank": "Species", "speciesScientificNames[1]": ""},
        "Holothuria (Holodeima) grisea": {"speciesScientificNames[0]": "Holothuria grisea", "currentRank": "Species", "speciesScientificNames[1]": ""},
        "Holothuria (Stemperothuria) imitans": {"speciesScientificNames[0]": "Holothuria imitans", "currentRank": "Species", "speciesScientificNames[1]": ""},
        "Holothuria (Cystipus) inabilis": {"speciesScientificNames[0]": "Holothuria inabilis", "currentRank": "Species", "speciesScientificNames[1]": ""},
        "Holothuria (Halodeima) inornata": {"speciesScientificNames[0]": "Holothuria inornata", "currentRank": "Species", "speciesScientificNames[1]": ""},
        "Holothuria (Vaneyothuria) lentiginosa lentiginosa": {"speciesScientificNames[0]": "Holothuria lentiginosa", "currentRank": "Species", "speciesScientificNames[1]": ""},
        "Holothuria (Selenkothuria) lubrica": {"speciesScientificNames[0]": "Holothuria lubrica", "currentRank": "Species", "speciesScientificNames[1]": ""},
        "Holothuria (Holothuria) mammata": {"speciesScientificNames[0]": "Holothuria mammata", "currentRank": "Species", "speciesScientificNames[1]": ""},
        "Holothuria (Theelothuria) paraprinceps": {"speciesScientificNames[0]": "Holothuria paraprinceps", "currentRank": "Species", "speciesScientificNames[1]": ""},
        "Holothuria (Roweothuria) poli": {"speciesScientificNames[0]": "Holothuria poli", "currentRank": "Species", "speciesScientificNames[1]": ""},
        "Holothuria (Selenkothuria) portovallartensis": {"speciesScientificNames[0]": "Holothuria portovallartensis", "currentRank": "Species", "speciesScientificNames[1]": ""},
        "Holothuria (Semperothuria) roseomaculata": {"speciesScientificNames[0]": "Holothuria roseomaculata", "currentRank": "Species", "speciesScientificNames[1]": ""},
        "Holothuria (Platyperona) sanctori": {"speciesScientificNames[0]": "Holothuria sanctori", "currentRank": "Species", "speciesScientificNames[1]": ""},
        "Holothuria (Holothuria) tubulosa": {"speciesScientificNames[0]": "Holothuria tubulosa", "currentRank": "Species", "speciesScientificNames[1]": ""},
        "Alitta virens (formerly Nereis virens)": {"speciesScientificNames[0]": "Neanthes virens", "currentRank": "Species", "speciesScientificNames[1]": ""},
        "Alcyoniidae (Octocorallia)": {"speciesScientificNames[0]": "Alcyoniidae", "currentRank": "Family", "speciesScientificNames[1]": ""},
        "Leptothecata (Leptomedusae)": {"speciesScientificNames[0]": "Leptothecatae", "currentRank": "Order", "speciesScientificNames[1]": ""},
        "Callyspongia (Callyspongia) nuda": {"speciesScientificNames[0]": "Callyspongia nuda", "currentRank": "Species", "speciesScientificNames[1]": ""},
        "Haliclona (Haliclona) oculata": {"speciesScientificNames[0]": "Haliclona oculata", "currentRank": "Species", "speciesScientificNames[1]": ""},
        "Halichondria (Halichondria) bowerbanki": {"speciesScientificNames[0]": "Halichondria bowerbanki", "currentRank": "Species", "speciesScientificNames[1]": ""},
        "Halichondria (Halichondria) panicea": {"speciesScientificNames[0]": "Halichondria panicea", "currentRank": "Species", "speciesScientificNames[1]": ""},
        # Hybrid species edge cases
        "Oreochromis aureus x O. niloticus": {"speciesScientificNames[0]": "Oreochromis aureus", "currentRank": "Species", "speciesScientificNames[1]": "Oreochromis niloticus"},
        "Oreochromis andersonii x O. niloticus": {"speciesScientificNames[0]": "Oreochromis andersonii", "currentRank": "Species", "speciesScientificNames[1]": "Oreochromis niloticus"},
        "Piaractus mesopotamicus x P. brachypomus": {"speciesScientificNames[0]": "Piaractus mesopotamicus", "currentRank": "Species", "speciesScientificNames[1]": "Piaractus brachypomus"},
        "Piaractus mesopotamicus x Colossoma macropomum": {"speciesScientificNames[0]": "Piaractus mesopotamicus", "currentRank": "Species", "speciesScientificNames[1]": "Colossoma macropomum"},
        "Colossoma macropomum x Piaractus brachypomus": {"speciesScientificNames[0]": "Colossoma macropomum", "currentRank": "Species", "speciesScientificNames[1]": "Piaractus brachypomus"},
        "Pseudoplatystoma corruscans x P. reticulatum": {"speciesScientificNames[0]": "Pseudoplatystoma corruscans", "currentRank": "Species", "speciesScientificNames[1]": "Pseudoplatystoma reticulatum"},
        "Leiarius marmoratus x Pseudoplatystoma reticulatum": {"speciesScientificNames[0]": "Leiarius marmoratus", "currentRank": "Species", "speciesScientificNames[1]": "Pseudoplatystoma reticulatum"},
        "Clarias gariepinus x C. macrocephalus": {"speciesScientificNames[0]": "Clarias gariepinus", "currentRank": "Species", "speciesScientificNames[1]": "Clarias macrocephalus"},
        "Heterobranchus longifilis x Clarias gariepinus": {"speciesScientificNames[0]": "Heterobranchus longifilis", "currentRank": "Species", "speciesScientificNames[1]": "Clarias gariepinus"},
        "Ictalurus punctatus x I. furcatus": {"speciesScientificNames[0]": "Ictalurus punctatus", "currentRank": "Species", "speciesScientificNames[1]": "Ictalurus furcatus"},
        "Channa maculata x C. argus": {"speciesScientificNames[0]": "Channa maculata", "currentRank": "Species", "speciesScientificNames[1]": "Channa argus"},
        "Morone chrysops x M. saxatilis": {"speciesScientificNames[0]": "Morone chrysops", "currentRank": "Species", "speciesScientificNames[1]": "Morone saxatilis"},
        "Osteichthyes": {"speciesScientificNames[0]": "Gnathostomata", "currentRank": "Infraphylum", "speciesScientificNames[1]": ""},
        "Osmerus spp, Hypomesus spp": {"speciesScientificNames[0]": "Osmerus", "currentRank": "Genus", "speciesScientificNames[1]": "Hypomesus"},
        "Stolothrissa, Limnothrissa spp": {"speciesScientificNames[0]": "Stolothrissa", "currentRank": "Genus", "speciesScientificNames[1]": "Limnothrissa"},
        "Xiphopenaeus, Trachypenaeus spp": {"speciesScientificNames[0]": "Xiphopenaeus", "currentRank": "Genus", "speciesScientificNames[1]": "Trachypenaeus"},
        # Previous edge cases
        "Alosa alosa, A. fallax": {"speciesScientificNames[0]": "Alosa alosa", "currentRank": "Species", "speciesScientificNames[1]": "Alosa fallax"},
        "Actinopterygii": {"speciesScientificNames[0]": "Actinopterygii", "currentRank": "Superclass", "speciesScientificNames[1]": ""},
        "Pleuronectiformes": {"speciesScientificNames[0]": "Pleuronectiformes", "currentRank": "Order", "speciesScientificNames[1]": ""},
        "Gadiformes": {"speciesScientificNames[0]": "Gadiformes", "currentRank": "Order", "speciesScientificNames[1]": ""},
        "Epinephelus fuscoguttatus x E. lanceolatus": {"speciesScientificNames[0]": "Epinephelus fuscoguttatus", "currentRank": "Species", "speciesScientificNames[1]": "Epinephelus lanceolatus"},
        "Anguilliformes": {"speciesScientificNames[0]": "Anguilliformes", "currentRank": "Order", "speciesScientificNames[1]": ""},
        "Melanostomiinae": {"speciesScientificNames[0]": "Melanostomiinae", "currentRank": "Subfamily", "speciesScientificNames[1]": ""},
        "Perciformes": {"speciesScientificNames[0]": "Perciformes", "currentRank": "Order", "speciesScientificNames[1]": ""},
        "Auxis thazard, A. rochei": {"speciesScientificNames[0]": "Auxis thazard", "currentRank": "Species", "speciesScientificNames[1]": "Auxis rochei"},
        "Thunnini": {"speciesScientificNames[0]": "Thunnini", "currentRank": "Tribe", "speciesScientificNames[1]": ""},
        "Scombrinae": {"speciesScientificNames[0]": "Scombrinae", "currentRank": "Subfamily", "speciesScientificNames[1]": ""},
        "Hexanchiformes": {"speciesScientificNames[0]": "Hexanchiformes", "currentRank": "Order", "speciesScientificNames[1]": ""},
        "Heterodontiformes": {"speciesScientificNames[0]": "Heterodontiformes", "currentRank": "Order", "speciesScientificNames[1]": ""},
        "Orectolobiformes": {"speciesScientificNames[0]": "Orectolobiformes", "currentRank": "Order", "speciesScientificNames[1]": ""},
        "Lamniformes": {"speciesScientificNames[0]": "Lamniformes", "currentRank": "Order", "speciesScientificNames[1]": ""},
        "Carcharhiniformes": {"speciesScientificNames[0]": "Carcharhiniformes", "currentRank": "Order", "speciesScientificNames[1]": ""},
        "Squaliformes": {"speciesScientificNames[0]": "Squaliformes", "currentRank": "Order", "speciesScientificNames[1]": ""},
        "Torpediniformes": {"speciesScientificNames[0]": "Torpediniformes", "currentRank": "Order", "speciesScientificNames[1]": ""},
        "Rajiformes": {"speciesScientificNames[0]": "Rajiformes", "currentRank": "Order", "speciesScientificNames[1]": ""},
        "Chimaeriformes": {"speciesScientificNames[0]": "Chimaeriformes", "currentRank": "Order", "speciesScientificNames[1]": ""},
        # Previous batch of edge cases
        "Elasmobranchii": {"speciesScientificNames[0]": "Elasmobranchii", "currentRank": "Subclass", "speciesScientificNames[1]": ""},
        "Chondrichthyes": {"speciesScientificNames[0]": "Chondrichthyes", "currentRank": "Superclass", "speciesScientificNames[1]": ""},
        "Crustacea": {"speciesScientificNames[0]": "Crustacea", "currentRank": "Subphylum", "speciesScientificNames[1]": ""},
        "Brachyura": {"speciesScientificNames[0]": "Brachyura", "currentRank": "Infraorder", "speciesScientificNames[1]": ""},
        "Reptantia": {"speciesScientificNames[0]": "Pleocyemata", "currentRank": "Suborder", "speciesScientificNames[1]": ""},
        "Anomura": {"speciesScientificNames[0]": "Anomura", "currentRank": "Infraorder", "speciesScientificNames[1]": ""},
        "Natantia": {"speciesScientificNames[0]": "Dendrobranchiata", "currentRank": "Suborder", "speciesScientificNames[1]": ""},
        "Pandalus spp, Pandalopsis spp": {"speciesScientificNames[0]": "Pandalus", "currentRank": "Genus", "speciesScientificNames[1]": "Pandalopsis"},
        "Caridea": {"speciesScientificNames[0]": "Caridea", "currentRank": "Infraorder", "speciesScientificNames[1]": ""},
        "Euphausiacea": {"speciesScientificNames[0]": "Euphausiacea", "currentRank": "Order", "speciesScientificNames[1]": ""},
        "Copepoda": {"speciesScientificNames[0]": "Copepoda", "currentRank": "Class", "speciesScientificNames[1]": ""},
        "Scalpellomorpha": {"speciesScientificNames[0]": "Scalpellomorpha", "currentRank": "Suborder", "speciesScientificNames[1]": ""},
        "Amphipoda": {"speciesScientificNames[0]": "Amphipoda", "currentRank": "Order", "speciesScientificNames[1]": ""},
        "Isopoda": {"speciesScientificNames[0]": "Isopoda", "currentRank": "Order", "speciesScientificNames[1]": ""},
        "Tanaidacea": {"speciesScientificNames[0]": "Tanaidacea", "currentRank": "Order", "speciesScientificNames[1]": ""},
        "Stomatopoda": {"speciesScientificNames[0]": "Stomatopoda", "currentRank": "Order", "speciesScientificNames[1]": ""},
        "Mollusca": {"speciesScientificNames[0]": "Mollusca", "currentRank": "Phylum", "speciesScientificNames[1]": ""},
        "Bivalvia": {"speciesScientificNames[0]": "Bivalvia", "currentRank": "Class", "speciesScientificNames[1]": ""},
        "Nudibranchia": {"speciesScientificNames[0]": "Nudibranchia", "currentRank": "Order", "speciesScientificNames[1]": ""},
        # New edge cases
        "Mysticeti": {"speciesScientificNames[0]": "Mysticeti", "currentRank": "Suborder", "speciesScientificNames[1]": ""},
        "Odontoceti": {"speciesScientificNames[0]": "Odontoceti", "currentRank": "Odontoceti", "speciesScientificNames[1]": ""},
        "Demospongiae": {"speciesScientificNames[0]": "Demospongiae", "currentRank": "Class", "speciesScientificNames[1]": ""},
        "Fucaceae": {"speciesScientificNames[0]": "Fucaceae", "currentRank": "Family", "speciesScientificNames[1]": ""},
        "Laminariaceae": {"speciesScientificNames[0]": "Laminariaceae", "currentRank": "Family", "speciesScientificNames[1]": ""},
        "Phaeophyceae": {"speciesScientificNames[0]": "Phaeophyceae", "currentRank": "Class", "speciesScientificNames[1]": ""},
        "Gigartinaceae": {"speciesScientificNames[0]": "Gigartinaceae", "currentRank": "Family", "speciesScientificNames[1]": ""},
        "Chlorophyceae": {"speciesScientificNames[0]": "Chlorophyceae", "currentRank": "Class", "speciesScientificNames[1]": ""},
        "Cyanophyceae": {"speciesScientificNames[0]": "Cyanophyceae", "currentRank": "Class", "speciesScientificNames[1]": ""},
        "Dinophyceae": {"speciesScientificNames[0]": "Dinophyceae", "currentRank": "Class", "speciesScientificNames[1]": ""},
        "Bacillariophyceae": {"speciesScientificNames[0]": "Bacillariophyceae", "currentRank": "Class", "speciesScientificNames[1]": ""},
        "Angiospermae": {"speciesScientificNames[0]": "Magnoliopsida", "currentRank": "Class", "speciesScientificNames[1]": ""},
        "Algae": {"speciesScientificNames[0]": "Chromista", "currentRank": "Kingdom", "speciesScientificNames[1]": ""},
        "Aves": {"speciesScientificNames[0]": "Aves", "currentRank": "Class", "speciesScientificNames[1]": ""}
    }

    try:
        # Read the input CSV file
        with open(input_file, 'r', newline='', encoding='utf-8') as infile:
            reader = csv.reader(infile)
            headers = next(reader)  # Get the header row

            # Find the index of Scientific_Name and Alpha3_Code columns
            scientific_name_idx = headers.index('Scientific_Name')
            alpha3_code_idx = headers.index('Alpha3_Code')

            # Create new headers with the new columns right after Alpha3_Code
            intermediate_headers = headers[:alpha3_code_idx+1] + ['currentRank', 'speciesScientificNames[0]', 'speciesScientificNames[1]'] + headers[alpha3_code_idx+1:]

            # Read all rows
            all_rows = list(reader)

        # Step 1: Process data with your existing logic
        processed_rows = []
        
        for row in all_rows:
            scientific_name = row[scientific_name_idx]

            # Initialize new column values
            current_rank = ""
            species_scientific_name_0 = ""
            species_scientific_name_1 = ""

            # Your existing processing logic here...
            if scientific_name in edge_cases:
                current_rank = edge_cases[scientific_name]["currentRank"]
                species_scientific_name_0 = edge_cases[scientific_name]["speciesScientificNames[0]"]
                species_scientific_name_1 = edge_cases[scientific_name]["speciesScientificNames[1]"]
            else:
                # Your existing pattern matching logic...
                if ',' in scientific_name and ' spp' not in scientific_name:
                    current_rank = "Species"
                    parts = scientific_name.split(',', 1)
                    species_scientific_name_0 = parts[0].strip()
                    second_part = parts[1].strip()
                    if second_part.startswith('A. ') or second_part.startswith('E. ') or second_part.startswith('O. ') or second_part.startswith('P. ') or second_part.startswith('C. ') or second_part.startswith('I. ') or second_part.startswith('M. '):
                        genus = species_scientific_name_0.split()[0]
                        species = second_part[3:].strip()
                        species_scientific_name_1 = f"{genus} {species}"
                    else:
                        species_scientific_name_1 = second_part
                elif ' x ' in scientific_name:
                    current_rank = "Species"
                    parts = scientific_name.split(' x ')
                    species_scientific_name_0 = parts[0].strip()
                    second_part = parts[1].strip()
                    if second_part.startswith('O. ') or second_part.startswith('P. ') or second_part.startswith('C. ') or second_part.startswith('I. ') or second_part.startswith('M. ') or second_part.startswith('E. '):
                        genus = species_scientific_name_0.split()[0]
                        species = second_part[3:].strip()
                        species_scientific_name_1 = f"{genus} {species}"
                    else:
                        species_scientific_name_1 = second_part
                else:
                    # Your existing word count logic...
                    words = scientific_name.split()
                    word_count = len(words)
                    
                    if word_count == 1 and words[0].lower().endswith('dae'):
                        current_rank = "Family"
                        species_scientific_name_0 = scientific_name
                    elif word_count == 2 and words[1].lower() == 'spp':
                        current_rank = "Genus"
                        species_scientific_name_0 = words[0]
                    elif word_count == 2 and words[1].lower() != 'spp':
                        current_rank = "Species"
                        species_scientific_name_0 = scientific_name
                    elif word_count == 3:
                        current_rank = "Subspecies"
                        species_scientific_name_0 = scientific_name
                    elif word_count == 1 and words[0].lower().endswith('formes'):
                        current_rank = "Order"
                        species_scientific_name_0 = scientific_name
                    elif word_count == 1 and words[0].lower().endswith('ia'):
                        current_rank = "Class"
                        species_scientific_name_0 = scientific_name
                    elif word_count == 1 and words[0].lower().endswith('phyceae'):
                        current_rank = "Class"
                        species_scientific_name_0 = scientific_name
                    elif word_count == 1 and words[0].lower().endswith('a'):
                        current_rank = "Phylum"
                        species_scientific_name_0 = scientific_name
                    elif word_count == 1 and words[0].lower().endswith('nae'):
                        current_rank = "Subfamily"
                        species_scientific_name_0 = scientific_name
                    elif word_count == 1 and words[0].lower().endswith('ini'):
                        current_rank = "Tribe"
                        species_scientific_name_0 = scientific_name
                    elif word_count == 1 and words[0].lower().endswith('a') and current_rank == "":
                        current_rank = "Infraorder"
                        species_scientific_name_0 = scientific_name
                    else:
                        species_scientific_name_0 = scientific_name

            # Create intermediate row with the additional columns
            intermediate_row = row[:alpha3_code_idx+1] + [current_rank, species_scientific_name_0, species_scientific_name_1] + row[alpha3_code_idx+1:]
            processed_rows.append(intermediate_row)

        # Step 2: Duplicate rows that have both speciesScientificNames[0] and speciesScientificNames[1]
        normalized_rows = []
        duplicate_count = 0
        
        for row in processed_rows:
            current_rank = row[alpha3_code_idx + 1]
            species_name_0 = row[alpha3_code_idx + 2]
            species_name_1 = row[alpha3_code_idx + 3]
            
            # Always add the first row (with speciesScientificNames[0])
            first_row = row.copy()
            first_row[alpha3_code_idx + 3] = ""  # Clear speciesScientificNames[1]
            normalized_rows.append(first_row)
            
            # If there's a second species, create a duplicate row
            if species_name_1 and species_name_1.strip():
                second_row = row.copy()
                second_row[alpha3_code_idx + 2] = species_name_1  # Move speciesScientificNames[1] to speciesScientificNames[0]
                second_row[alpha3_code_idx + 3] = ""  # Clear speciesScientificNames[1]
                normalized_rows.append(second_row)
                duplicate_count += 1

        # Step 3: Create final headers (remove Scientific_Name, remove speciesScientificNames[1], rename columns)
        final_headers = []
        for i, header in enumerate(intermediate_headers):
            if header == 'Scientific_Name':
                continue  # Skip original Scientific_Name column
            elif header == 'speciesScientificNames[1]':
                continue  # Skip speciesScientificNames[1] column
            elif header == 'speciesScientificNames[0]':
                final_headers.append('scientificName')  # Rename
            elif header == 'currentRank':
                final_headers.append('taxonRank')  # Rename
            else:
                final_headers.append(header)

        # Step 4: Process final rows (remove corresponding columns)
        final_rows = []
        scientific_name_idx_intermediate = intermediate_headers.index('Scientific_Name')
        species_name_1_idx = intermediate_headers.index('speciesScientificNames[1]')
        
        for row in normalized_rows:
            final_row = []
            for i, value in enumerate(row):
                if i == scientific_name_idx_intermediate:
                    continue  # Skip original Scientific_Name column
                elif i == species_name_1_idx:
                    continue  # Skip speciesScientificNames[1] column
                else:
                    final_row.append(value)
            final_rows.append(final_row)

        # Step 5: Write final output
        with open(output_file, 'w', newline='', encoding='utf-8') as outfile:
            writer = csv.writer(outfile)
            writer.writerow(final_headers)
            writer.writerows(final_rows)

        print(f"✅ ASFIS preprocessing completed: {output_file}")
        print(f"📊 Original rows: {len(all_rows)}")
        print(f"📊 Final rows: {len(final_rows)}")
        print(f"📊 Duplicated rows: {duplicate_count}")
        
        # Log preprocessing statistics
        with open(os.path.join(LOG_ROOT, "asfis_preprocessing_stats.log"), "w") as log:
            log.write(f"Edge cases processed: {len(edge_cases)}\n")
            log.write(f"Original rows: {len(all_rows)}\n")
            log.write(f"Final rows: {len(final_rows)}\n")
            log.write(f"Duplicated rows: {duplicate_count}\n")
            log.write(f"Expansion ratio: {len(final_rows) / len(all_rows):.2f}\n")
            
    except Exception as e:
        print(f"❌ ASFIS preprocessing failed: {e}")
        raise

if __name__ == "__main__":
    clean_asfis_data()  # Now calls without arguments, using defaults
