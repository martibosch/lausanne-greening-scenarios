.PHONY: reclassify station_measurements ref_et calibrate_ucm tair_ucm scenarios \
	scenario_metrics statpop

#################################################################################
# GLOBALS                                                                       #
#################################################################################

## variables
DATA_DIR = data
DATA_RAW_DIR := $(DATA_DIR)/raw
DATA_INTERIM_DIR := $(DATA_DIR)/interim
DATA_PROCESSED_DIR := $(DATA_DIR)/processed

MODELS_DIR = models

CODE_DIR = lausanne_greening_scenarios

NOTEBOOKS_DIR = notebooks

## rules
define MAKE_DATA_SUB_DIR
$(DATA_SUB_DIR): | $(DATA_DIR)
	mkdir $$@
endef
$(DATA_DIR):
	mkdir $@
$(foreach DATA_SUB_DIR, \
	$(DATA_RAW_DIR) $(DATA_INTERIM_DIR) $(DATA_PROCESSED_DIR), \
	$(eval $(MAKE_DATA_SUB_DIR)))
$(MODELS_DIR):
	mkdir $@


#################################################################################
# COMMANDS                                                                      #
#################################################################################

#################################################################################
# Utilities to be used in several tasks

### code
DOWNLOAD_S3_PY := $(CODE_DIR)/download_s3.py


#################################################################################
# LULC

## 1. Download the data
### variables
AGGLOM_EXTENT_DIR := $(DATA_RAW_DIR)/agglom-extent
AGGLOM_EXTENT_FILE_KEY = urban-footprinter/lausanne-agglom/agglom-extent.zip
AGGLOM_EXTENT_SHP := $(AGGLOM_EXTENT_DIR)/agglom-extent.shp
AGGLOM_LULC_FILE_KEY = urban-footprinter/lausanne-agglom/agglom-lulc.tif
AGGLOM_LULC_TIF := $(DATA_RAW_DIR)/agglom-lulc.tif
TREE_CANOPY_FILE_KEY = detectree/lausanne-agglom/tree-canopy.tif
TREE_CANOPY_TIF := $(DATA_RAW_DIR)/tree-canopy.tif
CADASTRE_DIR := $(DATA_RAW_DIR)/cadastre
CADASTRE_FILE_KEY = cantons/vaud/cadastre/Cadastre_agglomeration.zip
CADASTRE_UNZIP_FILEPATTERN := \
	Cadastre/(NPCS|MOVD)_CAD_TPR_(BATHS|CSBOIS|CSDIV|CSDUR|CSEAU|CSVERT)_S.*
CADASTRE_SHP := $(CADASTRE_DIR)/cadastre.shp
#### code
MAKE_CADASTRE_SHP_FROM_ZIP_PY := $(CODE_DIR)/make_cadastre_shp_from_zip.py

### rules
$(AGGLOM_EXTENT_DIR): | $(DATA_RAW_DIR)
	mkdir $@
$(AGGLOM_EXTENT_DIR)/%.zip: | $(AGGLOM_EXTENT_DIR)
	python $(DOWNLOAD_S3_PY) $(AGGLOM_EXTENT_FILE_KEY) $@
$(AGGLOM_EXTENT_DIR)/%.shp: $(AGGLOM_EXTENT_DIR)/%.zip
	unzip $< -d $(AGGLOM_EXTENT_DIR)
	touch $@
$(AGGLOM_LULC_TIF): | $(DATA_RAW_DIR)
	python $(DOWNLOAD_S3_PY) $(AGGLOM_LULC_FILE_KEY) $@
$(TREE_CANOPY_TIF): | $(DATA_RAW_DIR)
	python $(DOWNLOAD_S3_PY) $(TREE_CANOPY_FILE_KEY) $@
$(CADASTRE_DIR): | $(DATA_RAW_DIR)
	mkdir $@
$(CADASTRE_DIR)/%.zip: | $(CADASTRE_DIR)
	python $(DOWNLOAD_S3_PY) $(CADASTRE_FILE_KEY) $@
$(CADASTRE_DIR)/%.shp: $(CADASTRE_DIR)/%.zip $(MAKE_CADASTRE_SHP_FROM_ZIP_PY)
	python $(MAKE_CADASTRE_SHP_FROM_ZIP_PY) $< $@ \
		"$(CADASTRE_UNZIP_FILEPATTERN)"
	touch $@ 

## 2. Reclassify according to tree cover
### variables
# BIOPHYSICAL_TABLE_FILE_KEY := other/biophysical-table.csv
BIOPHYSICAL_TABLE_CSV := $(DATA_RAW_DIR)/biophysical-table.csv
DATA_RECLASSIF_DIR := $(DATA_INTERIM_DIR)/reclassif
TREE_COVER_TIF := $(DATA_RECLASSIF_DIR)/tree-cover.tif
BLDG_COVER_TIF := $(DATA_RECLASSIF_DIR)/bldg-cover.tif
RECLASSIF_TABLE_CSV := $(DATA_PROCESSED_DIR)/biophysical-table.csv
RECLASSIF_LULC_TIF := $(DATA_PROCESSED_DIR)/agglom-lulc.tif
#### code
CODE_RECLASSIFY_DIR := $(CODE_DIR)/reclassify
MAKE_PIXEL_TREE_COVER_PY := $(CODE_RECLASSIFY_DIR)/make_pixel_tree_cover.py
MAKE_PIXEL_BLDG_COVER_PY := $(CODE_RECLASSIFY_DIR)/make_pixel_bldg_cover.py
MAKE_RECLASSIFY_PY := $(CODE_RECLASSIFY_DIR)/make_reclassify.py

### rules
$(DATA_RECLASSIF_DIR): | $(DATA_INTERIM_DIR)
	mkdir $@
$(TREE_COVER_TIF): $(AGGLOM_LULC_TIF) $(TREE_CANOPY_TIF) \
	$(MAKE_PIXEL_TREE_COVER_PY) | $(DATA_RECLASSIF_DIR)
	python $(MAKE_PIXEL_TREE_COVER_PY) $(AGGLOM_LULC_TIF) \
		$(TREE_CANOPY_TIF) $@
$(BLDG_COVER_TIF): $(AGGLOM_LULC_TIF) $(CADASTRE_SHP) \
	$(MAKE_PIXEL_BLDG_COVER_PY) | $(DATA_RECLASSIF_DIR)
	python $(MAKE_PIXEL_BLDG_COVER_PY) $(AGGLOM_LULC_TIF) \
		$(CADASTRE_SHP) $@
$(RECLASSIF_LULC_TIF) $(RECLASSIF_TABLE_CSV): $(TREE_COVER_TIF) \
	$(BLDG_COVER_TIF) $(BIOPHYSICAL_TABLE_CSV) $(MAKE_RECLASSIFY_PY) \
	| $(DATA_PROCESSED_DIR)
	python $(MAKE_RECLASSIFY_PY) $(AGGLOM_LULC_TIF) $(TREE_COVER_TIF) \
		$(BLDG_COVER_TIF) $(BIOPHYSICAL_TABLE_CSV) \
		$(RECLASSIF_LULC_TIF) $(RECLASSIF_TABLE_CSV)
#### Rule with multiple targets https://bit.ly/35B8YdU
$(RECLASSIF_TABLE_CSV): $(RECLASSIF_LULC_TIF)
reclassify: $(RECLASSIF_LULC_TIF) $(RECLASSIF_TABLE_CSV)


#################################################################################
# STATIONS

### variables
STATION_RAW_DIR := $(DATA_RAW_DIR)/stations
STATION_RAW_FILENAMES = station-locations.csv agrometeo-tre200s0.csv \
	meteoswiss-lausanne-tre000s0.zip meteoswiss-lausanne-tre200s0.zip \
	WSLLAF.txt VaudAir_EnvoiTemp20180101-20200128_EPFL_20200129.xlsx
STATION_RAW_FILEPATHS := $(addprefix $(STATION_RAW_DIR)/, \
	$(STATION_RAW_FILENAMES))
STATION_LOCATIONS_CSV := $(STATION_RAW_DIR)/station-locations.csv
STATION_T_CSV := $(DATA_INTERIM_DIR)/station-t.csv
#### code
MAKE_STATION_T_DF_PY := $(CODE_DIR)/make_station_tair_df.py

### rules
$(STATION_RAW_DIR): | $(DATA_RAW_DIR)
	mkdir $@
define DOWNLOAD_STATION_DATA
$(STATION_RAW_DIR)/$(STATION_RAW_FILENAME): | $(STATION_RAW_DIR)
	python $(DOWNLOAD_S3_PY) \
		cantons/vaud/air-temperature/$(STATION_RAW_FILENAME) $$@
endef
$(foreach STATION_RAW_FILENAME, $(STATION_RAW_FILENAMES), \
	$(eval $(DOWNLOAD_STATION_DATA)))

$(STATION_T_CSV): $(STATION_RAW_FILEPATHS) \
	$(MAKE_STATION_T_DF_PY) | $(DATA_INTERIM_DIR)
	python $(MAKE_STATION_T_DF_PY) $(STATION_RAW_DIR) $@
station_measurements: $(STATION_T_CSV)


#################################################################################
# Scenarios

## 0. Preprocess the inputs required to simulate scenario temperatures with the
##    InVEST urban cooling model
### variables
CALIBRATED_PARAMS_JSON := $(DATA_RAW_DIR)/invest-calibrated-params.json
REF_ET_TIF := $(DATA_PROCESSED_DIR)/ref-et.tif
#### code
CODE_SCENARIOS_DIR := $(CODE_DIR)/scenarios
MAKE_REF_ET_PY := $(CODE_SCENARIOS_DIR)/make_ref_et.py

### rules
$(REF_ET_TIF): $(AGGLOM_LULC_TIF) $(AGGLOM_EXTENT_SHP) $(STATION_T_CSV) \
	$(MAKE_REF_ET_PY) | $(DATA_PROCESSED_DIR)
	python $(MAKE_REF_ET_PY) $(AGGLOM_LULC_TIF) $(AGGLOM_EXTENT_SHP) \
		$(STATION_T_CSV) $@
ref_et: $(REF_ET_TIF)

## 1. Generate scenario datasets
### variables
SCENARIO_DS_NC := $(DATA_PROCESSED_DIR)/scenarios.nc
#### code
MAKE_SCENARIO_DS_PY := $(CODE_SCENARIOS_DIR)/make_scenario_ds.py

### rules
$(SCENARIO_DS_NC): $(RECLASSIF_LULC_TIF) $(RECLASSIF_TABLE_CSV) \
	$(STATION_T_CSV) $(REF_ET_TIF) $(MAKE_SCENARIO_DS_PY)
	python $(MAKE_SCENARIO_DS_PY) $(RECLASSIF_LULC_TIF) \
		$(RECLASSIF_TABLE_CSV) $(STATION_T_CSV) $(REF_ET_TIF) \
		$(CALIBRATED_PARAMS_JSON) $@
scenarios: $(SCENARIO_DS_NC)

## 2. Compute landscape metrics of each scenario
### variables
SCENARIO_METRICS_CSV := $(DATA_PROCESSED_DIR)/scenario-metrics.csv
#### code
MAKE_SCENARIO_METRICS_PY := $(CODE_SCENARIOS_DIR)/make_scenario_metrics.py

### rules
$(SCENARIO_METRICS_CSV): $(SCENARIO_DS_NC) $(RECLASSIF_TABLE_CSV) \
	$(MAKE_SCENARIO_METRICS_PY)
	python $(MAKE_SCENARIO_METRICS_PY) $(SCENARIO_DS_NC) \
		$(RECLASSIF_TABLE_CSV) $@
scenario_metrics: $(SCENARIO_METRICS_CSV)


#################################################################################
# STATPOP

### variables
#### Statpop 2018: https://www.bfs.admin.ch/bfsstatic/dam/assets/9947069/master
STATPOP_URI = https://www.bfs.admin.ch/bfsstatic/dam/assets/14027479/master
STATPOP_DIR := $(DATA_RAW_DIR)/statpop
STATPOP_CSV := $(STATPOP_DIR)/statpop-2019.csv
#### code

### rules
$(STATPOP_DIR): | $(DATA_RAW_DIR)
	mkdir $@
$(STATPOP_DIR)/%.zip: | $(STATPOP_DIR)
	wget $(STATPOP_URI) -O $@
$(STATPOP_DIR)/%.csv: $(STATPOP_DIR)/%.zip
	unzip -j $< 'STATPOP2019.csv' -d $(STATPOP_DIR)
	mv $(STATPOP_DIR)/STATPOP2019.csv $(STATPOP_CSV)
	touch $@
statpop: $(STATPOP_CSV)


#################################################################################
# Self Documenting Commands                                                     #
#################################################################################

.DEFAULT_GOAL := help

# Inspired by <http://marmelab.com/blog/2016/02/29/auto-documented-makefile.html>
# sed script explained:
# /^##/:
# 	* save line in hold space
# 	* purge line
# 	* Loop:
# 		* append newline + line to hold space
# 		* go to next line
# 		* if line starts with doc comment, strip comment character off and loop
# 	* remove target prerequisites
# 	* append hold space (+ newline) to line
# 	* replace newline plus comments by `---`
# 	* print line
# Separate expressions are necessary because labels cannot be delimited by
# semicolon; see <http://stackoverflow.com/a/11799865/1968>
.PHONY: help
help:
	@echo "$$(tput bold)Available rules:$$(tput sgr0)"
	@echo
	@sed -n -e "/^## / { \
		h; \
		s/.*//; \
		:doc" \
		-e "H; \
		n; \
		s/^## //; \
		t doc" \
		-e "s/:.*//; \
		G; \
		s/\\n## /---/; \
		s/\\n/ /g; \
		p; \
	}" ${MAKEFILE_LIST} \
	| LC_ALL='C' sort --ignore-case \
	| awk -F '---' \
		-v ncol=$$(tput cols) \
		-v indent=19 \
		-v col_on="$$(tput setaf 6)" \
		-v col_off="$$(tput sgr0)" \
	'{ \
		printf "%s%*s%s ", col_on, -indent, $$1, col_off; \
		n = split($$2, words, " "); \
		line_length = ncol - indent; \
		for (i = 1; i <= n; i++) { \
			line_length -= length(words[i]) + 1; \
			if (line_length <= 0) { \
				line_length = ncol - indent - length(words[i]) - 1; \
				printf "\n%*s ", -indent, " "; \
			} \
			printf "%s ", words[i]; \
		} \
		printf "\n"; \
	}' \
	| more $(shell test $(shell uname) = Darwin && echo '--no-init --raw-control-chars')
