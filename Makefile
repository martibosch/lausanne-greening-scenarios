.PHONY: reclassify scenarios scenario_metrics swiss_dem station_measurements \
	lst ref_et tair_ucm 

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

## variables
CRS = EPSG:2056
### code
DOWNLOAD_S3_PY := $(CODE_DIR)/download_s3.py
UTILS_PY := $(CODE_DIR)/utils.py


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
# Scenarios

## 1. Generate scenarios
### variables
SCENARIO_DA_NC := $(DATA_INTERIM_DIR)/scenario-da.nc
#### code
CODE_SCENARIOS_DIR := $(CODE_DIR)/scenarios
MAKE_SCENARIO_DA_PY := $(CODE_SCENARIOS_DIR)/make_scenario_da.py

### rules
$(SCENARIO_DA_NC): $(RECLASSIF_LULC_TIF) $(RECLASSIF_TABLE_CSV) \
	$(MAKE_SCENARIO_DA_PY)
	python $(MAKE_SCENARIO_DA_PY) $(RECLASSIF_LULC_TIF) \
		$(RECLASSIF_TABLE_CSV) $@
scenarios: $(SCENARIO_DA_NC)

## 2. Compute landscape metrics of each scenario
### variables
SCENARIO_METRICS_CSV := $(DATA_INTERIM_DIR)/scenario-metrics.csv
#### code
MAKE_SCENARIO_METRICS_PY := $(CODE_SCENARIOS_DIR)/make_scenario_metrics.py

### rules
$(SCENARIO_METRICS_CSV): $(SCENARIO_DA_NC) $(RECLASSIF_TABLE_CSV) \
	$(MAKE_SCENARIO_METRICS_PY)
	python $(MAKE_SCENARIO_METRICS_PY) $(SCENARIO_DA_NC) \
		$(RECLASSIF_TABLE_CSV) $@
scenario_metrics: $(SCENARIO_METRICS_CSV)


#################################################################################
# DEM

DHM200_DIR := $(DATA_RAW_DIR)/dhm200
DHM200_URI = \
	https://data.geo.admin.ch/ch.swisstopo.digitales-hoehenmodell_25/data.zip
DHM200_ASC := $(DHM200_DIR)/DHM200.asc
SWISS_DEM_TIF := $(DATA_INTERIM_DIR)/swiss-dem.tif

### rules
$(DHM200_DIR): | $(DATA_RAW_DIR)
	mkdir $@
$(DHM200_DIR)/%.zip: | $(DHM200_DIR)
	wget $(DHM200_URI) -O $@
$(DHM200_DIR)/%.asc: $(DHM200_DIR)/%.zip
	unzip -j $< 'data/DHM200*' -d $(DHM200_DIR)
	touch $@
#### reproject ASCII grid. See https://bit.ly/2WEBxoL
TEMP_VRT := $(DATA_INTERIM_DIR)/temp.vrt
$(SWISS_DEM_TIF): $(DHM200_ASC)
	gdalwarp -s_srs EPSG:21781 -t_srs $(CRS) -of vrt $< $(TEMP_VRT)
	gdal_translate -of GTiff $(TEMP_VRT) $@
	rm $(TEMP_VRT)
swiss_dem: $(SWISS_DEM_TIF)


#################################################################################
# STATIONS

### variables
STATION_RAW_DIR := $(DATA_RAW_DIR)/stations
LANDSAT_TILES_CSV := $(DATA_RAW_DIR)/landsat-tiles.csv
STATION_RAW_FILENAMES = station-locations.csv agrometeo-tre200s0.csv \
	meteoswiss-lausanne-tre000s0.zip meteoswiss-lausanne-tre200s0.zip \
	WSLLAF.txt VaudAir_EnvoiTemp20180101-20200128_EPFL_20200129.xlsx
STATION_RAW_FILEPATHS := $(addprefix $(STATION_RAW_DIR)/, \
	$(STATION_RAW_FILENAMES))
STATION_LOCATIONS_CSV := $(STATION_RAW_DIR)/station-locations.csv
STATION_TAIR_CSV := $(DATA_INTERIM_DIR)/station-tair.csv
#### code
MAKE_STATION_TAIR_DF_PY := $(CODE_DIR)/make_station_tair_df.py

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

$(STATION_TAIR_CSV): $(LANDSAT_TILES_CSV) $(STATION_RAW_FILEPATHS) \
	$(MAKE_STATION_TAIR_DF_PY) | $(DATA_INTERIM_DIR)
	python $(MAKE_STATION_TAIR_DF_PY) $(LANDSAT_TILES_CSV) \
		$(STATION_RAW_DIR) $@
station_measurements: $(STATION_TAIR_CSV)


#################################################################################
# LST

MAKE_LST_PY := $(CODE_DIR)/make_lst.py
LST_NC := $(DATA_PROCESSED_DIR)/lst-da.nc
$(LST_NC): $(LANDSAT_TILES_CSV) $(AGGLOM_EXTENT_SHP) $(MAKE_LST_PY)
	python $(MAKE_LST_PY) $(LANDSAT_TILES_CSV) $(AGGLOM_EXTENT_SHP) $@
lst: $(LST_NC)

#################################################################################
# InVEST

## 0. Some code that we need for all the experiments
### variables
DATA_INVEST_DIR := $(DATA_INTERIM_DIR)/invest
REF_ET_NC := $(DATA_INVEST_DIR)/ref-et.nc
#### code
CODE_INVEST_DIR := $(CODE_DIR)/invest
MAKE_REF_ET_PY := $(CODE_INVEST_DIR)/make_ref_et.py

### rules
$(DATA_INVEST_DIR): | $(DATA_INTERIM_DIR)
	mkdir $@
$(REF_ET_NC): $(AGGLOM_LULC_TIF) $(AGGLOM_EXTENT_SHP) $(STATION_TAIR_CSV) \
	$(MAKE_REF_ET_PY) | $(DATA_INVEST_DIR)
	python $(MAKE_REF_ET_PY) $(AGGLOM_LULC_TIF) $(AGGLOM_EXTENT_SHP) \
		$(STATION_TAIR_CSV) $@
ref_et: $(REF_ET_NC)

## 1. Calibrate the model
### variables
CALIBRATED_PARAMS_JSON := $(DATA_INVEST_DIR)/calibrated-params.json
#### code
MAKE_CALIBRATE_UCM_PY := $(CODE_INVEST_DIR)/make_calibrate_ucm.py

### rules
$(CALIBRATED_PARAMS_JSON): $(RECLASSIF_LULC_TIF) $(RECLASSIF_TABLE_CSV) \
	$(REF_ET_NC) $(STATION_LOCATIONS_CSV) $(STATION_TAIR_CSV) \
	$(MAKE_CALIBRATE_UCM_PY)
	python $(MAKE_CALIBRATE_UCM_PY) $(RECLASSIF_LULC_TIF) \
		$(RECLASSIF_TABLE_CSV) $(REF_ET_NC) $(STATION_LOCATIONS_CSV) \
		$(STATION_TAIR_CSV) $@
calibrate_ucm: $(CALIBRATED_PARAMS_JSON)

# ## 2. Predict an air temperature raster
# ### variables
# TAIR_RASTER_TIF := $(DATA_PROCESSED_DIR)/tair-raster.tif
# #### code
# MAKE_TAIR_RASTER_PY := $(CODE_INVEST_DIR)/make_tair_raster.py

# ### rules
# $(TAIR_RASTER_TIF): $(RECLASSIF_LULC_TIF) $(RECLASSIF_TABLE_CSV) \
# 	$(AGGLOM_EXTENT_SHP) $(REF_ET_TIF) $(STATION_TAIR_CSV) \
# 	$(CALIBRATED_PARAMS_JSON) $(MAKE_TAIR_RASTER_PY)
# 	python $(MAKE_TAIR_RASTER_PY) $(RECLASSIF_LULC_TIF) \
# 		$(RECLASSIF_TABLE_CSV) $(AGGLOM_EXTENT_SHP) $(REF_ET_NC) \
# 		$(STATION_TAIR_CSV) $(CALIBRATED_PARAMS_JSON) $@
# tair_raster: $(TAIR_RASTER_TIF)
## 2. Predict an air temperature data-array
### variables
TAIR_UCM_NC := $(DATA_PROCESSED_DIR)/tair-ucm.nc
#### code
MAKE_TAIR_UCM_PY := $(CODE_INVEST_DIR)/make_tair_ucm.py

### rules
$(TAIR_UCM_NC): $(CALIBRATED_PARAMS_JSON) $(AGGLOM_EXTENT_SHP) \
	$(RECLASSIF_LULC_TIF) $(RECLASSIF_TABLE_CSV) $(REF_ET_NC) \
	$(STATION_TAIR_CSV) $(STATION_LOCATIONS_CSV) $(MAKE_TAIR_UCM_PY)
	python $(MAKE_TAIR_UCM_PY) $(CALIBRATED_PARAMS_JSON) \
		$(AGGLOM_EXTENT_SHP) $(RECLASSIF_LULC_TIF) \
		$(RECLASSIF_TABLE_CSV) $(REF_ET_NC) $(STATION_TAIR_CSV) \
		$(STATION_LOCATIONS_CSV) $@
tair_ucm: $(TAIR_UCM_NC)


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
