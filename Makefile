.PHONY: agglom_lulc agglom_landsat train_test_split agglom_trees reclassify

#################################################################################
# GLOBALS                                                                       #
#################################################################################

PROJECT_DIR := $(shell dirname $(realpath $(lastword $(MAKEFILE_LIST))))

DATA_DIR = data
DATA_RAW_DIR := $(DATA_DIR)/raw
DATA_INTERIM_DIR := $(DATA_DIR)/interim
DATA_PROCESSED_DIR := $(DATA_DIR)/processed

CODE_DIR = lausanne_heat_islands

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

#################################################################################
# COMMANDS                                                                      #
#################################################################################

#################################################################################
# Utilities to be used in several tasks

## variables
### code
CODE_UTILS_DIR := $(CODE_DIR)/utils
DOWNLOAD_S3_PY := $(CODE_UTILS_DIR)/download_s3.py


#################################################################################
# Get urban extent and rasterize cadastre to 10m resolution

## 1. Download and unzip shapefile

### variables
CADASTRE_DIR := $(DATA_RAW_DIR)/cadastre
CADASTRE_FILE_KEY = cantons/asit-vd/cadastre/Cadastre_agglomeration.zip
CADASTRE_UNZIP_FILEPATTERN := \
	Cadastre/(NPCS|MOVD)_CAD_TPR_(BATHS|CSBOIS|CSDIV|CSDUR|CSEAU|CSVERT)_S.*
CADASTRE_SHP := $(CADASTRE_DIR)/cadastre.shp

#### code
CODE_LULC_DIR := $(CODE_DIR)/lulc
CADASTRE_SHP_FROM_ZIP_PY := $(CODE_LULC_DIR)/shp_from_zip.py


### rules
$(CADASTRE_DIR): | $(DATA_RAW_DIR)
	mkdir $@
$(CADASTRE_DIR)/%.zip: $(DOWNLOAD_S3_PY) | $(CADASTRE_DIR)
	python $(DOWNLOAD_S3_PY) $(CADASTRE_FILE_KEY) $@
$(CADASTRE_DIR)/%.shp: $(CADASTRE_DIR)/%.zip $(CADASTRE_SHP_FROM_ZIP_PY)
	python $(CADASTRE_SHP_FROM_ZIP_PY) $< $@ "$(CADASTRE_UNZIP_FILEPATTERN)"
	touch $@
# cadastre_shp: $(CADASTRE_SHP)

## 2. Get the urban extent and rasterize shp to a 10m resolution

### variables
AGGLOM_LULC_DIR := $(DATA_INTERIM_DIR)/agglom_lulc
MAKE_AGGLOM_LULC_PY := $(CODE_LULC_DIR)/make_agglom_lulc.py
AGGLOM_LULC_TIF := $(AGGLOM_LULC_DIR)/agglom_lulc.tif

### rules
$(AGGLOM_LULC_DIR): | $(DATA_INTERIM_DIR)
	mkdir $@
$(AGGLOM_LULC_TIF): $(CADASTRE_SHP) $(MAKE_AGGLOM_LULC_PY) | $(AGGLOM_LULC_DIR)
	python $(MAKE_AGGLOM_LULC_PY) $< $@
agglom_lulc: $(AGGLOM_LULC_TIF)


#################################################################################
# Crop landsat files to the urban extent and stack them in a multiband raster

## 1. Download and untar the landsat data
### variables
LANDSAT_DIR := $(DATA_RAW_DIR)/landsat
LANDSAT_FILE_KEY = landsat/LE07_L1TP_196028_20120313_20161202_01_T1.tar.gz
AGGLOM_LANDSAT_DIR := $(DATA_INTERIM_DIR)/agglom_landsat
AGGLOM_LANDSAT_TIF := $(AGGLOM_LANDSAT_DIR)/agglom_landsat.tif

#### code
CODE_SPECTRAL_DIR := $(CODE_DIR)/spectral
MAKE_LANDSAT_RASTER_PY := $(CODE_SPECTRAL_DIR)/make_landsat_raster.py

### rules
$(LANDSAT_DIR): | $(DATA_RAW_DIR)
	mkdir $@
$(AGGLOM_LANDSAT_DIR): | $(DATA_INTERIM_DIR)
	mkdir $@
$(LANDSAT_DIR)/%.tar.gz: $(DOWNLOAD_S3_PY) | $(LANDSAT_DIR)
	python $(DOWNLOAD_S3_PY) $(LANDSAT_FILE_KEY) $@
$(AGGLOM_LANDSAT_DIR)/%.tif: $(LANDSAT_DIR)/%.tar.gz $(MAKE_LANDSAT_RASTER_PY) \
	| $(AGGLOM_LANDSAT_DIR)
	python $(MAKE_LANDSAT_RASTER_PY) $< $(AGGLOM_LULC_TIF) $@
agglom_landsat: $(AGGLOM_LANDSAT_TIF)


#################################################################################
# Generate 1m raster of tree/non-tree pixels

## 1. Download SWISSIMAGE

### variables
SWISSIMAGE_DIR := $(DATA_RAW_DIR)/swissimage
SWISSIMAGE_FILE_KEY = swissimage/1m/lausanne/swissimage1m_latest_lausanne_uhi.tif
SWISSIMAGE_TIF := $(SWISSIMAGE_DIR)/swissimage.tif

### rules
$(SWISSIMAGE_DIR): | $(DATA_RAW_DIR)
	mkdir $@
$(SWISSIMAGE_TIF): $(DOWNLOAD_S3_PY) | $(SWISSIMAGE_DIR)
	python $(DOWNLOAD_S3_PY) $(SWISSIMAGE_FILE_KEY) $@

## 2. Split SWISSIMAGE tif into tiles

### variables
SWISSIMAGE_TILES_DIR := $(DATA_INTERIM_DIR)/swissimage_tiles
SWISSIMAGE_TILES_CSV := $(SWISSIMAGE_TILES_DIR)/swissimage_tiles.csv
#### code
CODE_TREES_DIR := $(CODE_DIR)/trees
MAKE_SWISSIMAGE_TILES_PY := $(CODE_TREES_DIR)/make_swissimage_tiles.py

### rules
$(SWISSIMAGE_TILES_DIR): | $(DATA_INTERIM_DIR)
	mkdir $@
$(SWISSIMAGE_TILES_CSV): $(SWISSIMAGE_TIF) $(AGGLOM_LULC_TIF) \
	$(MAKE_SWISSIMAGE_TILES_PY) | $(SWISSIMAGE_TILES_DIR)
	python $(MAKE_SWISSIMAGE_TILES_PY) $< $(AGGLOM_LULC_TIF) \
		$(SWISSIMAGE_TILES_DIR) $@

## 3. Compute the train/test split

### variables
SPLIT_CSV := $(SWISSIMAGE_TILES_DIR)/split.csv
NUM_COMPONENTS = 24
NUM_TILE_CLUSTERS = 4

### rules
$(SPLIT_CSV): $(SWISSIMAGE_TILES_CSV)
	detectree train-test-split --img-dir $(SWISSIMAGE_TILES_DIR) \
		--output-filepath $(SPLIT_CSV) \
		--num-components $(NUM_COMPONENTS) \
		--num-img-clusters $(NUM_TILE_CLUSTERS)
# train_test_split: $(SPLIT_CSV)

## 4. Make the response tiles from LIDAR data

### variables
RESPONSE_TILES_DIR := $(DATA_INTERIM_DIR)/response_tiles
RESPONSE_TILES_CSV := $(RESPONSE_TILES_DIR)/response_tiles.csv
#### code
MAKE_RESPONSE_TILES_PY := $(CODE_TREES_DIR)/make_response_tiles.py

### rules
$(RESPONSE_TILES_DIR): | $(DATA_INTERIM_DIR)
	mkdir $@
$(RESPONSE_TILES_CSV): $(SPLIT_CSV) $(MAKE_RESPONSE_TILES_PY) \
	| $(RESPONSE_TILES_DIR)
	python $(MAKE_RESPONSE_TILES_PY) $(SPLIT_CSV) $(RESPONSE_TILES_DIR) $@
# response_tiles: $(RESPONSE_TILES_CSV)

## 5. Train a classifier for each tile cluster

### variables
MODELS_DIR = models
MODEL_JOBLIB_FILEPATHS := $(foreach CLUSTER_LABEL, \
	$(shell seq 0 $$(($(NUM_TILE_CLUSTERS)-1))), \
	$(MODELS_DIR)/$(CLUSTER_LABEL).joblib)

### rules
$(MODELS_DIR):
	mkdir $@
$(MODELS_DIR)/%.joblib: $(RESPONSE_TILES_CSV) | $(MODELS_DIR)
	detectree train-classifier --split-filepath $(SPLIT_CSV) \
		--response-img-dir $(RESPONSE_TILES_DIR) --img-cluster $* \
		--output-filepath $@
# train_models: $(MODEL_JOBLIB_FILEPATHS)

## 6. Classify the tiles

### variables
CLASSIFIED_TILES_DIR := $(DATA_INTERIM_DIR)/classified_tiles
CLASSIFIED_TILES_CSV_FILEPATHS := $(foreach CLUSTER_LABEL, \
	$(shell seq 0 $$(($(NUM_TILE_CLUSTERS)-1))), \
	$(CLASSIFIED_TILES_DIR)/$(CLUSTER_LABEL).csv)
#### code
PREDICT_TILES_PY := $(CODE_TREES_DIR)/predict_tiles.py

### rules
$(CLASSIFIED_TILES_DIR): | $(DATA_INTERIM_DIR)
	mkdir $@
$(CLASSIFIED_TILES_DIR)/%.csv: $(MODELS_DIR)/%.joblib $(PREDICT_TILES_PY) \
	| $(CLASSIFIED_TILES_DIR)
	python $(PREDICT_TILES_PY) $(SPLIT_CSV) $< $(CLASSIFIED_TILES_DIR) $@ \
		--img-cluster $(notdir $(basename $@))
# classify_tiles: $(CLASSIFIED_TILES_CSV_FILEPATHS)

## 7. Mosaic the classified and response tiles into a single file

### variables
AGGLOM_TREES_DIR := $(DATA_INTERIM_DIR)/agglom_trees
AGGLOM_TREES_TIF := $(AGGLOM_TREES_DIR)/agglom_trees.tif
TEMP_AGGLOM_TREES_TIF := $(AGGLOM_TREES_DIR)/foo.tif
TREE_NODATA = 0  # shouldn't be ugly hardcoded like that...

### rules
$(AGGLOM_TREES_DIR): | $(DATA_INTERIM_DIR)
	mkdir $@
$(AGGLOM_TREES_TIF): $(RESPONSE_TILES_CSV) $(CLASSIFIED_TILES_CSV_FILEPATHS) \
	| $(AGGLOM_TREES_DIR)
	gdal_merge.py -o $(TEMP_AGGLOM_TREES_TIF) -n $(TREE_NODATA) \
		$(wildcard $(CLASSIFIED_TILES_DIR)/*.tif) \
		$(wildcard $(RESPONSE_TILES_DIR)/*.tif)
	gdalwarp -t_srs EPSG:2056 $(TEMP_AGGLOM_TREES_TIF) $@
	rm $(TEMP_AGGLOM_TREES_TIF)
agglom_trees: $(AGGLOM_TREES_TIF)


#################################################################################
# Reclassify LULC codes according to tree and building cover distribution

### variables
BIOPHYSICAL_TABLE_FILE_KEY := other/biophysical_table.csv
BIOPHYSICAL_TABLE_CSV := $(DATA_RAW_DIR)/biophysical_table.csv
DATA_RECLASSIF_DIR := $(DATA_INTERIM_DIR)/reclassif
TREE_COVER_TIF := $(DATA_RECLASSIF_DIR)/tree_cover.tif
BUILDING_COVER_TIF := $(DATA_RECLASSIF_DIR)/building_cover.tif
RECLASSIF_TABLE_CSV := $(DATA_PROCESSED_DIR)/reclassif_table.csv
RECLASSIF_LULC_TIF := $(DATA_PROCESSED_DIR)/reclassif_extract.tif

#### code
CODE_RECLASSIFY_DIR := $(CODE_DIR)/reclassify
GET_PIXEL_TREE_COVER_PY := $(CODE_RECLASSIFY_DIR)/get_pixel_tree_cover.py
GET_PIXEL_BUILDING_COVER_PY := $(CODE_RECLASSIFY_DIR)/get_pixel_building_cover.py
RECLASSIFY_PY := $(CODE_RECLASSIFY_DIR)/reclassify.py

### rules
$(DATA_RECLASSIF_DIR): | $(DATA_INTERIM_DIR)
	mkdir $@
$(BIOPHYSICAL_TABLE_CSV): $(DOWNLOAD_S3_PY) | $(DATA_RAW_DIR)
	python $(DOWNLOAD_S3_PY) $(BIOPHYSICAL_TABLE_FILE_KEY) $@
$(TREE_COVER_TIF): $(AGGLOM_LULC_TIF) $(AGGLOM_TREES_TIF) \
	$(GET_PIXEL_TREE_COVER_PY) | $(DATA_RECLASSIF_DIR)
	python $(GET_PIXEL_TREE_COVER_PY) $(AGGLOM_LULC_TIF) $(AGGLOM_TREES_TIF) \
		$@
$(BUILDING_COVER_TIF): $(AGGLOM_LULC_TIF) $(CADASTRE_SHP) \
	$(GET_PIXEL_BUILDING_COVER_PY) | $(DATA_RECLASSIF_DIR)
	python $(GET_PIXEL_BUILDING_COVER_PY) $(AGGLOM_LULC_TIF) \
		$(CADASTRE_SHP) $@
$(RECLASSIF_LULC_TIF) $(RECLASSIF_TABLE_CSV): $(TREE_COVER_TIF) \
	$(BUILDING_COVER_TIF) $(BIOPHYSICAL_TABLE_CSV) $(RECLASSIFY_PY)
	python $(RECLASSIFY_PY) $(AGGLOM_LULC_TIF) $(TREE_COVER_TIF) \
		$(BUILDING_COVER_TIF) $(BIOPHYSICAL_TABLE_CSV) \
		$(RECLASSIF_LULC_TIF) $(RECLASSIF_TABLE_CSV)
#### Rule with multiple targets https://bit.ly/35B8YdU
$(RECLASSIF_TABLE_CSV): $(RECLASSIF_LULC_TIF)
reclassify: $(RECLASSIF_LULC_TIF) $(RECLASSIF_TABLE_CSV)


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
