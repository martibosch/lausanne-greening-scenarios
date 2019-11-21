.PHONY: agglom_lulc

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
CODE_DATA_DIR := $(CODE_DIR)/data
CODE_UTILS_DIR := $(CODE_DIR)/utils
DOWNLOAD_S3_PY := $(CODE_UTILS_DIR)/download_s3.py


#################################################################################
# Get urban extent and rasterize cadastre to 10m resolution

## 1. Download and unzip shapefile

### variables
CADASTRE_DIR := $(DATA_RAW_DIR)/cadastre
CADASTRE_FILE_KEY = cantons/asit-vd/Cadastre_agglomeration.zip
CADASTRE_UNZIP_FILEPATTERN := \
	Cadastre/(NPCS|MOVD)_CAD_TPR_(BATHS|CSBOIS|CSDIV|CSDUR|CSEAU|CSVERT)_S.*
CADASTRE_SHP := $(CADASTRE_DIR)/cadastre.shp

#### code
CADASTRE_SHP_FROM_ZIP_PY := $(CODE_DATA_DIR)/shp_from_zip.py


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
MAKE_AGGLOM_LULC_PY := $(CODE_DATA_DIR)/make_agglom_lulc.py
AGGLOM_LULC_TIF := $(AGGLOM_LULC_DIR)/agglom_lulc.tif

### rules
$(AGGLOM_LULC_DIR): | $(DATA_INTERIM_DIR)
	mkdir $@
$(AGGLOM_LULC_TIF): $(CADASTRE_SHP) $(MAKE_AGGLOM_LULC_PY) | $(AGGLOM_LULC_DIR)
	python $(MAKE_AGGLOM_LULC_PY) $< $@
agglom_lulc: $(AGGLOM_LULC_TIF)


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
