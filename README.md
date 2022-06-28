[![GitHub license](https://img.shields.io/github/license/martibosch/lausanne-greening-scenarios.svg)](https://github.com/martibosch/lausanne-greening-scenarios/blob/master/LICENSE)
[![DOI](https://zenodo.org//badge/DOI/10.1101/2020.11.09.373779.svg)](https://zenodo.org/record/4316572)

# Lausanne greening scenarios

Spatially-explicit simulation of urban heat mitigation by increasing the tree canopy cover in Lausanne

**Citation**: Bosch, M., Locatelli, M., Hamel, P., Jaligot, R., Chenal, J., and Joost, S. 2020. "Evaluating urban greening scenarios for urban heat mitigation: a spatially-explicit approach". *Royal Society Open Science 8(12): 202174*. [10.1098/rsos.202174](https://doi.org/10.1098/rsos.202174)

![LULC change maps](reports/figures/scenario-lulc-maps.png)
![Temperature maps](reports/figures/scenario-T-maps.png)
![Heat mitigation maps](reports/figures/scenario-heat-mitigation-maps.png)

## Instructions to reproduce

The computational workflow to reproduce the results makes use of [a Makefile](https://github.com/martibosch/lausanne-greening-scenarios/blob/master/Makefile) which orchestrates the execution of all the steps to transform the raw data into tables and figures<sup>[1](#note-1)</sup>. To reproduce the computational workflow in your computer, you can follow the steps below:

1. Clone the repository and change the working directory to the repository's root:

```bash
git clone https://github.com/martibosch/lausanne-greening-scenarios
cd lausanne-greening-scenarios
```

2. Create the environment (this requires conda) and activate it:

```bash
conda env create -f environment.yml
# the above command creates a conda environment named `lausanne-greening-scenarios`
conda activate lausanne-greening-scenarios
```
 
3. Register the IPython kernel of the `lausanne-greening-scenarios` environment:

```bash
python -m ipykernel install --user --name lausanne-greening-scenarios --display-name \
    "Python (lausanne-greening-scenarios)"
```

4. You can use `make` to download the data data required to reproduce the results (which is available [at a dedicated Zenodo repository](https://zenodo.org/record/4316572)) as in:

```bash
make download_zenodo_data
```

5. Finally, you can launch a Jupyter Notebook server and generate the tables and figures interactively by executing the notebooks of the `notebooks` directory. The first cell of each notebook features a call to a target of the Makefile, which will download and process all the data required to execute the subsequent cells. The following notebooks are provided:

    * [Scenario evaluation](https://github.com/martibosch/lausanne-greening-scenarios/blob/master/notebooks/scenarios.ipynb)
    * [Scenario metrics](https://github.com/martibosch/lausanne-greening-scenarios/blob/master/notebooks/scenario-metrics.ipynb)
    * [Human exposure](https://github.com/martibosch/lausanne-greening-scenarios/blob/master/notebooks/human-exposure.ipynb)

## Notes

1. <a name="note-1"></a> Many of the datasets used here are open and therefore all the processing steps can be reproduced by anyone. However, some other datasets are proprietary and thus cannot be shared openly. In the latter case, in order to allow the maximum reproducibility of our results, the following interim files are provided:

    * [`station-t.csv`](https://zenodo.org/record/4316572/files/station-t.csv?download=1): temperature measurements at the monitoring stations for the reference date (i.e., 27/07/2018)
    * [`ref-et.tif`](https://zenodo.org/record/4316572/files/ref-et.tif?download=1): reference evapotranspiration raster for the reference date (i.e., 27/07/2018)
    * [`bldg-cover.tif`](https://zenodo.org/record/4314832/files/bldg-cover.tif?download=1): raster with the percentage of building cover in each pixel of the Lausanne agglomeration 

    The sources for the first two files are detailed [at the Zenodo repository for this paper](https://zenodo.org/record/4316572), whereas the source of `bldg-cover.tif` is detailed at [10.5281/zenodo.4314832](https://doi.org/10.5281/zenodo.4314832). If you use these files, their sources must be properly acknowledged.

## See also

* [Lausanne heat islands](https://github.com/martibosch/lausanne-heat-islands)
* [Lausanne agglomeration extent](https://github.com/martibosch/lausanne-agglom-extent)
* [Lausanne tree canopy](https://github.com/martibosch/lausanne-tree-canopy)
* [InVEST urban cooling model calibration](https://github.com/martibosch/invest-ucm-calibration)
* [Swiss urban heat islands utils](https://github.com/martibosch/swiss-uhi-utils)

## Acknowledgments

* With the support of the École Polytechnique Fédérale de Lausanne (EPFL)
* Project based on the [cookiecutter data science project template](https://drivendata.github.io/cookiecutter-data-science). #cookiecutterdatascience
