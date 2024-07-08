# Generating the Munich population

The following sections describe how to generate a synthetic population for
Munich using the pipeline. First all necessary data must be gathered.
Afterwards, the pipeline can be run to create a synthetic population in *CSV*
and *GPKG* format. These outputs can be used for analysis, or serve as input
to a MATSim simulation.

This guide will cover the following steps:

- [Gathering the data](#section-data)
- [Running the pipeline](#section-population)
- [Running a simulation](#section-simulation)

## <a name="section-data"></a>Gathering the data

To create the scenario, a couple of data sources must be collected. It is best
to start with an empty folder, e.g. `/data`. All data sets need to be named
in a specific way and put into specific sub-directories. The following paragraphs
describe this process.

### 1) German administrative boundaries

- [Administrative boundary data](https://gdz.bkg.bund.de/index.php/default/digitale-geodaten/verwaltungsgebiete/verwaltungsgebiete-1-250-000-mit-einwohnerzahlen-stand-31-12-vg250-ew-31-12.html)
- Go to "Direktdownload"
- Download the version for the *UTM32s* projection and in *geopackage* format
- Put the downloaded *zip* file into `/data/germany`

### 2) Bavarian population data (municipality, sex, age group)

- [Population data](https://www.statistik.bayern.de/statistik/gebiet_bevoelkerung/bevoelkerungsstand/)
- Search for **A1310C** and click on the box
- Download the data for 2022 (*202200*) in *XLS* format
- Put the resulting *xla* file into `/data/bavaria`

### 3) Bavarian employment data (district, sex, age group)

- [Employment data](https://www.statistikdaten.bayern.de/genesis/online?operation=statistic&levelindex=1&levelid=1720112584563&code=13111#abreadcrumb)
- Search for **13111-004r** and click on the link
- For **ERW032**, select *Wohnort* (last word in the dropdown
- Click *Werteabruf*
- In the following view, click *XLSX* on top to download the file
- Put the resulting *xlsx* file into `/data/bavaria`

### 4) Bavarian employment data (municipality, total)

- [Employment data](https://www.statistik.bayern.de/statistik/gebiet_bevoelkerung/erwerbstaetigkeit/index.html)
- Search for **a6502c** and click on the box
- Download the data for 2022 (*202200*) in *XLS* format
- Put the resulting *xla* file into `/data/bavaria`

### 5) German driving license ownership data (municipality; Germany sex, age, type; Bundesland, sex, type)

- [License ownership data](https://www.kba.de/DE/Statistik/Kraftfahrer/Fahrerlaubnisse/Fahrerlaubnisbestand/fahrerlaubnisbestand_node.html)
- Download the *XLSX* file at the bottom of the page
- Put the resulting file into `/data/germany`


### 6) Bavarian building registry (Oberbayern)

- [Building registry](https://geodaten.bayern.de/opengeodata/OpenDataDetail.html?pn=hausumringe)
- Click on **Karte aktivieren** to activate the map
- Click on the region around Munich
- In the opened window (for "Oberbayern") click **Download**
- Put the resulting *zip* file into `/data/bavaria`

### 7) German GTFS

This data set is only needed if you run a MATSim simulation or enable mode choice in the population synthesis.

- [GTFS data](https://gtfs.de/de/feeds/de_full/)
- Click on **Download**
- Put the resulting *zip* file into `/data/gtfs_idf`

### 8) OpenStreetMap (Oberbayern)

This data set is only needed if you run a MATSim simulation or enable mode choice in the population synthesis.

- [Geofabrik Bayern](http://download.geofabrik.de/europe/germany/bayern.html)
- Download the data set for **Oberbayern** in *osm.pbf* format
- Put the resulting *osm.pbf* file into `/data/osm_idf`

### 9) French National household travel survey (ENTD 2008)

The national household travel survey is available from the Ministry of Ecology:

- [National household travel survey](https://www.statistiques.developpement-durable.gouv.fr/enquete-nationale-transports-et-deplacements-entd-2008)
- Scroll all the way down the website to the **Table des donnés** (a clickable
pop-down menu).
- You can either download all the available *csv* files in the list, but only
a few are actually relevant for the pipeline. Those are:
  - Données socio-démographiques des ménages (Q_tcm_menage_0.csv)
  - Données socio-démographiques des individus (Q_tcm_individu.csv)
  - Logement, stationnement, véhicules à disposition des ménages (Q_menage.csv)
  - Données trajets domicile-travail, domicile-étude, accidents (Q_individu.csv)
  - Données mobilité contrainte, trajets vers lieu de travail (Q_ind_lieu_teg.csv)
  - Données mobilité déplacements locaux (K_deploc.csv)
- Put the downloaded *csv* files in to the folder `data/entd_2008`.

### Overview

Your folder structure should now have at least the following files:

- `data/bavaria/091_Oberbayern_Hausumringe.zip`
- `data/bavaria/13111-004r.xlsx`
- `data/bavaria/a1310c_202200.xla`
- `data/bavaria/a6502c_202200.xla`
- `data/entd_2008/Q_individu.csv`
- `data/entd_2008/Q_tcm_individu.csv`
- `data/entd_2008/Q_menage.csv`
- `data/entd_2008/Q_tcm_menage_0.csv`
- `data/entd_2008/K_deploc.csv`
- `data/entd_2008/Q_ind_lieu_teg.csv`
- `data/germany/fe4_2024.xlsx`
- `data/germany/vg250-ew_12-31.utm32s.gpkg.ebenen.zip`
- `data/gtfs_idf/latest.zip`
- `data/osm_idf/oberbayern-latest.osm.pbf`

## <a name="section-population">Running the pipeline

The pipeline code is available in [this repository](https://github.com/eqasim-org/ile-de-france).
To use the code, you have to clone the repository with `git`:

```bash
git clone https://github.com/eqasim-org/ile-de-france pipeline
```

which will create the `pipeline` folder containing the pipeline code. To
set up all dependencies, especially the [synpp](https://github.com/eqasim-org/synpp) package,
which is the code of the pipeline code, we recommend setting up a Python
environment using [Anaconda](https://www.anaconda.com/):

```bash
cd pipeline
conda env create -f environment.yml -n munich
```

This will create a new Anaconda environment with the name `munich`.

To activate the environment, run:

```bash
conda activate munich
```

Now have a look at `config_munich.yml` which is the configuration of the pipeline code.
Have a look at [synpp](https://github.com/eqasim-org/synpp) in case you want to get a more general
understanding of what it does. For the moment, it is important to adjust
two configuration values inside of `config_munich.yml`:

- `working_directory`: This should be an *existing* (ideally empty) folder where
the pipeline will put temporary and cached files during runtime.
- `data_path`: This should be the path to the folder where you were collecting
and arranging all the raw data sets as described above.
- `output_path`: This should be the path to the folder where the output data
of the pipeline should be stored. It must exist and should ideally be empty
for now.

To set up the working/output directory, create, for instance, a `cache` and a
`output` directory. These are already configured in `config.yml`:

```bash
mkdir cache
mkdir output
```

Everything is set now to run the pipeline. The way `config_munich.yml` is configured
it will create the relevant output files in the `output` folder.

To run the pipeline, call the [synpp](https://github.com/eqasim-org/synpp) runner:

```bash
python3 -m synpp config_munich.yml
```

It will read `config_munich.yml`, process all the pipeline code
and eventually create the synthetic population. You should see a couple of
stages running one after another. Most notably, first, the pipeline will read all
the raw data sets to filter them and put them into the correct internal formats.

After running, you should be able to see a couple of files in the `output`
folder:

- `meta.json` contains some meta data, e.g. with which random seed or sampling
rate the population was created and when.
- `persons.csv` and `households.csv` contain all persons and households in the
population with their respective sociodemographic attributes.
- `activities.csv` and `trips.csv` contain all activities and trips in the
daily mobility patterns of these people including attributes on the purposes
of activities.
- `activities.gpkg` and `trips.gpkg` represent the same trips and
activities, but in the spatial *GPKG* format. Activities contain point
geometries to indicate where they happen and the trips file contains line
geometries to indicate origin and destination of each trip.

## <a name="section-simulation">Running the simulation

The pipeline can be used to generate a full runnable [MATSim](https://matsim.org/)
scenario and run it for a couple of iterations to test it. For that, you need
to make sure that the following tools are installed on your system (you can just
try to run the pipeline, it will complain if this is not the case):

- **Java** needs to be installed, with a minimum version of Java 11. In case
you are not sure, you can download the open [AdoptJDK](https://adoptopenjdk.net/). *Attention:* There are incompatibilities with more recent version (for instance 17), so for the time being we recommend using version 17.
- **Maven** `>= 3.8.7` needs to be installed to build the necessary Java packages for setting
up the scenario (such as pt2matsim) and running the simulation. Maven can be
downloaded [here](https://maven.apache.org/) if it does not already exist on
your system.
- **Osmosis** needs to be accessible from the command line to convert and filter
to convert, filter and merge OSM data sets. Alternatively, you can set the path
to the binary using the `osmosis_binary` option in the confiuration file. Osmosis
can be downloaded [here](https://wiki.openstreetmap.org/wiki/Osmosis).
- **git** `=> 2.39.2` is used to clone the repositories containing the simulation code. In
case you clone the pipeline repository previously, you should be all set. However, Windows has problems with working with the long path names that result from the pipelien structure of the project. To avoid the problem, you very likely should set git into *long path mode* by calling `git config --system core.longpaths true`.
- In recent versions of **Ubuntu** you may need to install the `font-config` package to avoid crashes of MATSim when writing images (`sudo apt install fontconfig`).

Then, open your `config.yml` and uncomment the `matsim.output` stage in the
`run` section. If you call `python3 -m synpp` again, the pipeline will know
already which stages have been running before, so it will only run additional
stages that are needed to set up and test the simulation.

After running, you should find the MATSim scenario files in the `output`
folder:

- `munich_population.xml.gz` containing the agents and their daily plans.
- `munich_facilities.xml.gz` containing all businesses, services, etc.
- `munich_network.xml.gz` containing the road and transit network
- `munich_households.xml.gz` containing additional household information
- `munich_transit_schedule.xml.gz` and `munich_transit_vehicles.xml.gz` containing public transport data
- `munich_config.xml` containing the MATSim configuration values
- `munich-1.0.6.jar` containing a fully packaged version of the simulation code including MATSim and all other dependencies

If you want to run the simulation again (in the pipeline it is only run for
two iterations to test that everything works), you can now call the following:

```bash
java -Xmx14G -cp munich-1.0.6.jar org.eqasim.ile_de_france.RunSimulation --config-path munich_config.xml
```

This will create a `simulation_output` folder (as defined in the `munich_config.xml`)
where all simulation is written.

For more flexibility and advanced simulations, have a look at the MATSim
simulation code provided at https://github.com/eqasim-org/eqasim-java. The generated
`munich-*.jar` from this pipeline is an automatically compiled version of
this code.
