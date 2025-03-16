# Running the simulation

The pipeline can be used to generate a full runnable [MATSim](https://matsim.org/)
scenario and run it for a couple of iterations to test it. For that, you need
to make sure that the following tools are installed on your system (you can just
try to run the pipeline, it will complain if this is not the case):

- **Java** needs to be installed, with a minimum version of Java 17. In case
you are not sure, you can download the open [AdoptJDK](https://adoptopenjdk.net/).
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

Then, open your `config_bavaria.yml` and uncomment the `matsim.output` stage in the
`run` section. If you call `python3 -m synpp` again, the pipeline will know
already which stages have been running before, so it will only run additional
stages that are needed to set up and test the simulation.

After running, you should find the MATSim scenario files in the `output`
folder:

- `bavaria_population.xml.gz` containing the agents and their daily plans.
- `bavaria_facilities.xml.gz` containing all businesses, services, etc.
- `bavaria_network.xml.gz` containing the road and transit network
- `bavaria_households.xml.gz` containing additional household information
- `bavaria_transit_schedule.xml.gz` and `bavaria_transit_vehicles.xml.gz` containing public transport data
- `bavaria_config.xml` containing the MATSim configuration values
- `bavaria_run.jar` containing a fully packaged version of the simulation code including MATSim and all other dependencies

If you want to run the simulation again (in the pipeline it is only run for
two iterations to test that everything works), you can now call the following:

```bash
java -Xmx14G -cp bavaria_run.jar org.eqasim.bavaria.RunSimulation --config-path bavaria_config.xml
```

This will create a `simulation_output` folder (as defined in the `bavaria_config.xml`)
where all simulation is written.

For more flexibility and advanced simulations, have a look at the MATSim
simulation code provided at https://github.com/eqasim-org/eqasim-java/tree/bavaria-main. The generated
`bavaria-*.jar` from this pipeline is an automatically compiled version of
this code.
