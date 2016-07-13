# DiDi Algorithm Competition on H2O

Lets play.

## Dependencies
Sparkling Water 1.6.5 which integrates:
  - Spark 1.6.1
  - H2O 3.0 Shannon
  - Data from DiDi competition: http://research.xiaojukeji.com/competition/detail.action?competitionId=DiTech2016
  - directiories in /opt/data/season_1/:
      - /out  - here will be your ourput
      - /outdata  - here raw tables
      - /outdatanorm - here normalized ones
      - /outtraffic - all traffic predictions
      - /outweather - weather predictions

For more details see [build.gradle](build.gradle).

## Status

- Step 1: Data preparation
- Step 2: Data munging - raw
    - weather
    - traffic
    - POI
- Step 3: Data munging - filling
    - weather - based on previous data
    - traffic - use DRF predictions
    - POI - merge (too big for 16G RAM)
- Step 4: Machine Learning
    - GBM - done/ quite bad output
    - DRF ! - very good - current best 2.39
    - DL - partially - need more investigation  (2.88 at small test)
- Presentation mode: get 2 train days - predict 1 test day

## TODO
- set proper DRF traffic seeds - to have repeatable output!
- check DL with different configs (exp: Puppy Brain)
- 2 phase predictions: first on "demand" then on "gap"
- move to bigger server and use full POI
- build models for each district - then no need for POI (same for traffics)



## Project structure
 
```
├─ gradle/        - Gradle definition files
├─ src/           - Source code
│  ├─ main/       - Main implementation code 
│  │  ├─ scala/
│  ├─ test/       - Test code
│  │  ├─ scala/
├─ build.gradle   - Build file for this project
├─ gradlew        - Gradle wrapper 
```



## Project building

For building, please, use provided `gradlew` command:
```
./gradlew build
```

### Run
For running an application (DataMunging at the moment):
```
./gradlew run
```

## Running tests

To run tests, please, run:
```
./gradlew test
```

# Checking code style

To check codestyle:
```
./gradlew scalaStyle
```

## Creating and Running Spark Application

Create application assembly which can be directly submitted to Spark cluster:
```
./gradlew shadowJar
```
The command creates jar file `build/libs/ddi.jar` containing all necessary classes to run application on top of Spark cluster.

Submit application to Spark cluster (in this case, local cluster is used):
```
export MASTER='local-cluster[3,2,1024]'
$SPARK_HOME/bin/spark-submit --class com.yarenty.ddi.MLProcessor build/libs/ddi.jar
```




