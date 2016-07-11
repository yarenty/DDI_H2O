# DDI on H2O project

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




