## Deliverables

### Code of the application.
 
- Source code path: ./src/main/scala/com/mediapath/oa/Solution.scala
- Compiled jar: mediamath-oa_2.11-0.1.jar
- Input path: ./input/
- Output path: ./output/
  
 
### Documentation for launching a development environment and running the application.

#### Make sure below software is installed.

- Java Runtime Environment 8 (1.8.0_144 or up)
- Scala 2.11.8 (No scala 2.12)
- Spark 2.2.1
- sbt 0.13.16

#### To run the program locally with client deploy mode.

```$xslt
$ spark-submit --master local[*] --class com.mediamath.oa.Solution --deploy-mode client ./mediamath-oa_2.11-0.1.jar
```

#### Once program finished, output files should be generated in ./output/ folder. To view them run

```$xslt
$ cat output/count_of_events.csv/part-00*
2,visit,64
1,purchase,60
..

$ cat output/count_of_unique_users.csv/part-00*
2,visit,10
1,purchase,10
...

```

##### To run the tests
```$xslt
$ sbt test
[info] Loading global plugins from /Users/dishao/.sbt/0.13/plugins
...
...
[info] Run completed in 333 milliseconds.
[info] Total number of tests run: 3
[info] Suites: completed 1, aborted 0
[info] Tests: succeeded 3, failed 0, canceled 0, ignored 0, pending 0
[info] All tests passed.
[success] Total time: 7 s, completed Feb 2, 2018 3:53:02 PM
```

#### To build from source code

```$xslt
$ sbt package
```
