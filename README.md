## Deliverables

### Code of the application.
 
Source code path: ./src/main/scala/com/mediapath/oa/Solution.scala
Compiled jar: mediamath-oa_2.11-0.1.jar
Input path: ./input/
Output path: ./output/
 
### Test suite for the application.
 
 
### Documentation for launching a development environment and running the application.

- Make sure below software is installed.

- - Java Runtime Environment 8 (1.8.0_144 or up)
- - Scala 2.11.8 (No scala 2.12)
- - Spark 2.2.1

- To run the program locally with client deploy mode.

```$xslt
$ spark-submit --master local[*] --class com.mediamath.oa.Solution --deploy-mode client ./mediamath-oa_2.11-0.1.jar
```

- Once program finished, output files should be generated in ./output/ folder. To view them run

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


- To build from source code (Optional)

Make sure sbt 0.13.16 is installed

```$xslt
$ sbt package
```
