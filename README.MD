#Building project

After cloning project, go to it's root and do
```sbt clean compile package```
which will create jar at 
```target/scala-2.12/playground_2.12-0.1.jar```

#Running project

It can be done from root via `script.sh` <br />
```./script.sh "yyyy-MM-dd" "tableName" "keyword"```
i.e.
```./script.sh "2020-09-08" "endBitcoin" "end"```

or you can switch to `target/scala-2.12/` and run <br />
```./spark-submit playground_2.12-0.1.jar "yyyy-MM-dd" "tableName" "keyword"``` <br />
i.e. <br />
```./spark-submit playground_2.12-0.1.jar "2020-09-08" "endBitcoin" "end"``` <br />

Only difference is that if you use script it will notify you of success or failure.

#Notes

Since API that we are using pertains data for only last 7 or so days, it's better to use latest dates when running script.