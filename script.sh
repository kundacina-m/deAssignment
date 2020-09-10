#!/bin/bash
spark-submit target/scala-2.12/playground_2.12-0.1.jar $1 $2 $3
if [ $? -eq 0 ]
then
	echo "Success"
else
	echo "Failed"
fi
