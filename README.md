# Machine Learning with Spark - Second Edition
This is the code repository for [Machine Learning with Spark - Second Edition](https://www.packtpub.com/big-data-and-business-intelligence/machine-learning-spark-second-edition?utm_source=github&utm_medium=repository&utm_campaign=9781785889936), published by [Packt](https://www.packtpub.com/?utm_source=github). It contains all the supporting project files necessary to work through the book from start to finish.
## About the Book
This book will teach you about popular machine learning algorithms and their  implementation. You will learn how various machine learning concepts are implemented in the context of Spark ML. You will start by installing Spark in a single and multinode cluster. Next you'll see how to execute Scala and Python based programs for Spark ML. Then we will take a few datasets and go deeper into clustering, classification, and regression. Toward the end, we will also cover text processing using Spark ML.
## Instructions and Navigation
All of the code is organized into folders. Each folder starts with a number followed by the application name. For example, Chapter02.

Chapter 03 does not contain code files.

The code will look like the following:
```
val conf = new SparkConf()
.setAppName("Test Spark App")
.setMaster("local[4]")
val sc = new SparkContext(conf)
```

Throughout this book, we assume that you have some basic experience with programming in Scala or Python and have some basic knowledge of machine learning, statistics, and data analysis.

## Related Products
* [Mastering Machine Learning with R - Second Edition](https://www.packtpub.com/big-data-and-business-intelligence/mastering-machine-learning-r-second-edition?utm_source=github&utm_medium=repository&utm_campaign=9781787287471)

* [Building Machine Learning Systems with Python - Second Edition](https://www.packtpub.com/big-data-and-business-intelligence/building-machine-learning-systems-python-second-edition?utm_source=github&utm_medium=repository&utm_campaign=9781784392772)

* [Fast Data Processing with Spark - Second Edition](https://www.packtpub.com/big-data-and-business-intelligence/fast-data-processing-spark-second-edition?utm_source=github&utm_medium=repository&utm_campaign=9781784392574)

### Suggestions and Feedback
[Click here](https://docs.google.com/forms/d/e/1FAIpQLSe5qwunkGf6PUvzPirPDtuy1Du5Rlzew23UBp2S-P3wB-GcwQ/viewform) if you have any feedback or suggestions.
