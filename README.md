# Exam project

Big Data course (81932), University of Bologna.

This repository must be used as a template to create and deliver the exam project. It contains:

- The basic structure for MapReduce and Spark jobs.
- Guidelines for complex MaprReduce jobs.
- The template for the report.

Instruction to run the jobs:

- MapReduce:
- hadoop jar BDE-mr-chiana.jar AvgViewsPerCategory /user/lchiana/exam/dataset/BR_youtube_trending_data.csv /user/lchiana/exam/dataset/CA_youtube_trending_data.csv /user/lchiana/exam/dataset/DE_youtube_trending_data.csv /user/lchiana/exam/dataset/FR_youtube_trending_data.csv /user/lchiana/exam/dataset/KR_youtube_trending_data.csv /user/lchiana/exam/dataset/MX_youtube_trending_data.csv /user/lchiana/exam/dataset/US_youtube_trending_data.csv /user/lchiana/exam/dataset/Categoy_id_flat.json /user/lchiana/exam/outputs/mapreduce/output1 /user/lchiana/exam/outputs/mapreduce/output2 /user/lchiana/exam/outputs/mapreduce/output3
- Spark:
- spark-submit --class Main BDE-spark-chiana.jar /user/lchiana/exam/dataset /user/lchiana/exam/dataset/Categoy_id_flat.json /user/lchiana/exam/outputs/spark/output