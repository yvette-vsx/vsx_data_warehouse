from pyspark.sql.types import StructType, StructField, StringType, IntegerType

common: list[StructField] = [
    StructField("sys_ts", IntegerType(), False),
    StructField("distinct_id", StringType(), True),
    StructField("insert_id", StringType(), True),
]

schemas = {
    "lessonend": StructType(
        [
            StructField("class_id", StringType(), True),
            StructField("client", StringType(), True),
            StructField("lesson_id", StringType(), True),
            StructField("platform", StringType(), True),
            StructField("user_id", StringType(), True),
            StructField("release_version", StringType()),
        ]
        + common
    ),
}
