from pyspark.sql.window import Window
from pyspark.sql.functions import row_number


class Deduplicator:

    def remove_duplicates(self, df, key_col):

        window = Window.partitionBy(key_col).orderBy(key_col)

        return (
            df.withColumn("rn", row_number().over(window))
              .filter("rn = 1")
              .drop("rn")
        )