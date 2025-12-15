import sys
from pyspark import SparkContext, SparkConf

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: count_words_in_line <input_folder>", file=sys.stderr)
        sys.exit(-1)

    conf = SparkConf().setAppName("max-words-in-line")
    sc = SparkContext(conf=conf)

    text_file = sc.textFile("s3a://" + sys.argv[1])

    line_word_counts = text_file.map(
        lambda line: (line, len(line.split()))
    )

    max_line = line_word_counts.reduce(
        lambda a, b: a if a[1] >= b[1] else b
    )

    print("--------------------------------------------")
    print("Line with most words:")
    print(max_line[0])
    print("Word count:", max_line[1])
    print("--------------------------------------------")
