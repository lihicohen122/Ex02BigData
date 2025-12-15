import sys
from pyspark import SparkContext, SparkConf

def clean_word(word):
    word = word.rstrip(".,")
    return word

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: wordcount <s3_path>", file=sys.stderr)
        sys.exit(-1)

    conf = SparkConf().setAppName("find-longest-word")
    sc = SparkContext(conf=conf)

    text_file = sc.textFile("s3a://" + sys.argv[1])

    rdd1 = text_file.flatMap(lambda line: line.split(" "))
    cleaned = rdd1.map(lambda word: clean_word(word)).filter(lambda w: w.isalpha())

    # reduce to find the longest word
    longest_word = cleaned.reduce(lambda a, b: a if len(a) > len(b) else b)

    print("--------------------------------------------")
    print(f"The longest word is: {longest_word} (length {len(longest_word)})")
    print("--------------------------------------------")
