import sys
import pandas

def readText(name):
    return pandas.read_table(name)

def removeUnused(df):
    if "Rating" in df.columns:
        del df["Rating"]
    return df

def rewriteHeader(df):
    df.columns = ["aid", "acpid", "article"]
    return df

def writeCsv(name, df):
    df.to_csv(name, sep="|", index=False)


def createOutname(inname):
    parts = inname.split(".")
    parts[-1] = "csv"
    return ".".join(parts)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Need an offering")
        sys.exit(0)
    infile = sys.argv[1]
    outfile = createOutname(infile)
    writeCsv(outfile, rewriteHeader(removeUnused(readText(infile))))
