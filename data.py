import numpy
import pandas
from google.cloud import bigquery

client = bigquery.Client()

def getData():
    query = client.run_sync_query("""
    SELECT
      a_user_key,
      a_acpid,
      a_title,
      url
    FROM
      traffic.events
    WHERE
      _PARTITIONTIME=TIMESTAMP('2017-02-17')
      AND a_acpid IS NOT NULL
      AND a_title IS NOT NULL
      AND a_hidden=FALSE
      AND a_user_key IS NOT NULL
    ;""")

    query.use_legacy_sql = False
    print("Running query")
    query.run()

    data = []
    pageToken = None

    while True:
        print("Fetching page")
        rows, totalRows, pageToken = query.fetch_data(page_token=pageToken)
        for row in rows:
            data.append(list(row))
        if not pageToken:
            break
    data = numpy.array(rows)
    df = pandas.DataFrame(data=data, columns=["userId", "articleId", "articleTitle", "url"])
    return df

def test():
    a = ("2017-02-17 00:29:34", "a43b1769-a341-46ec-b1aa-c330c429bcb7", "5-35-386704", "– Hjertet mitt slo ikke på 20 minutter", "http://mobil.oa.no/nyheter/harestua/hadeland/hjertet-mitt-slo-ikke-pa-20-minutter/s/5-35-386704")
    row = []
    row.append(list(a))
    row.append(list(a))
    data = numpy.array(row)
    df = pandas.DataFrame(index=data[:, 0], data=data[:, 1:], columns=["userId", "articleId", "articleTitle", "url"])
    print(df)

if __name__ == "__main__":
    df = getData()
    print("printing to hdf5")
    df.to_hdf("read_20170217.h5", key="read")
    print("Complete!")
    # test()
