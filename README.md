# Zion

### Tips
It is recommended (but not crucial) to work in a virtual environment in order to not mess up your global package directory:
```
pyvenv virtual
source virtual/bin/activate
```
## Usage

### Installation

```
brew install python3
brew install apache-spark
export PYSPARK_PYTHON=python3
pip install -r requirements.txt
```

I would also recommend to lower logging level from INFO to WARN:
```
# In <apache-spark dir>/libexec/conf
cp log4j.properties.template log4j.properties
vi log4j.properties
```
Change "INFO" in `log4j.rootCategory=INFO, console` to "WARN"

### Running

```
# Run the application
spark-submit main.py
```

## Datasets

### Movielens
Download the dataset here: http://grouplens.org/datasets/movielens/

Each line in the ratings dataset (ratings.csv) is formatted as: `userId,movieId,rating,timestamp`

Each line in the movies (movies.csv) dataset is formatted as: `movieId,title,genres`,
were genres has the format: `Genre1|Genre2|Genre3...`

The tags file (tags.csv) has the format: `userId,movieId,tag,timestamp`

And finally, the links.csv file has the format: `movieId,imdbId,tmdbId`
