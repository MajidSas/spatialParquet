# Spatial Parquet

This contains the code associated with the paper:
> Majid Saeedan and Ahmed Eldawy. 2022. Spatial Parquet: A Column File Format for Geospatial Data Lakes. In The 30th International Conference on Advances in Geographic Information Systems (SIGSPATIAL ’22), November 1–4, 2022, Seattle, WA, USA. ACM, New York, NY, USA, 4 pages. [Link](https://dl.acm.org/doi/10.1145/3557915.3561038)

## How to Use
To build, run the following on the root directory:

```shell script
mvn clean install
``` 

> edu.ucr.cs.bdlab.spatialParquet
This package contains the reader/writer implementation of Spatial Parquet.
Check the `Main` class for how they are used.

> edu.ucr.cs.bdlab.geoParquet
This package contains a reader/writer implementation of the GeoParquet structure.

To use it with FP-Delta encoding, must used a modified version of Parquet.
This is done as follows:
- Change the dependency version of `org.apache.parquet` from `1.12.2` to `1.13.0-SNAPSHOTpom.xml`
- Go to the folder [`parquet-mr`](https://github.com/MajidSas/parquet-mr/) and follow the build instructions.
- Build Spatial Parquet again and use it similar to the `Main` file.

This is currently required because Parquet doesn't provide the option to set a custom encoding. The current implementation of the encoding is experimental and can see some performance improvement in terms of read/write time and providing more options for encoding to provide more flexibility.
