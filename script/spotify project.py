import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node tracks
tracks_node1712083182824 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://projeto-spotify-datalwhdata/staging/track.csv"], "recurse": True}, transformation_ctx="tracks_node1712083182824")

# Script generated for node artists
artists_node1712083064093 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://projeto-spotify-datalwhdata/staging/artists.csv"], "recurse": True}, transformation_ctx="artists_node1712083064093")

# Script generated for node albuns
albuns_node1712083179956 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://projeto-spotify-datalwhdata/staging/albums.csv"], "recurse": True}, transformation_ctx="albuns_node1712083179956")

# Script generated for node Join album e artista
Joinalbumeartista_node1712083382677 = Join.apply(frame1=albuns_node1712083179956, frame2=artists_node1712083064093, keys1=["artist_id"], keys2=["id"], transformation_ctx="Joinalbumeartista_node1712083382677")

# Script generated for node Join tracks
Jointracks_node1712083518917 = Join.apply(frame1=tracks_node1712083182824, frame2=Joinalbumeartista_node1712083382677, keys1=["track_id"], keys2=["track_id"], transformation_ctx="Jointracks_node1712083518917")

# Script generated for node Drop Fields
DropFields_node1712083636232 = DropFields.apply(frame=Jointracks_node1712083518917, paths=[], transformation_ctx="DropFields_node1712083636232")

# Script generated for node destinio
destinio_node1712083680376 = glueContext.getSink(path="s3://projeto-spotify-datalwhdata/datawharehouse/", connection_type="s3", updateBehavior="LOG", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="destinio_node1712083680376")
destinio_node1712083680376.setCatalogInfo(catalogDatabase="spotify",catalogTableName="datawarehouse")
destinio_node1712083680376.setFormat("glueparquet", compression="snappy")
destinio_node1712083680376.writeFrame(DropFields_node1712083636232)
job.commit()