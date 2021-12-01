import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [TempDir, JOB_NAME]
args = getResolvedOptions(sys.argv, ['TempDir','JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
## @type: DataSource
## @args: [database = "olympics", table_name = "athletes", transformation_ctx = "datasource0"]
## @return: datasource0
## @inputs: []
datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "olympics", table_name = "athletes", transformation_ctx = "datasource0")
## @type: ApplyMapping
## @args: [mapping = [("index", "long", "id", "long"), ("id", "int", "athlete_id", "int"), ("name", "string", "name", "string"), ("sex", "string", "sex", "string"), ("age", "float", "age", "int"), ("height", "float", "height", "int"), ("weight", "float", "weight", "int"), ("team", "string", "team", "string"), ("noc", "string", "noc", "string"), ("games", "string", "games", "string"), ("year", "int", "year", "int"), ("season", "string", "season", "string"), ("city", "string", "city", "string"), ("sport", "string", "sport", "string"), ("event", "string", "event", "string"), ("medal", "string", "medal", "string")], transformation_ctx = "applymapping1"]
## @return: applymapping1
## @inputs: [frame = datasource0]
applymapping1 = ApplyMapping.apply(frame = datasource0, mappings = [("index", "int", "id", "int"), ("id", "int", "athlete_id", "int"), ("name", "string", "name", "string"), ("sex", "string", "sex", "string"), ("age", "float", "age", "int"), ("height", "float", "height", "int"), ("weight", "float", "weight", "int"), ("team", "string", "team", "string"), ("noc", "string", "noc", "string"), ("games", "string", "games", "string"), ("year", "int", "year", "int"), ("season", "string", "season", "string"), ("city", "string", "city", "string"), ("sport", "string", "sport", "string"), ("event", "string", "event", "string"), ("medal", "string", "medal", "string")], transformation_ctx = "applymapping1")
## @type: ResolveChoice
## @args: [choice = "make_cols", transformation_ctx = "resolvechoice2"]
## @return: resolvechoice2
## @inputs: [frame = applymapping1]
resolvechoice2 = ResolveChoice.apply(frame = applymapping1, choice = "make_cols", transformation_ctx = "resolvechoice2")
## @type: DropNullFields
## @args: [transformation_ctx = "dropnullfields3"]
## @return: dropnullfields3
## @inputs: [frame = resolvechoice2]
dropnullfields3 = DropNullFields.apply(frame = resolvechoice2, transformation_ctx = "dropnullfields3")
## @type: DataSink
## @args: [catalog_connection = "redshift-cluster-olympics", connection_options = {"dbtable": "athletes_events", "database": "olympics"}, redshift_tmp_dir = TempDir, transformation_ctx = "datasink4"]
## @return: datasink4
## @inputs: [frame = dropnullfields3]
datasink4 = glueContext.write_dynamic_frame.from_jdbc_conf(frame = dropnullfields3, catalog_connection = "redshift-cluster-olympics", connection_options = {"dbtable": "athletes_events", "database": "olympics"}, redshift_tmp_dir = args["TempDir"], transformation_ctx = "datasink4")
job.commit()
