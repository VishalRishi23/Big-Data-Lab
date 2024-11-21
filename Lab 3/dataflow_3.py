import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import StandardOptions
options = PipelineOptions()
google_cloud_options = options.view_as(GoogleCloudOptions)
google_cloud_options.project = 'innate-shape-340909'
google_cloud_options.job_name = 'lab4'
google_cloud_options.temp_location = "gs://ch18b013/tmp"
google_cloud_options.region = "us-central1"
options.view_as(StandardOptions).runner = 'DataflowRunner'
p = beam.Pipeline(options=options)
pc = p | 'Read' >> beam.io.ReadFromText('gs://bdl2022/lines_big.txt')
pc = pc | 'Count' >>  beam.FlatMap(lambda line: [len(line.split(' '))])
pc = pc | 'Average' >> beam.CombineGlobally(beam.combiners.MeanCombineFn())
pc = pc | 'Write' >> beam.io.WriteToText('gs://ch18b013/Outputs/Output2.txt')
result = p.run()