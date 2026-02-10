import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--project", required=True)
parser.add_argument("--input_path", required=True)
parser.add_argument("--bq_dataset", required=True)
parser.add_argument("--bq_table", required=True)
parser.add_argument("--bq_schema", required=True)
parser.add_argument("--temp_location", required=True)
parser.add_argument("--staging_location", required=True)
parser.add_argument("--execution_ts", required=True)
args, beam_args = parser.parse_known_args()

options = PipelineOptions(
    beam_args,
    project=args.project,
    temp_location=args.temp_location,
    staging_location=args.staging_location,
    save_main_session=True
)

def transform_customer(row):
    fields = row.split(",")
    if len(fields) != 3:
        return None
    customer_id, name, email = fields
    return {
        "customer_id": customer_id.strip(),
        "name": name.strip(),
        "email": email.strip()
    }

# with beam.Pipeline(options=options) as p:
#     (
#         p
#         | "ReadCSV" >> beam.io.ReadFromText(args.input_path, skip_header_lines=1)
#         | "Transform" >> beam.Map(transform_customer)
#         | "FilterNone" >> beam.Filter(lambda x: x is not None)
#         | "WriteToBigQuery" >> beam.io.WriteToBigQuery(
#             table=f"{args.project}:{args.bq_dataset}.{args.bq_table}",
#             schema=args.bq_schema,
#             write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
#             create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
#         )
#     )
# -------------------------
# Beam pipeline (local test version)
# -------------------------
with beam.Pipeline(options=options) as p:
    (
        p
        | "ReadCSV" >> beam.io.ReadFromText(args.input_path, skip_header_lines=1)
        | "Transform" >> beam.Map(transform_customer)
        | "FilterNone" >> beam.Filter(lambda x: x is not None)
        | "WriteToLocalFile" >> beam.io.WriteToText(
            f"test_data/{args.bq_table}_output.txt"
        )
    )