import argparse
import logging

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

log = logging.getLogger(__name__)

EXPECTED_FIELD_COUNT = 4


def transform_order(row):
    fields = row.split(",")
    if len(fields) != EXPECTED_FIELD_COUNT:
        log.warning("Skipping malformed row: %r", row)
        return None

    order_id, customer_id, amount, order_date = fields
    try:
        amount = float(amount.strip())
    except ValueError:
        log.warning("Skipping bad amount: %r", row)
        return None

    return {
        "order_id": order_id.strip(),
        "customer_id": customer_id.strip(),
        "amount": amount,
        "order_date": order_date.strip(),
    }


def build_pipeline(p, args, local_mode=False):
    records = (
        p
        | "ReadCSV" >> beam.io.ReadFromText(args.input_path, skip_header_lines=1)
        | "Transform" >> beam.Map(transform_order)
        | "FilterInvalid" >> beam.Filter(lambda x: x is not None)
    )

    if local_mode:
        records | "WriteToLocalFile" >> beam.io.WriteToText(
            f"test_data/{args.bq_table}_output.txt"
        )
    else:
        records | "WriteToBigQuery" >> beam.io.WriteToBigQuery(
            table=f"{args.project}:{args.bq_dataset}.{args.bq_table}",
            schema=args.bq_schema,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        )
    return records


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", required=True)
    parser.add_argument("--input_path", required=True)
    parser.add_argument("--bq_dataset", required=True)
    parser.add_argument("--bq_table", required=True)
    parser.add_argument("--bq_schema", required=True)
    parser.add_argument("--temp_location", required=True)
    parser.add_argument("--staging_location", required=True)
    parser.add_argument("--execution_ts", required=True)
    parser.add_argument("--local_mode", action="store_true")
    return parser.parse_known_args()


if __name__ == "__main__":
    args, beam_args = parse_args()

    options = PipelineOptions(
        beam_args,
        project=args.project,
        temp_location=args.temp_location,
        staging_location=args.staging_location,
        save_main_session=True,
    )

    log.info("Starting orders pipeline. Execution timestamp: %s", args.execution_ts)

    with beam.Pipeline(options=options) as p:
        build_pipeline(p, args, local_mode=args.local_mode)