# Smart Meter Data Processing Pipeline — Design Part
# SOFE4630U Milestone 3
#
# This Apache Beam pipeline reads smart meter JSON messages from a Pub/Sub
# topic, filters out records with missing measurements, converts pressure
# from kPa to psi and temperature from Celsius to Fahrenheit, and writes
# the processed data to another Pub/Sub topic.

import argparse
import json
import logging

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


class FilterMissing(beam.DoFn):
    """Eliminate records with missing measurements (containing None)."""

    def process(self, element):
        # Check all values in the measurement dictionary
        for key, value in element.items():
            if value is None or str(value).strip().lower() == 'none':
                # Skip this record — it has a missing measurement
                logging.info('Filtered out record with missing value for key: %s', key)
                return
        yield element


class ConvertUnits(beam.DoFn):
    """Convert pressure from kPa to psi and temperature from Celsius to Fahrenheit.

    Conversion formulas:
        P(psi) = P(kPa) / 6.895
        T(F)   = T(C) * 1.8 + 32
    """

    def process(self, element):
        result = dict(element)  # shallow copy

        # Convert pressure: kPa → psi
        if 'Pressure(kPa)' in result:
            try:
                result['Pressure(psi)'] = float(result['Pressure(kPa)']) / 6.895
                del result['Pressure(kPa)']
            except (ValueError, TypeError):
                pass

        # Convert temperature: Celsius → Fahrenheit
        if 'Temperature(C)' in result:
            try:
                result['Temperature(F)'] = float(result['Temperature(C)']) * 1.8 + 32
                del result['Temperature(C)']
            except (ValueError, TypeError):
                pass

        yield result


def run(argv=None):
    parser = argparse.ArgumentParser(
        description='Smart Meter Dataflow Pipeline',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        '--input',
        dest='input',
        required=True,
        help='Input Pub/Sub topic to read from, e.g. projects/<PROJECT>/topics/<TOPIC>',
    )
    parser.add_argument(
        '--output',
        dest='output',
        required=True,
        help='Output Pub/Sub topic to write to, e.g. projects/<PROJECT>/topics/<TOPIC>',
    )
    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True

    with beam.Pipeline(options=pipeline_options) as p:
        # 1. Read from PubSub: read measurement readings (bytes → JSON dict)
        readings = (
            p
            | 'Read from PubSub' >> beam.io.ReadFromPubSub(topic=known_args.input)
            | 'To Dict' >> beam.Map(lambda x: json.loads(x))
        )

        # 2. Filter: eliminate records with missing measurements (None)
        filtered = readings | 'Filter' >> beam.ParDo(FilterMissing())

        # 3. Convert: pressure kPa→psi, temperature C→F
        converted = filtered | 'Convert' >> beam.ParDo(ConvertUnits())

        # 4. Write to PubSub: send processed measurements to output topic
        (
            converted
            | 'To Bytes' >> beam.Map(lambda x: json.dumps(x).encode('utf-8'))
            | 'Write to PubSub' >> beam.io.WriteToPubSub(topic=known_args.output)
        )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
