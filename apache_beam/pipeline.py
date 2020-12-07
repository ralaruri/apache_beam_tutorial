import logging
import apache_beam as beam
import argparse
import requests 

from datetime import datetime

def parse_lines(element):
    return element.split(",")


class CalcVisitDuration(beam.DoFn):
    def process(self, element):
        dt_format = "%Y-%m-%dT%H:%M:%S"
        start_dt = datetime.strptime(element[1], dt_format)
        end_dt = datetime.strptime(element[2], dt_format)
        diff = end_dt - start_dt 
        yield [element[0], diff.total_seconds()]

class GetIpCountryOrigin(beam.DoFn):
    def process(self, element):
        ip = element[0]
        response = requests.get(f"http://ip-api.com/json/{ip}?fields=country")
        country = response.json()["country"]
        yield [ip, country]

def map_country_to_ip(element, ip_map):
    # ['United States', 220.00]
    # {'0.0.0.0': 'United States, '1.1.1.1': 'United States'}

    ip = element[0]
    return [ip_map[ip], element[1]]

    # ['United States', 202.00]

def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--input")
    parser.add_argument("--output")
    args, beam_args = parser.parse_known_args(argv)

    with beam.Pipeline(argv=beam_args) as p:
        lines = (
            p 
            | "ReadFile" >> beam.io.ReadFromText(args.input, skip_header_lines=1)
            | "Parselines" >> beam.Map(parse_lines)
        )

        duration = lines | "CalcVisitDuration" >> beam.ParDo(CalcVisitDuration()) #< could be maps aswell
        ip_map = lines | "GetIpCountryOrigin" >> beam.ParDo(GetIpCountryOrigin())

        result = (
            duration 
            | "MapIpToCountry" >> beam.Map(map_country_to_ip, ip_map=beam.pvalue.AsDict(ip_map))
            | "AverageByCountry" >> beam.CombinePerKey(beam.combiners.MeanCombineFn())
            | "FormatOuput" >> beam.Map(lambda element: ",".join(map(str, element))) 
            # making sure everythign is converted to string type
        )
        
        result | "WriteOutput" >> beam.io.WriteToText(
            args.output, file_name_suffix=".csv"
        )
        #will print each element in the line in the p.collection. 


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.WARNING)
    run()