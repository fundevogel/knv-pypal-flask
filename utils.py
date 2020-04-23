# ~*~ coding=utf-8 ~*~

from hashlib import md5
from io import StringIO, BytesIO

from pandas import read_csv
from PyPDF2 import PdfFileReader, PdfFileMerger
from PyPDF2.utils import PdfReadError


# CORE #

def load_data(csv_files, encoding='iso-8859-1', delimiter=';'):
    data = []

    for csv_file in csv_files:
        try:
            text_stream = StringIO((csv_file.stream.read()).decode(encoding))
            df = read_csv(text_stream, sep=delimiter)
            data += df.to_dict('records')

        except FileNotFoundError:
            continue

    return data


def match_pdf(data, invoices):
    # Extract matching numbers
    matched_numbers = []

    for item in data:
        if item['Vorgang'] != 'nicht zugeordnet':
            if ';' in item['Vorgang']:
                matched_numbers += [number for number in item['Vorgang'].split(';')]
            else:
                matched_numbers.append(item['Vorgang'])

    # Init merger object
    merger = PdfFileMerger()

    # Merge corresponding invoices
    for number in dedupe(matched_numbers):
        if number in invoices:
            content = invoices[number]

            byte_stream = BytesIO(content.read())
            byte_stream.seek(0)

            try:
                merger.append(PdfFileReader(byte_stream))
                byte_stream.close()

            except PdfReadError:
                pass

    return merger


# UTILITIES #

def dedupe(duped_data, encoding='utf-8'):
    deduped_data = []
    codes = set()

    for item in duped_data:
        hash_digest = md5(str(item).encode(encoding)).hexdigest()

        if hash_digest not in codes:
            codes.add(hash_digest)
            deduped_data.append(item)

    return deduped_data


def group_data(ungrouped_data):
    grouped_data = {}

    for item in ungrouped_data:
        try:
            if 'Datum' in item:
                _, month, year = item['Datum'].split('.')

        except ValueError:
            # EOF
            pass

        code = '-'.join([str(year), str(month)])

        if code not in grouped_data.keys():
            grouped_data[code] = []

        grouped_data[code].append(item)

    return grouped_data
