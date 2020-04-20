# ~*~ coding=utf-8 ~*~

from datetime import datetime, timedelta
from hashlib import md5
from io import StringIO, BytesIO
from itertools import groupby
from operator import itemgetter

from pandas import DataFrame, read_csv
from PyPDF2 import PdfFileReader, PdfFileMerger
from PyPDF2.utils import PdfReadError


# CORE #

def match_data(payment_data, order_data, info_data):
    # Processing data for ..
    # (1) .. payments
    # (2) .. orders
    # (3) .. infos
    payments = process_payments(payment_data)
    orders = process_orders(order_data)
    infos = process_infos(info_data)

    results = []

    for payment in payments:
        # Assign payment to invoice number(s)
        # (1) Find matching order for current payment
        # (2) Find matching invoice number for this order
        matching_order = match_orders(payment, orders)

        if not matching_order:
            results.append(payment)
            continue

        matching_infos = match_infos(matching_order, infos)

        # Skip if no matching invoice numbers
        if not matching_infos:
            results.append(payment)
            continue

        # Store data
        # (1) Add invoice number(s) to payment data
        # (2) Save matched payment
        payment['Vorgang'] = ';'.join(matching_infos)
        results.append(payment)

    return results


# UTILITIES #

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


# PROCESSING #

def convert_cost(string):
    string = float(string.replace(',', '.'))
    integer = f'{string:.2f}'

    return str(integer)


def process_payments(data):
    payments = []

    for item in data:
        # Skip withdrawals
        if item['Brutto'][:1] == '-':
            continue

        payment = {}

        try:
            payment['Datum'] = item['Datum']
            payment['Vorgang'] = 'nicht zugeordnet'
            payment['Name'] = item['Name']
            payment['Brutto']  = convert_cost(item['Brutto'])
            payment['Gebühr'] = item['Gebühr']
            payment['Netto'] = item['Netto']

            payments.append(payment)

        except AttributeError:
            pass

    payments.sort(key=lambda payment: datetime.strptime(payment['Datum'], '%d.%m.%Y'))

    return dedupe(payments)


def process_orders(order_data):
    orders = []

    for key, data in groupby(order_data, itemgetter('ormorderid')):
        # (1) You know what they say, `'itertools._grouper' object is not subscriptable`
        # (2) Just a silly shorthand since we don't need another loop
        item = list(data)[0]

        # TODO: Skip alternative payment methods
        if 'paymenttype' in item and item['paymenttype'] != 'PAYPAL':
            continue

        order = {}

        order['ID'] = item['ormorderid']
        date_object = datetime.strptime(item['timeplaced'][:10], '%Y-%m-%d')
        order['Datum'] = date_object.strftime('%d.%m.%Y')
        order['Name'] = ' '.join([item['rechnungaddressfirstname'], item['rechnungaddresslastname']])
        order['Betrag'] = convert_cost(item['totalordercost'])

        orders.append(order)

    return dedupe(orders)


def process_infos(info_data):
    infos = {}

    for key, data in groupby(info_data, itemgetter('OrmNumber')):
        numbers = [str(item['Invoice Number'])[:-2] for item in data if str(item['Invoice Number']) != 'nan']
        infos[key] = dedupe(numbers)

    return infos


# MATCHING #

def match_dates(base_date, test_date, days=1):
    date_objects = [datetime.strptime(date, '%d.%m.%Y') for date in [base_date, test_date]]
    date_range = timedelta(days=days)

    if date_objects[0] <= date_objects[1] <= date_objects[0] + date_range:
        return True

    return False


def match_orders(payment, orders):
    candidates = []

    for item in orders:
        costs_match = payment['Brutto'] == item['Betrag']
        dates_match = match_dates(payment['Datum'], item['Datum']) == True

        if costs_match and dates_match:
            hits = 0

            # TODO: Levenshtein
            candidates.append((hits, item))

    matches = sorted(candidates, key=itemgetter(0), reverse=True)

    if matches:
        return matches[0][1]

    return {}


def match_infos(order, infos):
    info = []

    for order_id, numbers in infos.items():
        if order_id == order['ID']:
            info = numbers

    return info


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
