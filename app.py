#!/usr/bin/env python3.6
# ~*~ coding=utf-8 ~*~

import os
from io import StringIO, BytesIO
from zipfile import ZipFile, ZipInfo

from flask import Flask, request, redirect, url_for, render_template, send_file
from knv_pypal import match_data
from pandas import DataFrame, read_csv

from utils import load_data, match_pdf, group_data


app = Flask(__name__)

# Limit upload size to 8 MB
app.config['MAX_CONTENT_LENGTH'] = 8 * 1024 * 1024

ALLOWED_EXTENSIONS = {'pdf', 'csv'}


def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        # TODO: Fix this
        # if 'file' not in request.files:
        #     print('No file attached in request')
        #     return redirect(request.url)

        # Load CSV data
        # (1) Single sources
        payment_data = load_data(request.files.getlist('payments'), 'utf-8', ',')
        order_data = load_data(request.files.getlist('orders'))
        info_data = load_data(request.files.getlist('infos'))
        # (2) Matched sources
        matched_data = knv_pypal.match_data(payment_data, order_data, info_data)

        # Load PDF data
        invoices = {}

        for invoice in request.files.getlist('invoices'):
            number = invoice.filename.split('-')[2][:-4]
            invoices[number] = invoice

        results = []

        for code, data in group_data(matched_data).items():
            # Generate CSV stream
            # (1) Init text stream
            # (2) Write CSV dataframe
            text_stream = StringIO()
            DataFrame(data).to_csv(text_stream, index=False, sep=';')
            # (3) Init byte stream
            # (4) Write text stream
            csv_stream = BytesIO()
            csv_stream.write(text_stream.getvalue().encode('utf-8'))
            # (5) Set pointer to start of byte stream
            # (6) Close text stream
            csv_stream.seek(0)
            text_stream.close()

            # Prepare PDF data
            merger = match_pdf(data, invoices)

            # Generate PDF stream
            # (1) Init PDF stream
            # (2) Write PDF bytedata
            pdf_stream = BytesIO()
            merger.write(pdf_stream)
            # (3) Set pointer to start of byte stream
            pdf_stream.seek(0)

            # Pass code & generated streams to results
            results.append((code, csv_stream, pdf_stream))

        # Init ZIP stream
        archive = BytesIO()

        # Show time
        with ZipFile(archive, 'w') as zip_file:
            for result in results:
                code, csv_stream, pdf_stream = result

                pdf_file = ZipInfo(code + '.pdf')
                zip_file.writestr(pdf_file, pdf_stream.read())

                csv_file = ZipInfo(code + '.csv')
                zip_file.writestr(csv_file, csv_stream.read())

        archive.seek(0)

        return send_file(
            archive,
            as_attachment=True,
            attachment_filename='test.zip',
            mimetype='application/zip',
        )

    return render_template('index.html')


if __name__ == "__main__":
    app.run(host='127.0.0.1', port=8080)
