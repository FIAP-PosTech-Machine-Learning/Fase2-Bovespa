import os
import csv
import re
from datetime import datetime


date_pattern = re.compile(r"IBOVDia_(\d{2})-(\d{2})-(\d{2})\.csv")


def _convert_decimal(num_str):
    return float(num_str.replace('.', '').replace(',', '.'))


def _insert_date_column(file_path, date_value):
    with open(file_path, 'r', newline='', encoding='iso-8859-1') as csvfile:
        reader = csv.reader(csvfile, delimiter=';')
        rows = list(reader)

        header = rows[1]
        if 'date' in header:
            return

        header.insert(0, 'date')

        header[1] = 'setor'
        header[2] = 'codigo'
        header[3] = 'acao'
        header[4] = 'tipo'
        header[5] = 'qtde_teorica'
        header[6] = 'part'
        header[7] = 'part_acum'

        for row in rows[2:]:
            row.insert(0, date_value)

            if (len(row) >= 8):
                row[5] = _convert_decimal(row[5])
                row[6] = _convert_decimal(row[6])
                row[7] = _convert_decimal(row[7])

    with open(file_path, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile, delimiter=';')
        writer.writerows(rows[1:-2])
    print(f"Updated file: {file_path}")


def process_csv_files(directory):
    for filename in os.listdir(directory):
        if filename.endswith('.csv'):
            match = date_pattern.match(filename)
            if match:
                day, month, year = match.groups()

                full_date = f"20{year}-{month}-{day}"
                date_value = datetime.strptime(full_date, "%Y-%m-%d").date()

                file_path = os.path.join(directory, filename)
                _insert_date_column(file_path, date_value)


if __name__ == '__main__':
    process_csv_files('./bovespa/')
