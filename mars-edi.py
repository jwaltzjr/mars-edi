import datetime
import logging
import os
import shutil

import env

from krc.krcemail import KrcEmail

notification_emails = [
    'jwaltzjr@krclogistics.com',
    'jalamillo@krclogistics.com'
]

src_path = r'E:\FTP204dump'
archive_path = r'E:\mars-edi-backup'

runtime = datetime.datetime.now().strftime('%m-%d-%y %H.%M')
if not os.path.exists('logs'):
    os.makedirs('logs')
logging.basicConfig(
    filename=os.path.join('logs', 'edi-import-log %s.log' % runtime),
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s: %(message)s'
)

def import_edi(edi_records, src_path, archive_path, archive_time):
    backup_path = create_backup_folder(archive_path, archive_time)
    email_msg = 'The following EDI records were found:'

    for record in edi_records:
        logging.info('Processing %s' % record)
        email_msg += parse_record(record)
        backup_record(record, src_path, backup_path)
        logging.info('Finished processing %s' % record)

    send_email(email_msg, notification_emails)

def parse_record(record):
    logging.info('Parsing record %s' % record)

    current_record_path = os.path.join(src_path, record)
    with open(current_record_path, 'r') as current_record:
        logging.info('Record opened successfully.')
        edi = current_record.readlines()

    order_info = parse_lines(edi[0].replace('\u2026','\n'))

    logging.info('Parsed record successfully.')
    logging.debug('Order information: %s' % order_info.strip().replace('\n', ' '))
    return order_info

def parse_lines(edi, output='', index=0, totals_parsed=False, pos=[]):
    if index < len(edi):
        split_line = edi[index].split('*')
        if split_line[:2] == ['ST','204']:
            output += '\n\nNew Record: {}\n'.format(split_line[-1])
        elif split_line[0] == 'L11' and split_line[-1] == 'WH':
            output += 'Warehouse Link: {}\n'.format(split_line[1])
        elif split_line[0] == 'L11' and split_line[-1] == 'MB':
            output += 'Master Bill: {}\n'.format(split_line[1])
        elif split_line[0] == 'S5' and not totals_parsed:
            cases = split_line[split_line.index('CA') - 1]
            weight = split_line[split_line.index('L') - 1]
            output += 'Total Cases: {}\nTotal Weight: {}\n'.format(cases, weight)
            totals_parsed = True
        elif split_line[:2] == ['G62','38']:
            formatted_date = format_edi_date(split_line[2])
            output += 'Ship by: {}\n'.format(formatted_date)
        elif split_line[:2] == ['G62','77']:
            formatted_date = format_edi_date(split_line[2])
            output += 'Pickup Date: {}\n'.format(formatted_date)
        elif split_line[0] == 'OID':
            bol_number = split_line[1]
            po_number = split_line[2]
            cases = split_line[split_line.index('CA') + 1]
            weight = split_line[split_line.index('L') + 1]
            if po_number not in pos:
                pos.append(po_number)
                output += 'Order: BOL {} | PO {} | PCS {} | LBS {}\n'.format(
                    bol_number, po_number, cases, weight
                )
        elif split_line[:2] == ['G62','70']:
            formatted_date = format_edi_date(split_line[2])
            output += 'Delivery Date: {}\n'.format(formatted_date)
        elif split_line[:2] == ['G62','53']:
            formatted_date = format_edi_date(split_line[2])
            output += 'Delivery Window Start: {}\n'.format(formatted_date)
        elif split_line[:2] == ['G62','54']:
            formatted_date = format_edi_date(split_line[2])
            output += 'Delivery Window End: {}\n'.format(formatted_date)
        elif split_line[:2] == ['N1','ST']:
            name = split_line[2]
            address = get_split_line(edi, index+1)[1]
            location = get_split_line(edi, index+2)
            city, state, zip_code = location[1:4]
            output += 'Deliver To:\n{}\n{}\n{}, {} {}\n'.format(
                name, address, city, state, zip_code
            )
            index += 2
        elif split_line[0] == 'SE':
            # END OF ORDER
            totals_parsed = False
            pos = []
        return parse_lines(
            edi,
            output=output,
            index=index+1,
            totals_parsed=totals_parsed,
            pos=pos
        )
    else:
        return output

def format_edi_date(raw_date):
    return raw_date[:4] + '-' + raw_date[4:6] + '-' + raw_date[6:8]

def create_backup_folder(parent_archive_path, archive_time):
    current_archive_path = os.path.join(
        parent_archive_path,
        archive_time
    )
    logging.debug('Current archive path: %s' % current_archive_path)

    try:
        os.mkdir(current_archive_path)
        logging.info('Created directory %s' % current_archive_path)
    except:
        logging.warning('Failed to create directory %s' % current_archive_path)

    return current_archive_path

def backup_record(record, src_path, backup_path):
    logging.info('Backing up record from %s to %s' % (src_path, backup_path))
    src_file = os.path.join(src_path, record)
    backup_file = os.path.join(backup_path, record)
    shutil.move(src_file, backup_file)
    logging.info('Backup was successful.')

def send_email(msg, send_to):
    logging.info('Building email...')

    html = """
    <!doctype html>
    <html>
    <body>
    <p>%s</p>
    </body>
    </html>
    """ % msg.replace('\n','<br>')

    email_message = KrcEmail(
        notification_emails,
        subject='New Mars EDI Records',
        message = msg,
        message_html = html,
        password = env.email_password.value
    )
    logging.info('Email was built successfully.')
    try:
        email_message.send()
    except:
        logging.exception('Email could not be sent. Email: %s' % email_message.email.as_string())
    else:
        logging.info('Email was sent successfully.')

edi_records = os.listdir(src_path)
if edi_records:
    logging.info('Edi records found.')
    logging.debug('Records: %s' % edi_records)
    try:
        import_edi(edi_records, src_path, archive_path, runtime)
    except:
        logging.exception('Import failed. Closing...')
        raise
    logging.info('Import was finished successfully. Closing...')
else:
    logging.warning('Edi records not found. Closing...')
