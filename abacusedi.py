import datetime
import logging
import os
import shutil

import env

from krc.krcemail import KrcEmail

notification_emails = [
    'tleoni@krclogistics.com',
    'jmichalski@krclogistics.com',
    'jwaltzjr@krclogistics.com'
]

# C:\ = 10.0.0.132
src_path = r'C:\EDI_FTP_IN\KRC\kschm'
dest_path = r'\\10.0.0.126\e\test_xm_edi_import'
archive_path = r'C:\EDI_FTP_IN\KRC\archives'

runtime = datetime.datetime.now().strftime('%m-%d-%y %H.%M')
if not os.path.exists('logs'):
    os.makedirs('logs')
logging.basicConfig(
    filename=os.path.join('logs', 'edi-import-log %s.log' % runtime),
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s: %(message)s'
)

def import_edi(edi_records, src_path, dest_path, archive_path, archive_time):
    backup_path = create_backup_folder(archive_path, archive_time)
    email_msg = 'The following EDI records were imported:'

    for record in edi_records:
        logging.info('Processing %s' % record)
        email_msg += parse_record(record)
        backup_record(record, src_path, backup_path)
        import_record(record, src_path, dest_path)
        logging.info('Finished processing %s' % record)

    send_email(email_msg, notification_emails)

def parse_record(record):
    logging.info('Parsing record %s' % record)

    current_record_path = os.path.join(src_path, record)
    with open(current_record_path, 'r') as current_record:
        logging.info('Record opened successfully.')
        order_info = '\n'
        edi = current_record.readlines()

        for line in edi:
            if line[:3] == 'N1*':
                order_info += '\n' + line.split('*')[2].strip()
            elif line[:3] == 'N3*':
                order_info += '\n' + line.split('*')[1].strip()
            elif line[:3] == 'N4*':
                order_info += '\n' + line.split('*')[1].strip() + ', ' + \
                    line.split('*')[2].strip() + ' ' + line.split('*')[3].strip()
            elif line[:6] == 'N9*BM*':
                order_info += '\n' + 'BOL: ' + line.split('*')[2].strip()

    clean_info = order_info.replace('~','')
    logging.info('Parsed record successfully.')
    logging.debug('Order information: %s' % clean_info.strip().replace('\n', ' '))
    return clean_info

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
    shutil.copy(src_file, backup_file)
    logging.info('Backup was successful.')

def import_record(record, src_path, import_path):
    # Move into the folder Abacus is watching for EDI
    logging.info('Moving from %s to %s' % (src_path, import_path))
    src_file = os.path.join(src_path, record)
    import_file = os.path.join(import_path, record)
    shutil.move(src_file, import_file)
    logging.info('Move was successful.')

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
        subject='New EDI Records Found in Abacus',
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
        import_edi(edi_records, src_path, dest_path, archive_path, runtime)
    except:
        logger.exception('Import failed. Closing...')
    logging.info('Import was finished successfully. Closing...')
else:
    logging.warning('Edi records not found. Closing...')
