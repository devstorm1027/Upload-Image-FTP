
import pymysql
import os
import csv
import re
import urllib
import ftplib

from PIL import Image
from io import StringIO, BytesIO
from PyPDF2 import PdfFileWriter, PdfFileReader


class Insert(object):

    def __init__(self):

        self.host = os.environ['DB_HOST']
        self.username = os.environ['DB_USER']
        self.password = os.environ['DB_PASSWORD']
        self.db = os.environ['DB']
        self.port = int(os.environ['DB_PORT'])

        self.ftp_port = int(os.environ['FTP_PORT'])
        self.ftp_user = int(os.environ['FTP_USER'])
        self.ftp_password = int(os.environ['FTP_PASSWORD'])

        self.cursor = self.connect_db()

    def connect_db(self):
        cnx = {'host': self.host,
               'username': self.username,
               'password': self.password,
               'db': self.db,
               'port': int(self.port)
               }
        self.conn = pymysql.connect(db=cnx['db'], host=cnx['host'], port=cnx['port'], user=cnx['username'],
                                    password=cnx['password'])
        self.cursor = self.conn.cursor()
        return self.cursor

    def select_id(self):
        self.cursor.execute("""SELECT id FROM wpqq_vrm_records""")
        ids = self.cursor.fetchall()
        return ids

    def insert_sku(self):
        ids = self.select_id()
        try:
            with open('/home/ubuntu/energy_result/final/transformers.csv', 'r') as f:
                reader = csv.reader(f)
                lis = [line for line in reader]
                for row in lis[1:]:
                    if not row[7].strip():
                        row[7] = 'NULL'
                    for id in ids[619:]:
                        self.cursor.execute("""SELECT e_sku FROM wpqq_vrm_records WHERE id = {}""".format(id[0]))
                        if not self.cursor._rows[0][0]:
                            self.cursor.execute("""UPDATE wpqq_vrm_records SET e_sku = {} WHERE id = {}""".format('"{}"'.format(row[7].strip()), id[0]))

                            self.conn.commit()
                            break
                print('Successed')
        except:
            print('Error')
        return

    def insert_title(self):
        ids = self.select_id()
        try:
            with open('/home/ubuntu/energy_result/final/dc-dc-converters.csv.csv', 'r') as f:
                reader = csv.reader(f)
                lis = [line for line in reader]
                for row in lis[1:]:
                    if not row[3].strip():
                        row[3] = 'NULL'
                    for id in ids[619:]:
                        self.cursor.execute("""SELECT e_title FROM wpqq_vrm_records WHERE id = {}""".format(id[0]))
                        if not self.cursor._rows[0][0]:
                            self.cursor.execute("""UPDATE wpqq_vrm_records SET e_title = {} WHERE id = {}""".format('"{}"'.format(row[3]), id[0]))

                            self.conn.commit()
                            break
                print('Successed')
        except:
            print('Error')
        return

    def insert_description(self):
        ids = self.select_id()
        try:
            with open('/home/ubuntu/energy_result/final/transformers.csv', 'r') as f:
                reader = csv.reader(f)
                lis = [line for line in reader]
                for row in lis[1:]:
                    if not row[2].strip():
                        row[2] = 'NULL'
                    for id in ids[619:]:
                        self.cursor.execute("""SELECT e_description FROM wpqq_vrm_records WHERE id = {}""".format(id[0]))
                        if not self.cursor._rows[0][0]:
                            self.cursor.execute(
                                """UPDATE wpqq_vrm_records SET e_description = {} WHERE id = {}""".format('"{}"'.format(
                                    str(row[2].replace("'", "").replace('"', '').strip().encode('utf-8'))[2:-1]), id[0]))
                            self.conn.commit()
                            break
                print('Successed')
        except:
            print('Error')
        return

    def insert_price(self):
        ids = self.select_id()
        try:
            with open('/home/ubuntu/energy_result/final/transformers.csv', 'r') as f:
                reader = csv.reader(f)
                lis = [line for line in reader]
                for row in lis[1:]:
                    currency = 'USD'
                    if not row[5].strip():
                        currency = 'NULL'
                    for id in ids[619:]:
                        self.cursor.execute("""SELECT e_price FROM wpqq_vrm_records WHERE id = {}""".format(id[0]))
                        if not self.cursor._rows[0][0]:
                            self.cursor.execute(
                                """UPDATE wpqq_vrm_records SET e_currency = {} WHERE id = {}""".format('"{}"'.format(currency), id[0]))
                            if row[5]:
                                row[5] = re.search(r"\d*\.\d+|\d+", row[5].replace(',', '').strip()).group()
                            else:
                                row[5] = 'NULL'
                            self.cursor.execute("""UPDATE wpqq_vrm_records SET e_price = {} WHERE id = {}""".format('"{}"'.format(row[5]), id[0]))

                            self.conn.commit()
                            break
                print('Successed')
        except:
            print('Error')
        return

    def insert_datasheet(self):
        ids = self.select_id()
        try:
            with open('/home/ubuntu/energy_result/final/transformers.csv', 'r') as f:
                reader = csv.reader(f)
                lis = [line for line in reader]
                for row in lis[1:]:
                    if not row[0].strip():
                        row[0] = 'NULL'
                    for id in ids[619:]:
                        self.cursor.execute("""SELECT e_datasheet FROM wpqq_vrm_records WHERE id = {}""".format(id[0]))
                        if not self.cursor._rows[0][0]:
                            self.cursor.execute("""UPDATE wpqq_vrm_records SET e_datasheet = {} WHERE id = {}""".format('"{}"'.format(row[0]), id[0]))

                            self.conn.commit()
                            break
                print('Successed')
        except:
            print('Error')

    def insert_warranty(self):
        ids = self.select_id()
        try:
            with open('/home/ubuntu/energy_result/final/transformers.csv', 'r') as f:
                reader = csv.reader(f)
                lis = [line for line in reader]
                for row in lis[1:]:
                    if not row[1].strip():
                        row[1] = 'NULL'
                    for id in ids[619:]:
                        self.cursor.execute("""SELECT e_warranty FROM wpqq_vrm_records WHERE id = {}""".format(id[0]))
                        if not self.cursor._rows[0][0]:
                            self.cursor.execute("""UPDATE wpqq_vrm_records SET e_warranty = {} WHERE id = {}""".format('"{}"'.format(row[1]), id[0]))

                            self.conn.commit()
                            break
                print('Successed')
        except:
            print('Error')

    def insert_category(self):
        ids = self.select_id()
        try:
            with open('/home/ubuntu/energy_result/final/transformers.csv', 'r') as f:
                reader = csv.reader(f)
                lis = [line for line in reader]
                for row in lis[1:]:
                    if not row[8].strip():
                        row[8] = 'NULL'
                    for id in ids[619:]:
                        self.cursor.execute("""SELECT e_category FROM wpqq_vrm_records WHERE id = {}""".format(id[0]))
                        if not self.cursor._rows[0][0]:
                            self.cursor.execute("""UPDATE wpqq_vrm_records SET e_category = {} WHERE id = {}""".format('"{}"'.format(row[8]), id[0]))

                            self.conn.commit()
                            break
                print('Successed')
        except:
            print('Error')

    def insert_subcategory(self):
        ids = self.select_id()
        try:
            with open('/home/ubuntu/energy_result/final/transformers.csv', 'r') as f:
                reader = csv.reader(f)
                lis = [line for line in reader]
                for row in lis[1:]:
                    if not row[9].strip():
                        row[9] = 'NULL'
                    for id in ids[619:]:
                        self.cursor.execute("""SELECT e_subcategory FROM wpqq_vrm_records WHERE id = {}""".format(id[0]))
                        if not self.cursor._rows[0][0]:
                            self.cursor.execute("""UPDATE wpqq_vrm_records SET e_subcategory = {} WHERE id = {}""".format('"{}"'.format(row[9]), id[0]))

                            self.conn.commit()
                            break
                print('Successed')
        except:
            print('Error')

    def insert_image_links(self):
        session = ftplib.FTP('109.199.122.155', 'andy_d@positive.net.au', 'positive123')
        try:
            with open('/home/ubuntu/energy_result/final/transformers.csv', 'r') as f:
                reader = csv.reader(f)
                lis = [line for line in reader]
                index = 1
                for row in lis[1:]:
                    if row[6]:
                        rows = row[6].split(',')
                        for r in rows:
                            img = Image.open(BytesIO(urllib.request.urlopen(r).read()))
                            image_name = r.split('/')[-1]
                            img.save(image_name)
                            session.storbinary('STOR ' + '20180621_transformers_' + str(index) + '.png', open(image_name, 'rb'))
                            os.remove(image_name)
                            index += 1
                            # session.quit()

                print('Successed')
        except:
            print('Error')
        return

    def insert_dir_links(self):
        session = ftplib.FTP(ftp_port, ftp_user, ftp_password)
        lis = []
        try:
            with open('/home/ubuntu/energy_result/final/dc-dc-converters.csv', 'r') as f:
                reader = csv.reader(f)
                lis = [line for line in reader]
                f_dir = ''
                s_dir = ''

                for row in lis[1:]:
                    if f_dir == '':
                        session.mkd(row[8].lower())
                        session.cwd(row[8].lower())
                        session.mkd(row[9].lower())
                        session.cwd(row[9].lower())
                        session.mkd(row[7].strip())
                        session.cwd(row[7].strip())

                    elif row[8].lower() != f_dir:
                        session.pwd('/')
                        session.mkd(row[8].lower())
                        session.cwd(row[8].lower())
                        session.mkd(row[9].lower())
                        session.cwd(row[9].lower())
                        session.mkd(row[7].strip())
                        session.cwd(row[7].strip())

                    elif row[8].lower() == f_dir and row[9].lower() == s_dir:
                        session.mkd(row[7].strip())
                        session.cwd(row[7].strip())

                    if row[6]:
                        rows = row[6].split(',')
                        index = 1
                        session.mkd('Image')
                        session.cwd('Image')
                        for r in rows:
                            img = Image.open(BytesIO(urllib.request.urlopen(r).read()))
                            image_name = r.split('/')[-1]
                            img.save(image_name)
                            session.storbinary('STOR ' + row[7] + '_' + str(index) + '.png',
                                               open(image_name, 'rb'))
                            os.remove(image_name)
                            index += 1
                        session.cwd('/' + row[8].lower() + '/' + row[9].lower() + '/' + row[7].strip())

                    pdfs = [row[0], row[1]]
                    p_index = 1

                    session.mkd('Pdf')
                    session.cwd('Pdf')
                    for p in pdfs:
                        if p != '':
                            pdf = p.split(',')
                            for pf in pdf:
                                bytePdfFile = BytesIO(urllib.request.urlopen(pf).read())
                                if str(bytePdfFile.getvalue())[2:-1] != '':
                                    pdfFile = PdfFileReader(bytePdfFile)
                                    writer = PdfFileWriter()

                                    for pageNum in range(pdfFile.getNumPages()):
                                        currentPage = pdfFile.getPage(pageNum)
                                        # currentPage.mergePage(watermark.getPage(0))
                                        writer.addPage(currentPage)
                                    pdf_name = pf.split('/')[-1]
                                    outputStream = open(pdf_name, "wb")
                                    writer.write(outputStream)
                                    outputStream.close()
                                    session.storbinary('STOR ' + row[7] + '_' + str(p_index) + '.pdf',
                                                       open(pdf_name, 'rb'))
                                    os.remove(pdf_name)
                                    p_index += 1


                    f_dir = row[8].lower()
                    i = len(lis[1:])

                    if (i == lis[1:].index(row) + 1):
                        print('Successed')
                        return

                    s_dir = lis[1:][lis[1:].index(row) + 1][9].lower()

                    session.cwd('/' + row[8].lower() + '/' + row[9].lower())
                    last_dir_name = session.pwd().split('/')[-1]
                    if s_dir != last_dir_name:
                        session.cwd('/' + row[8].lower())
                        session.mkd(s_dir)
                        session.cwd('/' + row[8].lower() + '/' + s_dir)

                print('Successed')
        except Exception as e:
            print(e)
        return

def main(event, context):
    ins = Insert()
    # ins.insert_sku()
    # ins.insert_title()
    # ins.insert_description()
    # ins.insert_price()
    # ins.insert_datasheet()
    # ins.insert_warranty()
    # ins.insert_category()
    # ins.insert_subcategory()
    # ins.insert_image_links()
    # ins.insert_pdf()
    # ins.insert_dir_links()

if __name__ == "__main__":
    main(0, 0)