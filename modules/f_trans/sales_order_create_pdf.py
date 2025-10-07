from datetime import datetime
import datetime as dt
from decimal import Decimal
from io import BytesIO
from fastapi.responses import StreamingResponse
from fpdf import FPDF
import qrcode


class PDF(FPDF):
    def __init__(self, data, orientation="P", unit="mm", format="A4"):
        super().__init__(orientation, unit, format)
        self.data = data

    def header(self):
        self.add_font("Poppins", "", "files/font/Poppins/Poppins-Regular.ttf")
        self.add_font("Poppins", "B", "files/font/Poppins/Poppins-Bold.ttf")
        self.add_font("Poppins", "I", "files/font/Poppins/Poppins-Italic.ttf")
        y = self.get_y()
        self.image("files/img/id_food.png", w=35, h=10, keep_aspect_ratio=True)
        page_width = self.w
        left_margin = self.l_margin
        right_margin = self.r_margin
        full_w = page_width - left_margin - right_margin

        w = full_w / 2
        self.set_xy(full_w - 45, 5)
        self.set_font("Poppins", "B", 9)
        self.set_text_color(20, 86, 166)

        self.cell(
            w=w / 2,
            h=4,
            text="INVOICE-IDFOOD",
            align="R",
            new_x="LMARGIN",
            new_y="NEXT",
        )
        self.set_font("Poppins", "", 8)

        self.set_text_color(0, 0, 0)

        self.set_x(full_w - 45)
        self.cell(
            w=w / 2,
            h=4,
            text="Waskita Rajawali Tower",
            align="R",
            new_x="LMARGIN",
            new_y="NEXT",
        )
        self.set_x(full_w - 45)

        self.cell(
            w=w / 2,
            h=4,
            text="Jl. MT Haryono No. 12,",
            align="R",
            new_x="LMARGIN",
            new_y="NEXT",
        )
        self.set_x(full_w - 45)

        self.cell(
            w=w / 2,
            h=4,
            text="Jakarta Timur - 13330",
            align="R",
            new_x="LMARGIN",
            new_y="NEXT",
        )

        self.ln(10)

    def generate_report(self):
        self.add_page()
        self.top_data()
        self.table_data()
        self.bottom()
        filename = f"files/invoice_sales_order/{self.data['id_trans']}.pdf"

        pdf_bytes = self.output(dest="S")
        pdf_buffer = BytesIO(pdf_bytes)
        pdf_buffer.seek(0)
        return pdf_buffer

    def top_data(self):
        self.set_font("Poppins", "I", 9)
        page_width = self.w
        left_margin = self.l_margin
        right_margin = self.r_margin
        full_w = page_width - left_margin - right_margin

        w = full_w / 2
        initial_y = self.get_y()
        self.set_x(full_w - full_w / 4)
        self.cell(
            w=w / 2,
            h=4,
            text="No. Invoice :",
            align="L",
            new_x="LMARGIN",
            new_y="NEXT",
        )
        self.set_x(full_w - full_w / 4)
        self.set_font("Poppins", "", 9)

        self.cell(
            w=w / 2,
            h=4,
            text=self.data["no_invoice"],
            align="L",
            new_x="LMARGIN",
            new_y="NEXT",
        )

        self.ln(5)

        y = self.get_y()
        self.set_xy(left_margin + 5, initial_y)
        # self.set_y(initial_y)
        self.set_font("Poppins", "I", 9)

        self.cell(
            w=w / 2,
            h=4,
            text="Kepada : ",
            align="L",
            new_x="LMARGIN",
            new_y="NEXT",
        )
        self.set_x(left_margin + 5)
        self.set_font("Poppins", "", 9)

        self.cell(
            w=w / 2,
            h=4,
            text=self.data["nama_customer"],
            align="L",
            new_x="LMARGIN",
            new_y="NEXT",
        )

        self.set_x(left_margin + 5)
        self.cell(
            w=50,
            h=4,
            text=self.data["alamat"],
            align="L",
            new_x="LMARGIN",
            new_y="NEXT",
        )

        self.set_x(left_margin + 5)
        self.cell(
            w=50,
            h=4,
            text="Telp/No.Hp : " + str(self.data["no_hp"]),
            align="L",
            new_x="LMARGIN",
            new_y="NEXT",
        )

        self.ln(3)
        invoice_y = self.get_y()
        invoice_x = self.get_x()
        self.set_x(left_margin + 5)
        self.set_font("Poppins", "I", 9)
        self.cell(
            w=w / 2,
            h=4,
            text="Tgl Invoice : ",
            align="L",
            new_x="LMARGIN",
            new_y="NEXT",
        )
        self.set_x(left_margin + 5)
        self.set_font("Poppins", "", 9)
        self.cell(
            w=w / 2,
            h=4,
            text=self.convert_value(self.data["tanggal_invoice"]),
            align="L",
            new_x="LMARGIN",
            new_y="TOP",
        )
        self.set_font("Poppins", "I", 9)

        self.set_xy(invoice_x + 50, invoice_y)
        self.cell(
            w=w / 2,
            h=4,
            text="Jth Tempo : ",
            align="L",
            new_x="LMARGIN",
            new_y="NEXT",
        )
        self.set_font("Poppins", "", 9)
        self.set_x(invoice_x + 50)

        self.cell(
            w=w / 2,
            h=4,
            text=self.convert_value(self.data["tanggal_due_date"]),
            align="L",
            new_x="LMARGIN",
            new_y="NEXT",
        )
        self.ln(2)

        self.set_x(left_margin + 5)
        self.set_font("Poppins", "I", 9)
        self.cell(
            w=w / 2,
            h=4,
            text="Salesman : ",
            align="L",
            new_x="LMARGIN",
            new_y="NEXT",
        )
        self.set_x(left_margin + 5)
        self.set_font("Poppins", "", 9)
        self.cell(
            w=w / 2,
            h=4,
            text=self.data["nama_sales"],
            align="L",
            new_x="LMARGIN",
            new_y="TOP",
        )

        self.set_font("Poppins", "I", 9)
        self.set_y(y - 3)
        self.set_x(full_w - full_w / 4)
        self.cell(
            w=w / 2,
            h=5,
            text="Total Nominal :",
            align="L",
            new_x="LMARGIN",
            new_y="NEXT",
        )

        self.set_font("Arial", "B", 10)
        self.set_x(full_w - full_w / 4)
        self.cell(
            w=w / 2,
            h=4,
            text=f"Rp. {self.convert_value(self.data['harga_total_ppn_pph'])}",
            align="L",
            new_x="LMARGIN",
            new_y="NEXT",
        )

        self.set_font("Poppins", "I", 9)
        self.set_x(full_w - full_w / 4)

        self.set_text_color(178, 4, 4)
        if self.data["complete_payment"] == "Lunas":
            self.set_text_color(33, 163, 102)

        self.cell(
            w=w / 2,
            h=5,
            text=self.data["complete_payment"],
            align="L",
            new_x="LMARGIN",
            new_y="NEXT",
        )
        self.set_text_color(0, 0, 0)

        self.set_font("Poppins", "I", 9)
        self.set_x(full_w - full_w / 4)
        self.cell(
            w=w / 2,
            h=6,
            text="Jenis Pembayaran :",
            align="L",
            new_x="LMARGIN",
            new_y="NEXT",
        )

        self.set_font("Poppins", "", 9)
        self.set_x(full_w - full_w / 4)
        self.cell(
            w=w / 2,
            h=3,
            text=self.data["pembayaran"],
            align="L",
            new_x="LMARGIN",
            new_y="NEXT",
        )

        self.ln(7)

        self.set_font("Poppins", "", 9)

    def table_data(self):
        page_width = self.w
        left_margin = self.l_margin
        right_margin = self.r_margin
        full_w = page_width - left_margin - right_margin
        w = full_w / 2

        header = ["Nama Produk", "Quantity", "Satuan", "Harga Satuan", "Total"]
        keys = [
            self.convert_value(self.data["nama_produk"]),
            self.convert_value(self.data["qty"]),
            self.convert_value(self.data["uom_satuan"]),
            self.convert_value(self.data["harga_satuan"]),
            self.convert_value(self.data["harga_total"]),
        ]

        self.line(
            x1=left_margin + 5,
            y1=self.get_y(),
            x2=self.w - self.r_margin - 5,
            y2=self.get_y(),
        )

        self.set_x(left_margin)
        self.set_font("Poppins", "", 9)

        with self.table(
            col_widths=(
                30,
                30,
                30,
                30,
                30,
            ),
            borders_layout="HORIZONTAL_LINES",
            width=self.w - self.l_margin - self.r_margin,
            align="L",
            text_align="C",
            first_row_as_headings=False,
        ) as table:
            # self.set_font("Poppins", "", 14)

            for data_row in [header]:
                table.row(data_row)

        self.line(
            x1=left_margin + 5,
            y1=self.get_y(),
            x2=self.w - self.r_margin - 5,
            y2=self.get_y(),
        )

        self.set_x(left_margin)

        with self.table(
            col_widths=(
                30,
                30,
                30,
                30,
                30,
            ),
            borders_layout="HORIZONTAL_LINES",
            width=self.w - self.l_margin - self.r_margin,
            align="L",
            text_align="C",
            first_row_as_headings=False,
        ) as table:
            for key in [keys]:

                self.set_font("Poppins", "I", 9)
                table.row(key)

        self.ln(7)
        self.line(
            x1=full_w / 1.5,
            y1=self.get_y(),
            x2=self.w - self.r_margin - 5,
            y2=self.get_y(),
        )

        self.set_font("Poppins", "", 9)
        self.set_x(full_w / 1.45)
        self.cell(
            w=50,
            h=8,
            text="Subtotal",
            align="L",
            new_x="LMARGIN",
            new_y="TOP",
        )
        self.set_x(full_w - full_w / 5.5)
        self.cell(
            w=w / 2.5,
            h=8,
            text=f'Rp. {self.convert_value(self.data["harga_total"])}',
            align="R",
            new_x="LMARGIN",
            new_y="NEXT",
        )

        ## PPN PPH
        self.line(
            x1=full_w / 1.5,
            y1=self.get_y(),
            x2=self.w - self.r_margin - 5,
            y2=self.get_y(),
        )

        self.set_font("Poppins", "", 9)
        self.set_x(full_w / 1.45)

        self.cell(
            w=50,
            h=8,
            text=f'PPN ({self.data["ppn_percent"]}%)',
            align="L",
            new_x="LMARGIN",
            new_y="TOP",
        )
        self.set_x(full_w - full_w / 5.5)
        self.cell(
            w=w / 2.5,
            h=8,
            text=f'Rp. {self.convert_value(self.data["ppn_value"])}',
            align="R",
            new_x="LMARGIN",
            new_y="NEXT",
        )
        self.set_x(full_w / 1.45)
        self.cell(
            w=50,
            h=8,
            text=f'PPH 22 ({self.convert_value(self.data["pph_22_percent"])}%)',
            align="L",
            new_x="LMARGIN",
            new_y="TOP",
        )
        self.set_x(full_w - full_w / 5.5)
        self.cell(
            w=w / 2.5,
            h=8,
            text=f'Rp. {self.convert_value(self.data["pph_22_value"])}',
            align="R",
            new_x="LMARGIN",
            new_y="NEXT",
        )
        self.set_x(full_w / 1.45)
        self.cell(
            w=50,
            h=8,
            text="Biaya Admin",
            align="L",
            new_x="LMARGIN",
            new_y="TOP",
        )
        self.set_x(full_w - full_w / 5.5)
        self.cell(
            w=w / 2.5,
            h=8,
            text=f'Rp. {self.convert_value(self.data["biaya_admin"])}',
            align="R",
            new_x="LMARGIN",
            new_y="NEXT",
        )

        ## Menghitung total semuanya
        self.line(
            x1=full_w / 1.5,
            y1=self.get_y(),
            x2=self.w - self.r_margin - 5,
            y2=self.get_y(),
        )

        self.set_font("Poppins", "", 9)

        self.set_x(full_w / 1.45)

        self.cell(
            w=50,
            h=8,
            text="Total",
            align="L",
            new_x="LMARGIN",
            new_y="TOP",
        )
        self.set_x(full_w - full_w / 5.5)
        self.set_font("Poppins", "", 9)

        self.cell(
            w=w / 2.5,
            h=8,
            text=f'Rp. {self.convert_value(self.data["harga_total_ppn_pph"])}',
            align="R",
            new_x="LMARGIN",
            new_y="NEXT",
        )
        self.line(
            x1=full_w / 1.5,
            y1=self.get_y(),
            x2=self.w - self.r_margin - 5,
            y2=self.get_y(),
        )
        self.set_xy(left_margin, self.get_y())

    def bottom(self):
        page_width = self.w
        left_margin = self.l_margin
        right_margin = self.r_margin
        full_w = page_width - left_margin - right_margin

        w = full_w / 2
        self.ln(5)
        self.set_font("Poppins", "I", 9)

        self.set_x(left_margin + 5)
        self.cell(
            w=10,
            h=5,
            text=f"Pembayaran Ke :",
            align="L",
            new_x="LMARGIN",
            new_y="NEXT",
        )
        self.set_font("Poppins", "B", 9)

        self.set_x(left_margin + 5)

        self.cell(
            w=w / 2,
            h=5,
            text=f'{self.data["account_bank_name"]}',
            align="L",
            new_x="LMARGIN",
            new_y="NEXT",
        )
        va = str(self.data["account_va"])

        account_va = ""
        for i, value in enumerate(va):
            if i == 5 or i == 8 or i == 11:
                account_va += "-"
                account_va += value
            else:
                account_va += value

        self.set_x(left_margin + 5)
        self.cell(
            w=10,
            h=5,
            text=account_va,
            align="L",
            new_x="LMARGIN",
            new_y="NEXT",
        )
        qr = qrcode.QRCode(
            version=1,
            error_correction=qrcode.constants.ERROR_CORRECT_L,
            box_size=10,
            border=4,
        )

        qr.add_data(
            "https://saleshub.idfood.co.id/api/f_trans/c_subsidiary_inventory_sales_order/create_pdf_so?id_="
            + self.data["md5_file"]
        )

        qr.make(fit=True)
        img = qr.make_image(fill_color="black", back_color="white")

        # Save QR code to a BytesIO object
        img_buffer = BytesIO()
        img.save(img_buffer, format="PNG")
        img_buffer.seek(0)  # Rewind buffer to the beginning
        y = self.get_y()
        # Add QR code image to the footer
        self.image(
            img_buffer, x=self.w - self.r_margin - 40, y=self.get_y() - 15, w=30, h=30
        )
        self.set_xy(self.w - self.r_margin - 46, self.get_y() + 15)
        self.set_font("Poppins", "I", 6)
        self.cell(
            w=10,
            h=5,
            text="*Scan untuk melihat invoice digital",
            align="L",
            new_x="LMARGIN",
            new_y="NEXT",
        )

    def convert_value(self, value):
        if value is None:
            return ""
        elif isinstance(value, dt.date):
            return value.strftime("%d-%m-%Y")
        elif isinstance(value, int):
            return "{:,}".format(value).replace(",", ".")
        elif isinstance(value, Decimal):
            value = float(value)

            formatted = (
                "{:,.2f}".format(value)
                .replace(",", " ")
                .replace(".", ",")
                .replace(" ", ".")
            )
            if formatted.endswith(",00"):
                formatted = formatted[:-3]

            return formatted

        return value


# data = {
#     "id_trans": "NUS.38.SO.2025.09.0001",
#     "produk_id": 3,
#     "nama_produk": "Rajagula GKP 1kg",
#     "kategori_id": 3,
#     "kategori": "Gula",
#     "id_uom_satuan": 2,
#     "uom_satuan": "Kg",
#     "company_id": 2,
#     "company_name": "PT Rajawali Nusindo",
#     "cabang_id": 38,
#     "cabang_name": "Tangerang",
#     "qty": 150,
#     "harga_satuan": Decimal("16500.00"),
#     "harga_total": Decimal("2475000.00"),
#     "ket_status_release": "release",
#     "status_release": True,
#     "tanggal": datetime.date(2025, 9, 30),
#     "file_upload": None,
#     "customer_id": 30024304,
#     "nama_customer": "ANEKA MAJU/BSD",
#     "ppn_percent": Decimal("11.0"),
#     "ppn_value": Decimal("272250.00"),
#     "pph_22_percent": Decimal("1.5"),
#     "pph_22_value": Decimal("37125.00"),
#     "harga_total_ppn_pph": Decimal("2786875.00"),
#     "no_urut": 1,
#     "updateindb": datetime.datetime(2025, 9, 30, 11, 19, 5, 771590),
#     "alamat": "PS.MODERN BSD CITY BLOK K78",
#     "no_ktp": "3307041001880003",
#     "no_hp": None,
#     "email": None,
#     "account_va": "9652510213800371",
#     "account_bank_name": "Bank BTN",
#     "no_invoice": "IDFOOD.NUS.38.INV.2025.09.0001",
#     "complete_payment": "Belum Lunas",
#     "ato": Decimal("2786875.00"),
#     "tanggal_invoice": datetime.date(2025, 9, 30),
#     "tanggal_due_date": datetime.date(2025, 10, 30),
#     "md5_file": "1e1967528ecc919bc16d5ce9a82959e8",
#     "id_pembayaran": 1,
#     "pembayaran": "Cash",
# }

# pdf = PDF(data)
# pdf.generate_report()
