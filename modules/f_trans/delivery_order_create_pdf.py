from datetime import datetime
import datetime as dt
from io import BytesIO
from fpdf import FPDF
import qrcode


class PDF(FPDF):
    def __init__(self, data, orientation="P", unit="mm", format="A4"):
        super().__init__(orientation, unit, format)
        self.data = data

    def header(self):
        y = self.get_y()
        self.image("files/img/id_food.png", w=28.5, h=8, keep_aspect_ratio=True)
        self.add_font("Poppins", "", "files/font/Poppins/Poppins-Regular.ttf")
        self.add_font("Poppins", "B", "files/font/Poppins/Poppins-Bold.ttf")
        self.add_font("Poppins", "I", "files/font/Poppins/Poppins-Italic.ttf")
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
            text="DeliveryOrder-IDFOOD",
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
        filename = f"files/invoice_delivery_order/{self.data['id_trans']}.pdf"
        self.output(filename)
        print("PDF GENERATED.")

    def top_data(self):
        self.set_font("Poppins", "", 9)
        page_width = self.w
        left_margin = self.l_margin
        right_margin = self.r_margin
        full_w = page_width - left_margin - right_margin

        w = full_w / 2
        initial_y = self.get_y()
        self.set_font("Poppins", "I", 9)

        self.set_x(full_w - full_w / 4)
        self.cell(
            w=w / 2,
            h=5,
            text="DO Number : ",
            align="L",
            new_x="LMARGIN",
            new_y="NEXT",
        )
        self.set_font("Poppins", "", 9)

        self.set_x(full_w - full_w / 4)
        self.cell(
            w=w / 2,
            h=5,
            text=self.data["id_trans"],
            align="L",
            new_x="LMARGIN",
            new_y="NEXT",
        )

        self.set_x(full_w - full_w / 4)
        self.cell(
            w=w / 2,
            h=6,
            text=self.convert_value(self.data["tanggal_do"]),
            align="L",
            new_x="LMARGIN",
            new_y="NEXT",
        )

        self.ln(5)

        y = self.get_y()

        self.set_xy(left_margin + 5, initial_y)
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

        self.ln(10)

        self.set_font("Poppins", "", 9)

    def table_data(self):
        page_width = self.w
        left_margin = self.l_margin
        right_margin = self.r_margin
        full_w = page_width - left_margin - right_margin
        w = full_w / 2

        header = [
            "Nama Produk",
            "Quantity",
            "Satuan",
        ]
        keys = [
            self.convert_value(self.data["nama_produk"]),
            self.convert_value(self.data["qty"]),
            self.convert_value(self.data["uom_satuan"]),
        ]

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
            ),
            borders_layout="HORIZONTAL_LINES",
            width=self.w - self.l_margin - self.r_margin,
            align="L",
            text_align="C",
            first_row_as_headings=False,
        ) as table:
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
            ),
            borders_layout="HORIZONTAL_LINES",
            width=self.w - self.l_margin - self.r_margin,
            align="L",
            text_align="C",
            first_row_as_headings=False,
        ) as table:
            for key in [keys]:
                table.row(key)

        self.line(
            x1=left_margin + 5,
            y1=self.get_y(),
            x2=self.w - self.r_margin - 5,
            y2=self.get_y(),
        )

        self.ln(5)

    def bottom(self):
        # qr = qrcode.QRCode(
        #     version=1,
        #     error_correction=qrcode.constants.ERROR_CORRECT_L,
        #     box_size=10,
        #     border=4,
        # )
        # qr.add_data("Some data here")  # Replace with your data (e.g., URL)
        # qr.make(fit=True)
        # img = qr.make_image(fill_color="black", back_color="white")

        # # Save QR code to a BytesIO object
        # img_buffer = BytesIO()
        # img.save(img_buffer, format="PNG")
        # img_buffer.seek(0)  # Rewind buffer to the beginning
        # y = self.get_y()
        # # Add QR code image to the footer
        # self.image(
        #     img_buffer, x=self.w - self.r_margin - 60, y=self.get_y() + 80, w=40, h=40
        # )
        self.set_font("Poppins", "", 9)
        page_width = self.w
        left_margin = self.l_margin
        right_margin = self.r_margin
        full_w = page_width - left_margin - right_margin

        self.set_x(full_w / 1.2 - 3)
        self.cell(
            w=25,
            h=6,
            text="Tanggal Terima",
            align="R",
            new_x="LMARGIN",
            new_y="NEXT",
        )

        self.ln(5)

        self.line(
            x1=full_w / 1.25,
            y1=self.get_y(),
            x2=self.w - self.r_margin - 17,
            y2=self.get_y(),
        )

        self.ln(10)

        y_ = self.get_y()
        self.set_x(left_margin + 20)
        self.cell(
            w=50,
            h=6,
            text="Pengirim, ",
            align="L",
            new_x="LMARGIN",
            new_y="NEXT",
        )
        self.line(
            x1=left_margin + 10,
            y1=self.get_y() + 20,
            x2=left_margin + 50,
            y2=self.get_y() + 20,
        )

        self.set_xy(full_w / 1.2, y_)
        self.cell(
            w=50,
            h=6,
            text="Penerima, ",
            align="L",
            new_x="LMARGIN",
            new_y="NEXT",
        )

        self.line(
            x1=full_w / 1.28,
            y1=self.get_y() + 20,
            x2=self.w - self.r_margin - 13,
            y2=self.get_y() + 20,
        )

    def convert_value(self, value):
        if value is None:
            return ""
        elif isinstance(value, dt.date):
            return value.strftime("%d-%m-%Y")
        elif isinstance(value, int):
            return "{:,}".format(value).replace(",", ".")

        return value


# data = {
#     "id_trans": "HLD.SO.2025.09.0003",
#     "produk_id": 1,
#     "nama_produk": "Gula(Ton)",
#     "kategori_id": 1,
#     "tanggal_do": "2025-09-12",
#     "kategori": "Bahan Pangan",
#     "alamat": "Jl. Raya No. 1, Jakarta",
#     "no_hp": "021-12345678",
#     "id_uom_satuan": 1,
#     "uom_satuan": "Ton",
#     "company_id": 1,
#     "company_name": "PT RNI (Holding)",
#     "cabang_id": 1,
#     "cabang_name": "Holding",
#     "qty": 1000,
#     "harga_satuan": 15000,
#     "harga_total": 15000000,
#     "ket_status_release": "release",
#     "status_release": True,
#     "tanggal": "2025-09-12",
#     "file_upload": None,
#     "customer_id": 2,
#     "nama_customer": "The Name",
#     "ppn_percent": 11.0,
#     "ppn_value": 1650000,
#     "pph_22_percent": 1.5,
#     "pph_22_value": 225000,
#     "harga_total_ppn_pph": 16875000,
#     "no_urut": 3,
#     "updateindb": "2025-09-12T09:59:11.057022",
# }

# pdf = PDF(data)
# pdf.generate_report()
