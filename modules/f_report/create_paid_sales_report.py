import datetime as dt
from decimal import Decimal
from io import BytesIO
from fpdf import FPDF
import qrcode


class PDF(FPDF):
    def __init__(
        self,
        resume_sale_data,
        detail_sales_data,
        resume_sales_payment,
        month_name,
        year,
        orientation="L",
        unit="mm",
        format="A4",
    ):
        super().__init__(orientation, unit, format)
        self.resume_sale_data = resume_sale_data
        self.detail_sales_data = detail_sales_data
        self.resume_sales_payment = resume_sales_payment
        self.month_name = month_name
        self.year = year

    def header(self):
        y = self.get_y()
        self.image("files/img/id_food.png", w=28.5, h=8, keep_aspect_ratio=True)
        self.add_font("Poppins", "", "files/font/Poppins/Poppins-Regular.ttf")
        self.add_font("Poppins", "B", "files/font/Poppins/Poppins-Bold.ttf")
        self.set_font("Poppins", "B", 12)
        resume_data = self.resume_sale_data[0]
        company_name = resume_data["company_name"]
        self.cell(
            0,
            10,
            f"Payment Recap Report {company_name} {self.month_name} {self.year}",
            align="C",
            new_x="LMARGIN",
            new_y="NEXT",
        )

    def generate_report(self):
        self.add_page()
        for data in self.resume_sale_data:
            self.top_data(data)

        self.resume_payment()
        self.sales_data()

        pdf_bytes = self.output(dest="S")
        pdf_buffer = BytesIO(pdf_bytes)
        pdf_buffer.seek(0)
        return pdf_buffer

    def top_data(self, sales_data):
        self.add_font("Poppins", "", "files/font/Poppins/Poppins-Regular.ttf")
        self.add_font("Poppins", "B", "files/font/Poppins/Poppins-Bold.ttf")

        self.set_font("Poppins", "", 8)

        x = self.get_x()
        full_width = self.w - self.l_margin - self.r_margin

        ######### SALES RESUME #############
        self.ln(3)
        self.set_font("Poppins", "BU", 8)
        self.cell(0, 10, "SALES RESUME", align="L", new_x="LMARGIN", new_y="NEXT")
        self.set_font("Poppins", "", 8)

        self.cell(0, 5, "Nama Produk", align="L", new_x="LMARGIN", new_y="TOP")
        self.set_x(full_width - full_width / 1.3)
        self.cell(0, 5, "Total Sales", align="L", new_x="LMARGIN", new_y="TOP")
        self.set_x(full_width - full_width / 1.8)
        self.cell(0, 5, "Total Paid Sales", align="L", new_x="LMARGIN", new_y="TOP")
        self.set_x(full_width - full_width / 3.6)
        self.cell(0, 5, "Quantity (Kg)", align="L", new_x="LMARGIN", new_y="NEXT")

        self.cell(
            0,
            7,
            f'{self.convert_value(sales_data["nama_produk"])}',
            align="L",
            new_x="LMARGIN",
            new_y="TOP",
        )
        self.set_x(full_width - full_width / 1.3)
        self.cell(
            0,
            5,
            f'Rp. {self.convert_value(sales_data["sales_total"])}',
            align="L",
            new_x="LMARGIN",
            new_y="TOP",
        )
        self.set_x(full_width - full_width / 1.8)
        self.cell(
            0,
            5,
            f'{self.convert_value(sales_data["paid_sales_total"])}',
            align="L",
            new_x="LMARGIN",
            new_y="TOP",
        )
        self.set_x(full_width - full_width / 3.6)
        self.cell(
            0,
            5,
            f'{self.convert_value(sales_data["sales_qty"])}',
            align="L",
            new_x="LMARGIN",
            new_y="NEXT",
        )

        self.ln(4)

        self.cell(0, 5, "HPP Satuan", align="L", new_x="LMARGIN", new_y="TOP")
        self.set_x(full_width - full_width / 1.3)
        self.cell(0, 5, "Margin Total", align="L", new_x="LMARGIN", new_y="TOP")
        self.set_x(full_width - full_width / 1.8)
        self.cell(0, 5, "Margin Total %", align="L", new_x="LMARGIN", new_y="NEXT")

        self.cell(
            0,
            5,
            f'Rp. {self.convert_value(sales_data["harga_sat_hpp"])}',
            align="L",
            new_x="LMARGIN",
            new_y="TOP",
        )
        self.set_x(full_width - full_width / 1.3)
        self.cell(
            0,
            5,
            f'Rp. {self.convert_value(sales_data["margin_total"])}',
            align="L",
            new_x="LMARGIN",
            new_y="TOP",
        )
        self.set_x(full_width - full_width / 1.8)
        self.cell(
            0,
            5,
            f'{self.convert_value(sales_data["margin_percent"])} %',
            align="L",
            new_x="LMARGIN",
            new_y="NEXT",
        )
        # self.set_x(full_width - full_width / 4.5)

        self.ln(3)

        self.line(
            x1=self.l_margin,
            x2=self.w - self.r_margin,
            y1=self.get_y(),
            y2=self.get_y(),
        )

    def resume_payment(self):
        self.ln(5)
        self.set_font("Poppins", "BU", 8)
        self.cell(
            0, 10, "Payment Branch Resume", align="L", new_x="LMARGIN", new_y="NEXT"
        )
        self.set_font("Poppins", "", 8)

        headers_list = [
            "Cabang",
            "Nama Produk",
            "Qty",
            "Sales",
            "Outstanding",
            "Paid",
            "Paid %",
        ]
        rows = []
        self.line(
            x1=self.l_margin,
            x2=self.w - self.r_margin,
            y1=self.get_y(),
            y2=self.get_y(),
        )

        if self.resume_sales_payment == []:
            temp_data = {
                "cabang_name": "",
                "nama_produk": "",
                "total_qty": "",
                "total_sales": "",
                "total_outstanding": "",
                "paid_sales": "",
                "paid_percentage": "",
            }

            headers = temp_data.keys()

        if self.resume_sales_payment != []:
            headers = self.resume_sales_payment[0].keys()
            header_list = list(headers)
            rows = [
                [item[key] for key in header_list] for item in self.resume_sales_payment
            ]
        else:
            rows = [[]]

        body_data = rows

        #### Header Table ####
        with self.table(
            col_widths=(15, 15, 15, 15, 15, 15, 15),
            borders_layout="HORIZONTAL_LINES",
            width=self.w - self.l_margin - self.r_margin,
            align="L",
            text_align="C",
        ) as table:
            for data_row in [headers_list]:
                table.row(data_row)

        self.line(
            x1=self.l_margin,
            x2=self.w - self.r_margin,
            y1=self.get_y(),
            y2=self.get_y(),
        )

        with self.table(
            col_widths=(15, 15, 15, 15, 15, 15, 15),
            text_align="C",
            align="L",
            cell_fill_color=250,
            borders_layout="HORIZONTAL_LINES",
            # cell_fill_mode="ROWS",
            first_row_as_headings=False,
            width=self.w - self.l_margin - self.r_margin,
        ) as table:
            for data_row in body_data:
                row = table.row()
                for datum in data_row:
                    a = self.convert_value(datum)
                    row.cell(a, padding=1)

    def sales_data(self):
        full_width = self.w - self.l_margin - self.r_margin
        half_width = full_width / 2

        ########## SALES DETAIL #############
        self.ln(5)
        self.set_font("Poppins", "BU", 8)
        self.cell(0, 10, "SALES DETAIL", align="L", new_x="LMARGIN", new_y="NEXT")
        self.set_font("Poppins", "", 7)

        headers_list = [
            "Invoice",
            "Cust",
            "Cabang",
            "Hrg Total",
            "Adm",
            "Hrg Total HPP",
            "Margin",
            "Mrg %",
            "Paid Sales",
            "Paid Mrg",
        ]
        rows = []
        self.line(
            x1=self.l_margin,
            x2=self.w - self.r_margin,
            y1=self.get_y(),
            y2=self.get_y(),
        )
        if self.detail_sales_data == []:
            temp_data = {
                "invoice_number": "IDFOOD.NUS.2.INV.2025.09.0001",
                "nama_customer": "Customer cabang 2",
                "nama_produk": "Customer cabang 2",
                "cabang_name": "Cabang 2",
                "qty": 350000,
                "uom_satuan": "Kg",
                "harga_satuan": 17000,
                "harga_total": 5950000000,
                "biaya_admin": 500,
                "harga_satuan_hpp": 15010,
                "harga_total_hpp": 5253500000,
                "margin": 696500000,
                "percent_margin": 0.12,
                "paid_sales": 0,
                "paid_margin": 0,
            }

            headers = temp_data.keys()

        if self.detail_sales_data != []:
            headers = self.detail_sales_data[0].keys()
            header_list = list(headers)
            rows = [
                [item[key] for key in header_list] for item in self.detail_sales_data
            ]
        else:
            rows = [[]]

        body_data = rows

        #### Header Table ####
        with self.table(
            col_widths=(28, 15, 15, 15, 15, 15, 15, 15, 15, 15),
            borders_layout="HORIZONTAL_LINES",
            width=self.w - self.l_margin - self.r_margin,
            align="L",
            text_align="C",
        ) as table:
            for data_row in [headers_list]:
                table.row(data_row)

        self.line(
            x1=self.l_margin,
            x2=self.w - self.r_margin,
            y1=self.get_y(),
            y2=self.get_y(),
        )

        ### Body Table ####
        total_qty = 0
        total_harga = 0
        total_biaya_admin = 0
        total_hpp = 0
        total_margin = 0
        total_paid_sales = 0
        total_margin_paid_sales = 0

        with self.table(
            col_widths=(28, 15, 15, 15, 15, 15, 15, 15, 15, 15),
            text_align="C",
            align="L",
            cell_fill_color=250,
            borders_layout="HORIZONTAL_LINES",
            # cell_fill_mode="ROWS",
            first_row_as_headings=False,
            width=self.w - self.l_margin - self.r_margin,
        ) as table:
            for data_row in body_data:
                row = table.row()
                column = 0
                for datum in data_row:
                    if column == 3:
                        total_harga += datum
                    if column == 4:
                        total_biaya_admin += datum
                    if column == 5:
                        total_hpp += datum
                    if column == 6:
                        total_margin += datum
                    if column == 8:
                        total_paid_sales += datum
                    if column == 9:
                        total_margin_paid_sales += datum

                    a = self.convert_value(datum)
                    row.cell(a, padding=1)
                    column += 1

        half_width = full_width / 2

        self.line(
            x1=self.l_margin,
            x2=self.w - self.r_margin,
            y1=self.get_y(),
            y2=self.get_y(),
        )

        #### Total Table ####
        header_total = [
            "",
            "",
            "",
            total_harga,
            total_biaya_admin,
            total_hpp,
            total_margin,
            "",
            total_paid_sales,
            total_margin_paid_sales,
        ]

        with self.table(
            col_widths=(28, 15, 15, 15, 15, 15, 15, 15, 15, 15),
            borders_layout="HORIZONTAL_LINES",
            width=self.w - self.l_margin - self.r_margin,
            align="L",
            text_align="C",
        ) as table:
            for data_row in [header_total]:
                row = table.row()
                for item in data_row:
                    row.cell(self.convert_value(item))

        self.line(
            x1=self.l_margin,
            x2=self.w - self.r_margin,
            y1=self.get_y(),
            y2=self.get_y(),
        )

    def convert_value(self, value):
        if value is None:
            return ""
        elif isinstance(value, dt.date):
            return value.strftime("%d-%m-%Y")
        elif isinstance(value, int):
            return "{:,}".format(value).replace(",", ".")
        elif isinstance(value, float):
            formatted = (
                "{:,.2f}".format(value)
                .replace(",", " ")
                .replace(".", ",")
                .replace(" ", ".")
            )
            if formatted.endswith(",00"):
                formatted = formatted[:-3]

            return formatted
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


resume_sales = [
    {
        "sales_total": 5950000000.0,
        "sales_qty": 350000,
        "hpp": 5253500000.0,
        "harga_sat_penj": 17000.0,
        "harga_sat_hpp": 15010.0,
        "margin_total": 696500000.0,
        "margin_percent": 11.71,
    }
]

inventory = [{"inv_qty": 2650000, "total_hpp": 39756500000.0, "harga_satuan": 15002.0}]

sales_detail = [
    {
        "invoice_number": "IDFOOD.NUS.2.INV.2025.09.0001",
        "nama_customer": "Customer cabang 2",
        "cabang_name": "Cabang 2",
        "company_name": "PT Rajawali Nusindo",
        "qty": 350000,
        "uom_satuan": "Kg",
        "harga_satuan": 17000,
        "harga_total": 5950000000,
        "harga_satuan_hpp": 15010,
        "harga_total_hpp": 5253500000,
        "margin": 696500000,
        "percent_margin": 0.12,
    }
]

inventory_detail = [
    {
        "company_name": "PT Rajawali Nusindo",
        "cabang_name": "Cabang 2",
        "qty": 650000,
        "harga_satuan": 15010,
        "harga_total": 9756500000,
    },
    {
        "company_name": "PT RNI (Holding)",
        "cabang_name": "Holding",
        "qty": 2000000,
        "harga_satuan": 15000,
        "harga_total": 30000000000,
    },
]

# pdf = PDF(
#     {},
#     resume_sale_data=resume_sales,
#     resume_inventory_data=inventory,
#     detail_sales_data=sales_detail,
#     detail_inventory_data=inventory_detail,
# )
# pdf.generate_report()
