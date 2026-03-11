import datetime as dt
from decimal import Decimal
from fpdf import FPDF
import qrcode

# from sales_order_recap_json.sales_detail import sales_detail
# from sales_order_recap_json.sales_resume import sales_resume
# from sales_order_recap_json.inventory_data import inventory_data
# from sales_order_recap_json.inventory_data_resume import (
#     inventory_resume,
# )


class PDF(FPDF):
    def __init__(
        self,
        number_report,
        resume_sale_data_direct,
        resume_inventory_data,
        detail_sales_data_direct,
        detail_inventory_data,
        resume_sale_data_dropship,
        detail_sales_data_dropship,
        orientation="L",
        unit="mm",
        format="A4",
    ):
        super().__init__(orientation, unit, format)
        self.number_report = number_report
        self.resume_sale_data = resume_sale_data_direct
        self.resume_inventory_data = resume_inventory_data
        self.detail_sales_data = detail_sales_data_direct
        self.detail_inventory_data = detail_inventory_data
        self.resume_sale_data_dropship = resume_sale_data_dropship
        self.detail_sales_data_dropship = detail_sales_data_dropship
        self.is_direct = True

    def header(self):
        y = self.get_y()
        self.image("files/img/id_food.png", w=28.5, h=8, keep_aspect_ratio=True)
        self.add_font("Poppins", "", "files/font/Poppins/Poppins-Regular.ttf")
        self.add_font("Poppins", "B", "files/font/Poppins/Poppins-Bold.ttf")
        self.add_font("Poppins", "I", "files/font/Poppins/Poppins-Italic.ttf")
        self.add_font("Poppins", "BI", "files/font/Poppins/Poppins-BlackItalic.ttf")
        y = self.get_y()
        self.set_font("Poppins", "B", 14)

        if self.is_direct:
            self.set_x(self.l_margin)
            self.set_y(y - 7)
            self.cell(0, 5, "DIRECT", align="R", new_x="LMARGIN", new_y="NEXT")
        else:
            self.set_x(self.l_margin)
            self.set_y(y - 7)
            self.cell(0, 5, "DROPSHIP", align="R", new_x="LMARGIN", new_y="NEXT")

        if self.page_no() != 1:
            y = y + 5

        self.set_y(y)

    def generate_report(self):
        self.add_page()
        self.top_data()
        self.sales_data(self.detail_sales_data)
        self.inventory_data()

        self.is_direct = False
        self.add_page()
        self.dropship_data()
        self.sales_data(self.detail_sales_data_dropship)

        filename = f"files/sales_order_report/{self.number_report}.pdf"
        self.output(filename)
        print("PDF GENERATED.")

    def top_data(self):
        self.add_font("Poppins", "", "files/font/Poppins/Poppins-Regular.ttf")
        self.add_font("Poppins", "B", "files/font/Poppins/Poppins-Bold.ttf")
        self.set_font("Poppins", "B", 14)
        self.set_text_color(0, 84, 171)
        self.cell(0, 10, "SALES RECAP REPORT", align="C", new_x="LMARGIN", new_y="NEXT")
        self.set_text_color(0, 0, 0)

        self.set_font("Poppins", "", 12)

        x = self.get_x()
        full_width = self.w - self.l_margin - self.r_margin

        ######### SALES RESUME #############
        inv_index = 0
        for index, value in enumerate(self.resume_sale_data):
            self.sales_resume(value)

            if self.resume_inventory_data[inv_index]["produk_id"] != value["produk_id"]:
                inv_index += 1

            self.inventory_data_resume(self.resume_inventory_data[inv_index])
            inv_index += 1

    def dropship_data(self):
        for index, value in enumerate(self.resume_sale_data_dropship):
            self.sales_resume(value)

    def sales_resume(self, data):
        full_width = self.w - self.l_margin - self.r_margin

        self.line(
            x1=self.l_margin,
            x2=self.w - self.r_margin,
            y1=self.get_y(),
            y2=self.get_y(),
        )

        self.ln(3)
        self.set_font("Poppins", "B", 12)
        self.set_text_color(0, 84, 171)
        self.cell(
            0,
            10,
            f"SALES RESUME ",
            align="L",
            new_x="LMARGIN",
            new_y="TOP",
        )
        self.set_text_color(76, 145, 0)
        self.set_font("Poppins", "I", 12)
        self.set_x(full_width - full_width / 6)
        self.cell(
            0,
            10,
            f"{data['nama_produk']}",
            align="L",
            new_x="LMARGIN",
            new_y="NEXT",
        )
        self.set_font("Poppins", "I", 12)
        self.set_text_color(0, 0, 0)

        self.cell(0, 5, "Total Sales", align="L", new_x="LMARGIN", new_y="TOP")
        self.set_x(full_width - full_width / 1.5)
        self.cell(
            0,
            5,
            f"Quantity ({data['uom_satuan']})",
            align="L",
            new_x="LMARGIN",
            new_y="TOP",
        )
        self.set_x(full_width - full_width / 2.5)
        self.cell(0, 5, "Harga Satuan Jual", align="L", new_x="LMARGIN", new_y="NEXT")

        self.set_font("Poppins", "", 12)

        self.cell(
            0,
            10,
            f'Rp. {self.convert_value(data["sales_total"])}',
            align="L",
            new_x="LMARGIN",
            new_y="TOP",
        )
        self.set_x(full_width - full_width / 1.5)
        self.cell(
            0,
            10,
            f'{self.convert_value(data["sales_qty"])}',
            align="L",
            new_x="LMARGIN",
            new_y="TOP",
        )
        self.set_x(full_width - full_width / 2.5)
        self.cell(
            0,
            10,
            f'Rp. {self.convert_value(data["harga_sat_penj"])}',
            align="L",
            new_x="LMARGIN",
            new_y="NEXT",
        )

        self.ln(5)

        self.set_font("Poppins", "I", 12)

        self.cell(0, 5, "Total HPP", align="L", new_x="LMARGIN", new_y="TOP")
        self.set_x(full_width - full_width / 1.5)
        self.cell(0, 5, "HPP Satuan", align="L", new_x="LMARGIN", new_y="TOP")
        self.set_x(full_width - full_width / 2.5)
        self.cell(0, 5, "Margin Total", align="L", new_x="LMARGIN", new_y="TOP")
        self.set_x(full_width - full_width / 5.5)
        self.cell(0, 5, "Margin Total %", align="L", new_x="LMARGIN", new_y="NEXT")

        self.set_font("Poppins", "", 12)
        self.cell(
            0,
            10,
            f'Rp. {self.convert_value(data["hpp"])}',
            align="L",
            new_x="LMARGIN",
            new_y="TOP",
        )
        self.set_x(full_width - full_width / 1.5)
        self.cell(
            0,
            10,
            f'Rp. {self.convert_value(data["harga_sat_hpp"])}',
            align="L",
            new_x="LMARGIN",
            new_y="TOP",
        )
        self.set_x(full_width - full_width / 2.5)
        self.cell(
            0,
            10,
            f'Rp. {self.convert_value(data["margin_total"])}',
            align="L",
            new_x="LMARGIN",
            new_y="TOP",
        )
        self.set_x(full_width - full_width / 5.5)
        self.cell(
            0,
            10,
            f'{self.convert_value(data["margin_percent"])} %',
            align="L",
            new_x="LMARGIN",
            new_y="NEXT",
        )

        self.ln(3)

    def inventory_data_resume(self, data):
        full_width = self.w - self.l_margin - self.r_margin

        self.ln(3)
        self.set_font("Poppins", "B", 12)
        self.set_text_color(0, 84, 171)
        self.cell(
            0,
            10,
            f"INVENTORY RESUME",
            align="L",
            new_x="LMARGIN",
            new_y="NEXT",
        )
        self.set_text_color(0, 0, 0)

        self.set_font("Poppins", "I", 12)

        self.cell(0, 5, "Total HPP", align="L", new_x="LMARGIN", new_y="TOP")
        self.set_x(full_width - full_width / 1.5)
        self.cell(
            0, 5, "Harga Satuan Inventory", align="L", new_x="LMARGIN", new_y="TOP"
        )
        self.set_x(full_width - full_width / 2.5)
        self.cell(
            0,
            5,
            f"Quantity ({data['uom_satuan']})",
            align="L",
            new_x="LMARGIN",
            new_y="NEXT",
        )

        self.set_font("Poppins", "", 12)

        self.cell(
            0,
            10,
            f'Rp. {self.convert_value(data["total_hpp"])}',
            align="L",
            new_x="LMARGIN",
            new_y="TOP",
        )
        self.set_x(full_width - full_width / 1.5)
        self.cell(
            0,
            10,
            f'RP. {self.convert_value(data["harga_satuan"])}',
            align="L",
            new_x="LMARGIN",
            new_y="TOP",
        )
        self.set_x(full_width - full_width / 2.5)
        self.cell(
            0,
            10,
            f'{self.convert_value(data["inv_qty"])}',
            align="L",
            new_x="LMARGIN",
            new_y="NEXT",
        )

        self.ln(3)

        self.line(
            x1=self.l_margin,
            x2=self.w - self.r_margin,
            y1=self.get_y(),
            y2=self.get_y(),
        )
        self.line(
            x1=self.l_margin,
            x2=self.w - self.r_margin,
            y1=self.get_y(),
            y2=self.get_y(),
        )

    def sales_data(self, detail_sales_data):
        # print(detail_sales_data)
        if detail_sales_data == []:
            return
        full_width = self.w - self.l_margin - self.r_margin
        half_width = full_width / 2

        ########## SALES DETAIL #############

        headers_list = [
            "Invoice",
            "Cust",
            "Cabang",
            "Comp",
            "Qty",
            "UOM",
            "Hrg Satuan",
            "Hrg Total",
            "Hrg Satuan Hpp",
            "Hrg Total HPP",
            "Margin",
            "Margin %",
        ]
        rows = []
        if detail_sales_data == []:
            temp_data = {
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

            headers = temp_data.keys()

        if detail_sales_data != []:
            headers = detail_sales_data[0].keys()
            header_list = list(headers)
            rows = [[item[key] for key in header_list] for item in detail_sales_data]
        else:
            rows = [[]]

        body_data = rows

        self.ln(8)
        self.set_font("Poppins", "BU", 12)
        self.cell(
            0,
            10,
            f"SALES DETAIL - {detail_sales_data[0]['nama_produk']}",
            align="L",
            new_x="LMARGIN",
            new_y="NEXT",
        )
        self.set_font("Poppins", "", 8)

        #### Header Table ####
        self.line(
            x1=self.l_margin,
            x2=self.w - self.r_margin,
            y1=self.get_y(),
            y2=self.get_y(),
        )
        with self.table(
            col_widths=(15, 15, 15, 10, 10, 15, 15, 15, 15, 15, 15, 15),
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
        total_hpp = 0
        total_margin = 0
        produk_id = body_data[0][0]
        rowIndex = 0

        with self.table(
            col_widths=(15, 15, 15, 10, 10, 15, 15, 15, 15, 15, 15, 15),
            text_align="C",
            align="L",
            cell_fill_color=250,
            borders_layout="HORIZONTAL_LINES",
            # cell_fill_mode="ROWS",
            first_row_as_headings=False,
            width=self.w - self.l_margin - self.r_margin,
        ) as table:
            for index, data_row in enumerate(body_data):
                row = table.row()
                column = 1
                # print(data_row[0])
                if produk_id != data_row[0]:
                    rowIndex = index
                    break

                for datum in data_row:
                    if column == 1 or column == 6:
                        column += 1
                        continue
                    if column == 7:
                        if datum == None:
                            # print(datum)
                            # print(row)
                            datum = 0
                        total_qty += datum
                    if column == 10:
                        if datum == None:
                            print(datum)
                        total_harga += datum
                    if column == 12:
                        total_hpp += datum
                    if column == 13:
                        total_margin += datum

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
            "",
            total_qty,
            "",
            "",
            total_harga,
            "",
            total_hpp,
            total_margin,
            "",
        ]
        with self.table(
            col_widths=(15, 15, 15, 10, 10, 15, 15, 15, 15, 15, 15, 15),
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

        if rowIndex != 0:
            self.sales_data(detail_sales_data[rowIndex:])

    def inventory_data(self):
        full_width = self.w - self.l_margin - self.r_margin
        half_width = full_width / 2

        ########## INVENTORY DETAIL #############
        self.ln(5)
        self.set_font("Poppins", "BU", 12)
        self.cell(0, 10, "INVENTORY DETAIL", align="L", new_x="LMARGIN", new_y="NEXT")
        self.set_font("Poppins", "", 8)

        header = ["Company", "Cabang", "Qty", "Harga Satuan", "Total HPP"]
        rows = []

        if self.detail_inventory_data == []:
            temp_data = {
                {
                    "company_name": "PT Rajawali Nusindo",
                    "cabang_name": "Cabang 2",
                    "qty": 650000,
                    "harga_satuan": 15010,
                    "harga_total": 9756500000,
                },
            }

            headers = temp_data.keys()

        if self.detail_inventory_data != []:
            headers = self.detail_inventory_data[0].keys()
            header_list = list(headers)
            rows = [
                [item[key] for key in header_list]
                for item in self.detail_inventory_data
            ]
        else:
            rows = [[]]

        body_data = rows

        self.line(
            x1=self.l_margin,
            x2=self.w - self.r_margin,
            y1=self.get_y(),
            y2=self.get_y(),
        )

        #### Header Table ####
        with self.table(
            col_widths=(30, 30, 30, 30, 30),
            borders_layout="HORIZONTAL_LINES",
            width=self.w - self.l_margin - self.r_margin,
            align="L",
            text_align="C",
        ) as table:
            for data_row in [header]:
                table.row(data_row)

        self.line(
            x1=self.l_margin,
            x2=self.w - self.r_margin,
            y1=self.get_y(),
            y2=self.get_y(),
        )

        #### Body Table ####
        qty = 0
        harga_total = 0
        with self.table(
            col_widths=(30, 30, 30, 30, 30),
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
                    if column == 2:
                        qty += datum
                    if column == 4:
                        harga_total += datum
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
        header_total = ["", "", qty, "", harga_total]
        with self.table(
            col_widths=(30, 30, 30, 30, 30),
            borders_layout="HORIZONTAL_LINES",
            width=self.w - self.l_margin - self.r_margin,
            align="L",
            text_align="C",
        ) as table:
            for data_row in [header_total]:
                row = table.row()
                for datum in data_row:
                    a = self.convert_value(datum)
                    row.cell(a, padding=1)
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


# pdf = PDF(
#     number_report="ALL.RPT.2025.11.0002",
#     resume_sale_data=sales_resume,
#     resume_inventory_data=inventory_resume,
#     detail_sales_data=sales_detail,
#     detail_inventory_data=inventory_data,
# )
# pdf.generate_report()
